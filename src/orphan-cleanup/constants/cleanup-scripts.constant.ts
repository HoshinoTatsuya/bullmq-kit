import { BULLMQ_KEYS as K } from './bullmq-keys.constant';
import { IS_QUEUE_LUA } from './is-queue-lua.constant';
import { ORPHAN_VERDICT as V } from './orphan-verdict.constant';

/** Значения аргумента dryRun скрипта `checkAndDelete`. */
export const DRY_RUN_ARG = { on: '1', off: '0' } as const;

/** Значения аргумента «хэш без timestamp уже видели сиротой» скрипта `checkAndDelete`. */
export const UNTIMESTAMPED_ARG = { confirmed: '1', firstSight: '0' } as const;

/**
 * Аргумент потолка LPOS скрипта `checkAndDelete`: `0` — без потолка (так же
 * `MAXLEN 0` понимает сам Redis), поэтому настройку `maxListScan` проверяет
 * конструктор (целое > 0 или Infinity), а Infinity передаётся этим значением.
 */
export const LIST_SCAN_ARG = { unbounded: '0' } as const;

/**
 * Скрипты Lua уборки. Ключи, известные заранее, — в `KEYS` (Redis Cluster
 * требует объявлять ключи, и все они — с префиксом очереди, то есть в слоте её
 * хэш-тега). Ключи дедупликации (`de:<id>`, `dn:<id>`) скрипт собирает сам из
 * префикса очереди и поля `deid`: id известен только внутри скрипта. Так же
 * делает сам BullMQ (removeDeduplicationKeyIfNeededOnRemoval.lua), и в
 * кластере BullMQ работает только с хэш-тегом в префиксе — те же ключи в том же
 * слоте.
 *
 * Проба на Redis 7.4 в режиме кластера (14.11.2026): необъявленный ключ ТОГО ЖЕ
 * слота скрипт читает и удаляет — и без shebang, и с `#!lua`; отказ («Script
 * attempted to access keys that do not hash to the same slot» или «non local
 * key») — только для ключа другого слота или чужого узла. Отказа «non declared
 * key» в Redis нет. Dragonfly необъявленные ключи по умолчанию отвергает, но
 * там и сам BullMQ требует `--default_lua_flags=allow-undeclared-keys`: его
 * скрипты собирают те же `de:`/`dn:` так же. Объявлять их заранее значило бы
 * лишний запрос `deid` на каждого кандидата ради окружения, где очередь и так
 * не работает.
 */
export const CLEANUP_SCRIPTS = {
  /**
   * Проверить и удалить ОДИН хэш атомарно: между проверкой и удалением задача
   * не может появиться в индексе — скрипт не прерывается.
   *
   * KEYS[1] — хэш задачи; KEYS[2 .. 1+I] — индексы статусов; KEYS[2+I ..] —
   * хвосты, удаляемые вместе с хэшем (без `:lock`).
   * ARGV: jobId, I, грейс мс, dryRun ('1'/'0'), префикс очереди с `:`,
   * потолок LPOS (`0` — без потолка).
   *
   * Индексы приходят в порядке «сначала sorted set'ы и множества, списки
   * последними»: ZSCORE и SISMEMBER — O(log N) и O(1), а LPOS отсутствующего id —
   * проход всего списка (замер на Redis 7.4: 15 мс на миллион элементов, и всё
   * это время Redis не отвечает никому). Задача из огромного `completed` вне
   * снимка находится по ZSCORE и до списков не доходит. С потолком LPOS
   * (`MAXLEN`) id, не найденный в первых N элементах списка длиннее N, — «не
   * смогли проверить», а не «нет в списке»: хэш НЕ удаляется.
   * Ответ — код из ORPHAN_VERDICT (коды и имена полей подставлены из констант,
   * а не повторены литералами).
   *
   * Порядок проверок: тип → индексы → возраст → дедупликация. Индекс
   * неожиданного типа — «не смогли проверить», то есть НЕ удалять: тип ключа
   * статуса меняется между версиями BullMQ, и пропуск такого индекса значил бы
   * «не проверяли, но удалили». Отсутствующий ключ индекса (`none`) — пустой индекс.
   *
   * Возраст — по `timestamp` против `TIME` сервера. Поле есть, но не число —
   * возраст неизвестен: не удалять. Поля нет — обычно поздняя запись в
   * удалённую задачу (storeJob.lua пишет `timestamp` тем же HMSET, что создаёт
   * хэш), но так же выглядит и служебный хэш БУДУЩЕЙ версии BullMQ с именем,
   * которого нет в списке служебных: удалить его — сломать очередь. Такой хэш
   * удаляется только явной опцией и по второму наблюдению
   * (ARGV[7] = UNTIMESTAMPED_ARG.confirmed выставляет вызывающий); по
   * умолчанию — никогда.
   *
   * Положительная форма: хэш с `timestamp` удаляется, только если в нём есть и
   * поля `JOB_SHAPE_FIELDS` — их storeJob.lua пишет тем же HMSET.
   *
   * Живой `:lock` — «под локом» (`locked`), до индексов: задачу держит воркер,
   * и это O(1) рубеж независимо от того, успел ли переход попасть в индекс.
   *
   * Вложенная очередь — ПЕРВОЙ проверкой: для каждого `:` в jobId префикс до
   * него проверяется как очередь BullMQ (`nestedQueue` ниже — та же функция).
   * Префикс BullMQ может содержать `:`, и ключ `<prefix>:x:<id>` очереди `x` с
   * префиксом `<prefix>` по форме — сирота этой очереди с id `x:<id>`. Ключи
   * `P:meta`/`P:id` собираются в скрипте, как и `de:`/`dn:` (см. выше): в
   * кластере они в слоте хэш-тега префикса очереди.
   */
  checkAndDelete: `
local jobKey = KEYS[1]
local jobId = ARGV[1]
local indexCount = tonumber(ARGV[2])
local graceMs = tonumber(ARGV[3])
local dryRun = ARGV[4] == '${DRY_RUN_ARG.on}'
local queuePrefix = ARGV[5]
local maxListScan = tonumber(ARGV[6])
local untimestampedConfirmed = ARGV[7] == '${UNTIMESTAMPED_ARG.confirmed}'
if redis.call('TYPE', jobKey)['ok'] ~= 'hash' then
  return ${V.notHash}
end
${IS_QUEUE_LUA}
local from = 1
while true do
  local colon = string.find(jobId, ':', from, true)
  if not colon then
    break
  end
  if isQueue(queuePrefix .. string.sub(jobId, 1, colon - 1)) then
    return ${V.nested}
  end
  from = colon + 1
end
if redis.call('EXISTS', jobKey .. ':${K.JOB_LOCK_SUFFIX}') == 1 then
  return ${V.locked}
end
-- Список длиннее потолка, в начале которого id не нашёлся: членство не
-- проверено. Решение — после ВСЕХ индексов: id может найтись в следующем.
local listUnverified = false
for i = 2, 1 + indexCount do
  local index = KEYS[i]
  local kind = redis.call('TYPE', index)['ok']
  if kind == 'list' then
    if maxListScan > 0 then
      if redis.call('LPOS', index, jobId, 'MAXLEN', maxListScan) then
        return ${V.alive}
      end
      if redis.call('LLEN', index) > maxListScan then
        listUnverified = true
      end
    elseif redis.call('LPOS', index, jobId) then
      return ${V.alive}
    end
  elseif kind == 'zset' then
    if redis.call('ZSCORE', index, jobId) then
      return ${V.alive}
    end
  elseif kind == 'set' then
    if redis.call('SISMEMBER', index, jobId) == 1 then
      return ${V.alive}
    end
  elseif kind ~= 'none' then
    return ${V.unverifiable}
  end
end
if listUnverified then
  return ${V.unverifiable}
end
local fields = redis.call('HMGET', jobKey, '${K.JOB_TIMESTAMP_FIELD}', '${K.JOB_DEDUPLICATION_ID_FIELD}', ${K.JOB_SHAPE_FIELDS.map((field) => `'${field}'`).join(', ')})
if not fields[1] and not untimestampedConfirmed then
  return ${V.untimestamped}
end
if fields[1] then
  for i = 3, #fields do
    if not fields[i] then
      return ${V.notJob}
    end
  end
  local created = tonumber(fields[1])
  if not created then
    return ${V.unknownAge}
  end
  local t = redis.call('TIME')
  local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  if now - created < graceMs then
    return ${V.young}
  end
end
local dedupKey = nil
local dedupNextKey = nil
if fields[2] then
  dedupNextKey = queuePrefix .. '${K.DEDUPLICATION_NEXT_KEY_PREFIX}' .. fields[2]
  if redis.call('HGET', dedupNextKey, '${K.DEDUPLICATION_NEXT_JOB_ID_FIELD}') == jobId then
    return ${V.alive}
  end
  dedupKey = queuePrefix .. '${K.DEDUPLICATION_KEY_PREFIX}' .. fields[2]
end
if dryRun then
  return ${V.wouldDelete}
end
redis.call('DEL', jobKey)
for i = 2 + indexCount, #KEYS do
  redis.call('DEL', KEYS[i])
end
-- Ключ дедупликации, указывающий на удалённую сироту, иначе держал бы id
-- дедупликации навсегда (keepLastIfActive и режим без ttl ставят его без
-- срока): каждая новая задача с этим id молча «дедуплицировалась» бы в
-- несуществующую. Снимается так же, как при удалении задачи самим BullMQ, —
-- вместе с dn:<id>, который ждал завершения этой (никогда не завершимой) задачи.
if dedupKey and redis.call('GET', dedupKey) == jobId then
  redis.call('DEL', dedupKey, dedupNextKey)
end
return ${V.deleted}
`,
  /**
   * KEYS[1] — `P:meta` (только для маршрута в кластере: слот хэш-тега префикса).
   * ARGV[1] — префикс `P` возможной вложенной очереди. Ответ: 1 — по `P` живёт
   * очередь BullMQ (признаки — `BULLMQ_KEYS.NESTED_QUEUE_*`), 0 — нет. Та же
   * функция, что в `checkAndDelete`.
   */
  nestedQueue: `${IS_QUEUE_LUA}
return isQueue(ARGV[1]) and 1 or 0
`,
  /** KEYS[1] — `<очередь>:meta`. ARGV[1] — поле версии. Ответ — значение или nil. */
  readQueueVersion: `
return redis.call('HGET', KEYS[1], ARGV[1])
`,
  /** KEYS[1] — лок прогона. ARGV: токен, срок мс. Ответ: 1 — взят, 0 — занят другим прогоном. */
  acquireRunLock: `
if redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX') then
  return 1
end
return 0
`,
  /** KEYS[1] — лок прогона. ARGV: токен. Снимается только СВОЙ: истёкший и взятый другим не трогается. */
  releaseRunLock: `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`,
} as const;
