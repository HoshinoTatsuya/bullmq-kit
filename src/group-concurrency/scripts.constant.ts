/**
 * Скрипты Lua потолка групп. Время — `TIME` сервера Redis, а не часы реплики:
 * у реплик часы расходятся, и срок слота, поставленный одной, другая сочла бы
 * истёкшим раньше времени. Каждый скрипт сначала снимает просроченных
 * держателей, затем ставит ключу срок по самому позднему держателю: ключ
 * группы задаётся внешним вводом (проект, IP), и без срока пространство
 * ключей росло бы без потолка.
 *
 * Все ключи — только из `KEYS` (скрипт не собирает имён сам): в Redis Cluster
 * скрипт обязан объявить каждый ключ, а ключи одного вызова — лежать в одном
 * слоте (для `acquireMany` — общий хэш-тег в префиксе, `{app}:slots`).
 */
const PRELUDE = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local function prune(key)
  redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
end
local function expireByLatest(key)
  local latest = redis.call('ZRANGE', key, -1, -1, 'WITHSCORES')
  if latest[2] then
    redis.call('PEXPIRE', key, math.max(1, tonumber(latest[2]) - now))
  end
end
-- Через сколько мс слот освободится ПО СРОКУ, если никто не снимет свой раньше.
-- Захват проходит при ZCARD < limit, значит истечь должны (ZCARD - limit + 1)
-- самых ранних держателей — нужен дедлайн держателя с индексом ZCARD - limit:
-- потолок, сниженный ниже числа держателей, ждёт не первого, а N-го.
-- -1 — по сроку не освободится никогда (limit 0: группа закрыта).
local function retryAfter(key, limit)
  if limit <= 0 then
    return -1
  end
  local index = redis.call('ZCARD', key) - limit
  if index < 0 then
    return 0
  end
  local item = redis.call('ZRANGE', key, index, index, 'WITHSCORES')
  -- Индекс всегда в [0, ZCARD - 1]: зовётся при ZCARD >= limit > 0 в том же
  -- скрипте, после prune, и ключ между ZCARD и ZRANGE не меняется (проба на
  -- Redis 7.4: все сочетания 0..6 держателей и потолков 0..7). Проверка —
  -- против будущей правки индекса: отказ с причиной вместо безымянной
  -- «arithmetic on a nil value»; не тихая подсказка, которая спрятала бы ошибку,
  -- и не redis.error_reply: вложенный в массив ответа, он дошёл бы до клиента
  -- элементом, а не отказом.
  if not item[2] then
    error('retryAfter: index ' .. index .. ' out of range for ' .. key)
  end
  return math.max(1, tonumber(item[2]) - now)
end
`;

export const GROUP_CONCURRENCY_SCRIPTS = {
  /**
   * KEYS[1] — группа. ARGV: держатель, срок мс, потолок (-1 — без потолка).
   * Ответ `{код, retryAfterMs}`: код 1 — занят новый слот, 2 — держатель уже
   * держит (продлён, слот не тратится), 0 — потолок исчерпан; retryAfterMs
   * значим только при 0 (-1 — по сроку не освободится), иначе -1.
   */
  acquire: `${PRELUDE}
local key = KEYS[1]
prune(key)
local deadline = now + tonumber(ARGV[2])
if redis.call('ZSCORE', key, ARGV[1]) then
  redis.call('ZADD', key, deadline, ARGV[1])
  expireByLatest(key)
  return {2, -1}
end
local limit = tonumber(ARGV[3])
if limit >= 0 and redis.call('ZCARD', key) >= limit then
  return {0, retryAfter(key, limit)}
end
redis.call('ZADD', key, deadline, ARGV[1])
expireByLatest(key)
return {1, -1}
`,
  /**
   * KEYS[1] — группа. ARGV: держатель, срок мс, потолок (-1 — без потолка).
   * Как `acquire`, но слот, который держатель УЖЕ держит, не трогает: резерв
   * короче рабочего срока, и продление им укоротило бы слот идущей работы.
   * Ответ — как у `acquire` (2 — уже держит, срок прежний).
   */
  reserve: `${PRELUDE}
local key = KEYS[1]
prune(key)
if redis.call('ZSCORE', key, ARGV[1]) then
  return {2, -1}
end
local limit = tonumber(ARGV[3])
if limit >= 0 and redis.call('ZCARD', key) >= limit then
  return {0, retryAfter(key, limit)}
end
redis.call('ZADD', key, now + tonumber(ARGV[2]), ARGV[1])
expireByLatest(key)
return {1, -1}
`,
  /**
   * KEYS — группы (≥ 1). ARGV: держатель, срок мс, потолки по группам в том же
   * порядке (-1 — без потолка). Всё или ничего: сначала проверяются ВСЕ
   * группы, и только если ни одна не упёрлась в потолок, слот занимается (или
   * продлевается) во всех. При отказе не меняется ничего — даже срок слотов,
   * которые держатель уже держит.
   * Ответ `{статус, retryAfterMs, коды}`: статус 1 — заняты все, 0 — отказ;
   * retryAfterMs при отказе — МАКСИМУМ по упёршимся группам (освободиться
   * должны все), -1 — хоть одна закрыта; коды по группам: 1 — свободна,
   * 2 — уже держит, 0 — потолок.
   */
  acquireMany: `${PRELUDE}
local holder = ARGV[1]
local deadline = now + tonumber(ARGV[2])
local codes = {}
local blocked = false
local wait = 0
local closed = false
for i, key in ipairs(KEYS) do
  prune(key)
  local limit = tonumber(ARGV[2 + i])
  if redis.call('ZSCORE', key, holder) then
    codes[i] = 2
  elseif limit >= 0 and redis.call('ZCARD', key) >= limit then
    codes[i] = 0
    blocked = true
    local after = retryAfter(key, limit)
    if after < 0 then
      closed = true
    elseif after > wait then
      wait = after
    end
  else
    codes[i] = 1
  end
end
if blocked then
  if closed then
    return {0, -1, codes}
  end
  return {0, wait, codes}
end
for _, key in ipairs(KEYS) do
  redis.call('ZADD', key, deadline, holder)
  expireByLatest(key)
end
return {1, -1, codes}
`,
  /**
   * KEYS[1] — группа. ARGV: держатель, срок мс. Продлевает ТОЛЬКО живой слот
   * (`XX`): продление после снятия или истечения заново заняло бы слот мимо
   * потолка. Ответ: 1 — продлён, 0 — слота нет.
   */
  heartbeat: `${PRELUDE}
local key = KEYS[1]
prune(key)
if not redis.call('ZSCORE', key, ARGV[1]) then
  return 0
end
redis.call('ZADD', key, 'XX', now + tonumber(ARGV[2]), ARGV[1])
expireByLatest(key)
return 1
`,
  /**
   * Захват с очередью ожидания. KEYS: группа, очередь ожидающих (ZSET, очко —
   * момент прихода), сроки регистрации ожидающих (ZSET, очко — дедлайн).
   * ARGV: держатель, срок слота мс, потолок (-1 — без потолка), срок
   * регистрации ожидающего мс, канал группы.
   *
   * Свободные слоты достаются ожидающим ПО ПОРЯДКУ прихода: держатель занимает
   * слот, только если его место в очереди меньше числа свободных. Ожидающий,
   * чья регистрация истекла (процесс умер, не дождавшись), снимается первым
   * делом — иначе он держал бы очередь вечно. Занявший будит следующих
   * ожидающих по числу оставшихся свободных: несколько снятий разом будят
   * каждое ПЕРВОГО, и без цепочки остальные ждали бы перепроверки. Ответ
   * `{код, retryAfterMs, место}` — коды как у `acquire`.
   */
  acquireQueued: `${PRELUDE}
local key, queue, deadlines = KEYS[1], KEYS[2], KEYS[3]
local holder = ARGV[1]
prune(key)
local dead = redis.call('ZRANGEBYSCORE', deadlines, '-inf', now)
for _, member in ipairs(dead) do
  redis.call('ZREM', queue, member)
end
redis.call('ZREMRANGEBYSCORE', deadlines, '-inf', now)
local deadline = now + tonumber(ARGV[2])
local function leave()
  redis.call('ZREM', queue, holder)
  redis.call('ZREM', deadlines, holder)
end
if redis.call('ZSCORE', key, holder) then
  redis.call('ZADD', key, deadline, holder)
  expireByLatest(key)
  leave()
  return {2, -1, 0}
end
local limit = tonumber(ARGV[3])
redis.call('ZADD', queue, 'NX', now, holder)
redis.call('ZADD', deadlines, now + tonumber(ARGV[4]), holder)
local rank = redis.call('ZRANK', queue, holder)
local free = rank + 1
if limit >= 0 then
  free = limit - redis.call('ZCARD', key)
end
if rank < free then
  redis.call('ZADD', key, deadline, holder)
  expireByLatest(key)
  leave()
  if limit >= 0 then
    local remaining = limit - redis.call('ZCARD', key)
    if remaining > 0 then
      for _, waiter in ipairs(redis.call('ZRANGE', queue, 0, remaining - 1)) do
        redis.call('PUBLISH', ARGV[5], waiter)
      end
    end
  end
  expireByLatest(deadlines)
  redis.call('PEXPIRE', queue, math.max(1, redis.call('PTTL', deadlines)))
  return {1, -1, 0}
end
expireByLatest(deadlines)
redis.call('PEXPIRE', queue, math.max(1, redis.call('PTTL', deadlines)))
return {0, retryAfter(key, limit), rank}
`,
  /**
   * KEYS: группа, очередь ожидающих, сроки регистрации ожидающих. ARGV:
   * держатель, канал группы. Снять слот и, если он был, разбудить ПЕРВОГО
   * ЖИВОГО ожидающего (публикация его имени): освободился один слот —
   * просыпается один, а не все разом. Ожидающие с истёкшей регистрацией
   * (процесс умер, не сняв себя) снимаются до выбора: иначе пробуждение ушло бы
   * умершему, а живой следующий ждал бы перепроверки.
   * Ответ: 1 — слот был, 0 — нет.
   */
  releaseNotify: `
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 1 then
  local t = redis.call('TIME')
  local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  for _, member in ipairs(redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now)) do
    redis.call('ZREM', KEYS[2], member)
  end
  redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', now)
  local head = redis.call('ZRANGE', KEYS[2], 0, 0)
  if head[1] then
    redis.call('PUBLISH', ARGV[2], head[1])
  end
end
return removed
`,
  /** KEYS: очередь ожидающих, сроки регистрации. ARGV: держатель. Уйти из очереди (таймаут, отмена). */
  leaveQueue: `
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
`,
  /** KEYS[1] — группа. Число живых держателей. */
  count: `${PRELUDE}
local key = KEYS[1]
prune(key)
return redis.call('ZCARD', key)
`,
  /**
   * KEYS[1] — группа. Живые держатели плоским списком
   * `{держатель, остаток мс, ...}` по возрастанию дедлайна.
   */
  holders: `${PRELUDE}
local key = KEYS[1]
prune(key)
local items = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
local reply = {}
for i = 1, #items, 2 do
  reply[#reply + 1] = items[i]
  reply[#reply + 1] = math.max(1, tonumber(items[i + 1]) - now)
end
return reply
`,
  /** KEYS[1] — группа. Снять всех держателей; ответ — сколько ЖИВЫХ было. */
  clear: `${PRELUDE}
local key = KEYS[1]
prune(key)
local live = redis.call('ZCARD', key)
redis.call('DEL', key)
return live
`,
} as const;

/**
 * Коды ответа скриптов захвата. Значения сравниваются в коде пакета, поэтому
 * живут здесь, а не литералами по месту.
 */
export const SCRIPT_REPLY = {
  limit: 0,
  acquired: 1,
  held: 2,
  /** retryAfterMs: по сроку не освободится (группа закрыта) или неприменимо. */
  never: -1,
  /** Статус `acquireMany`: отказ / заняты все. */
  manyBlocked: 0,
  manyAcquired: 1,
} as const;
