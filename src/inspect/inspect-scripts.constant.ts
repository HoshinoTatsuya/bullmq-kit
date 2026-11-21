import { BULLMQ_KEYS as K } from '../orphan-cleanup/constants/bullmq-keys.constant';

/**
 * Снимок здоровья очереди ОДНИМ скриптом: счёт каждого индекса, возраст самой
 * старой ждущей, просрочка самой ранней отложенной, активные без лока — в один
 * момент по времени сервера Redis, без расхождения между командами.
 *
 * KEYS — индексы статусов в порядке `INSPECT_STATUSES`. ARGV: префикс очереди с
 * `:`, потолок просмотра active. Хэши задач и `:lock` собираются в скрипте из
 * префикса — как у самой BullMQ и уборки (в кластере — слот хэш-тега префикса).
 *
 * Ответ: `{now, счёты..., oldestWaitTimestamp|-1, delayedWakeAt|-1, paused 0|1,
 * activeScanned, число ключей не своего типа, эти ключи..., stuck...}`.
 * Время пробуждения отложенной — очко delayed BullMQ 5 и 6: `wakeAt * 0x1000 + счётчик`.
 * Пауза — поле `paused` хэша `meta` (BullMQ 5 и 6; в 6 ждущие при этом остаются в `wait`).
 *
 * Ждущие — хвосты `wait` И `paused`: BullMQ 5 при паузе переносит ждущих в
 * `paused` (RENAME) и кладёт туда новые. Active — с ХВОСТА: воркер берёт задачу
 * `RPOPLPUSH wait active`, новая встаёт в голову, самые старые (и самые
 * вероятные зависшие) — в хвосте. Индекс не своего типа не читается (WRONGTYPE
 * ронял бы весь снимок) и называется в ответе: «0» за ним — не «пусто».
 */
export const INSPECT_SCRIPTS = {
  queueHealth: `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local prefix = ARGV[1]
local maxActive = tonumber(ARGV[2])
local reply = { now }
local usable = {}
local unexpected = {}
for i, key in ipairs(KEYS) do
  local kind = redis.call('TYPE', key)['ok']
  local expected = ARGV[2 + i]
  usable[i] = kind == expected
  if kind ~= 'none' and kind ~= expected then
    unexpected[#unexpected + 1] = key
  end
  if not usable[i] then
    reply[#reply + 1] = 0
  elseif kind == 'list' then
    reply[#reply + 1] = redis.call('LLEN', key)
  else
    reply[#reply + 1] = redis.call('ZCARD', key)
  end
end
local oldest = -1
for _, i in ipairs({ 1, 3 }) do
  if usable[i] then
    local tail = redis.call('LINDEX', KEYS[i], -1)
    if tail then
      local ts = tonumber(redis.call('HGET', prefix .. tail, '${K.JOB_TIMESTAMP_FIELD}'))
      if ts and (oldest < 0 or ts < oldest) then
        oldest = ts
      end
    end
  end
end
reply[#reply + 1] = oldest
local wake = -1
if usable[4] then
  local first = redis.call('ZRANGE', KEYS[4], 0, 0, 'WITHSCORES')
  if first[2] then
    wake = math.floor(tonumber(first[2]) / 4096)
  end
end
reply[#reply + 1] = wake
local meta = prefix .. '${K.NESTED_QUEUE_META_SUFFIX}'
if redis.call('TYPE', meta)['ok'] == 'hash' and redis.call('HEXISTS', meta, '${K.QUEUE_PAUSED_FIELD}') == 1 then
  reply[#reply + 1] = 1
else
  reply[#reply + 1] = 0
end
local active = {}
if usable[2] then
  active = redis.call('LRANGE', KEYS[2], -maxActive, -1)
end
reply[#reply + 1] = #active
reply[#reply + 1] = #unexpected
for _, key in ipairs(unexpected) do
  reply[#reply + 1] = key
end
for i = #active, 1, -1 do
  local id = active[i]
  if redis.call('EXISTS', prefix .. id .. ':${K.JOB_LOCK_SUFFIX}') == 0 then
    reply[#reply + 1] = id
  end
end
return reply
`,
} as const;
