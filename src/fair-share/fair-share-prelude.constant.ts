/**
 * Общее начало скриптов пула: время сервера, снятие просроченных держателей и
 * ожидающих, кодирование члена `<длина группы>:<группа><держатель>` (длина —
 * чтобы группа с `:` не сливалась с держателем) и срок ключа по последнему.
 */
export const FAIR_SHARE_PRELUDE = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local pool, waiting, weights = KEYS[1], KEYS[2], KEYS[3]
redis.call('ZREMRANGEBYSCORE', pool, '-inf', now)
redis.call('ZREMRANGEBYSCORE', waiting, '-inf', now)
local function member(group, holder)
  return string.len(group) .. ':' .. group .. holder
end
local function groupOf(m)
  local n = string.match(m, '^(%d+):')
  local length = tonumber(n)
  return string.sub(m, #n + 2, #n + 1 + length), string.sub(m, #n + 2 + length)
end
local function pexpireByLatest(key)
  local latest = redis.call('ZRANGE', key, -1, -1, 'WITHSCORES')
  if latest[2] then
    redis.call('PEXPIRE', key, math.max(1, tonumber(latest[2]) - now))
    return tonumber(latest[2]) - now
  end
  return 0
end
`;
