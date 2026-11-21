/**
 * Скрипт ведра токенов группы. Время — `TIME` сервера Redis (часы реплик
 * расходятся). Состояние — хэш `{tokens, ts}`; срок ключа — время полного
 * наполнения ведра: истёкший ключ и есть полное ведро, и пространство ключей
 * из внешнего ввода (арендатор, хост) не копится.
 *
 * KEYS[1] — ведро группы. ARGV: ёмкость (целое > 0), окно мс (за окно
 * наливается ёмкость токенов), сколько взять (целое > 0, не больше ёмкости).
 * Ответ `{код, retryAfterMs, tokensLeft}`: код 1 — взято, 0 — мало токенов;
 * retryAfterMs при 0 — ТОЧНО через сколько мс токенов хватит (без чужих
 * взятий), иначе -1.
 */
export const RATE_LIMIT_SCRIPTS = {
  take: `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + tonumber(t[2]) / 1000
local capacity = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local rate = capacity / windowMs
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if not tokens or not ts then
  tokens = capacity
  ts = now
end
if now > ts then
  tokens = math.min(capacity, tokens + (now - ts) * rate)
  ts = now
end
local ok = 0
local wait = -1
if tokens >= cost then
  tokens = tokens - cost
  ok = 1
else
  wait = math.ceil((cost - tokens) / rate)
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(ts))
redis.call('PEXPIRE', KEYS[1], math.max(1, math.ceil((capacity - tokens) / rate)))
return {ok, wait, tostring(tokens)}
`,
} as const;
