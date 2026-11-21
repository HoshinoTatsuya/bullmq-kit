import { FAIR_SHARE_PRELUDE as PRELUDE } from './fair-share-prelude.constant';

/**
 * Скрипты общего пула со справедливой долей групп. Время — `TIME` сервера.
 * Ключи: пул (ZSET держателей, член — `<длина группы>:<группа><держатель>`,
 * очко — дедлайн), ожидающие группы (ZSET группа → дедлайн ожидания), веса
 * (HASH группа → вес). Все — из `KEYS`: в Redis Cluster префикс с хэш-тегом.
 *
 * Доля группы — `max(1, ⌊потолок · вес / Σ весов активных⌋)`; активные — те,
 * у кого есть держатели, и те, кто недавно упёрся (ждёт). Группа берёт слот,
 * если пул не полон И она ниже своей доли — либо никто из ЖДУЩИХ не ниже своей
 * (свободное не простаивает: одна группа может занять весь пул, пока другим не
 * нужно). Вытеснения нет: группа сверх доли отдаёт слоты по мере завершения.
 */

export const FAIR_SHARE_SCRIPTS = {
  /**
   * ARGV: группа, держатель, срок мс, потолок пула, вес группы, срок ожидания мс.
   * Ответ `{код, retryAfterMs, доля, держит}`: 1 — занят, 2 — уже держит
   * (продлён), 0 — отказ; retryAfterMs при 0 — худший случай (истечение
   * держателя по сроку), иначе -1.
   */
  acquire: `${PRELUDE}
local group, holder = ARGV[1], ARGV[2]
local ttl, limit, weight, waitTtl = tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[6])
redis.call('HSET', weights, group, weight)
local own = member(group, holder)
local deadline = now + ttl
local function settle()
  local left = math.max(pexpireByLatest(pool), pexpireByLatest(waiting))
  redis.call('PEXPIRE', weights, math.max(1, left))
end
local members = redis.call('ZRANGE', pool, 0, -1)
local held = {}
for _, m in ipairs(members) do
  local g = groupOf(m)
  held[g] = (held[g] or 0) + 1
end
local active = { [group] = true }
for g in pairs(held) do
  active[g] = true
end
local waiters = redis.call('ZRANGE', waiting, 0, -1)
for _, g in ipairs(waiters) do
  active[g] = true
end
local function weightOf(g)
  return tonumber(redis.call('HGET', weights, g)) or 1
end
local sum = 0
for g in pairs(active) do
  sum = sum + weightOf(g)
end
local function share(g)
  return math.max(1, math.floor(limit * weightOf(g) / sum))
end
local mine = held[group] or 0
-- Уже держит — продление; доля считается так же, как при новом захвате.
if redis.call('ZSCORE', pool, own) then
  redis.call('ZADD', pool, deadline, own)
  settle()
  return {2, -1, share(group), mine}
end
local allowed = #members < limit
if allowed and mine >= share(group) then
  for _, g in ipairs(waiters) do
    if g ~= group and (held[g] or 0) < share(g) then
      allowed = false
      break
    end
  end
end
if allowed then
  redis.call('ZADD', pool, deadline, own)
  redis.call('ZREM', waiting, group)
  settle()
  return {1, -1, share(group), mine + 1}
end
redis.call('ZADD', waiting, now + waitTtl, group)
settle()
local wait = -1
if #members >= limit then
  local item = redis.call('ZRANGE', pool, #members - limit, #members - limit, 'WITHSCORES')
  wait = math.max(1, tonumber(item[2]) - now)
else
  local earliest = redis.call('ZRANGE', pool, 0, 0, 'WITHSCORES')
  if earliest[2] then
    wait = math.max(1, tonumber(earliest[2]) - now)
  end
end
return {0, wait, share(group), mine}
`,
  /** ARGV: группа, держатель. Снять слот; ответ 1 — был, 0 — нет. */
  release: `${PRELUDE}
return redis.call('ZREM', pool, member(ARGV[1], ARGV[2]))
`,
  /** Держатели пула плоским списком `{группа, держатель, остаток мс, ...}` по возрастанию дедлайна. */
  holders: `${PRELUDE}
local items = redis.call('ZRANGE', pool, 0, -1, 'WITHSCORES')
local reply = {}
for i = 1, #items, 2 do
  local g, h = groupOf(items[i])
  reply[#reply + 1] = g
  reply[#reply + 1] = h
  reply[#reply + 1] = math.max(1, tonumber(items[i + 1]) - now)
end
return reply
`,
} as const;
