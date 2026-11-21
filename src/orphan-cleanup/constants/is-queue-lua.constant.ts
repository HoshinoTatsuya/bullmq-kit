import { BULLMQ_KEYS as K } from './bullmq-keys.constant';

/**
 * Lua-функция `isQueue(P)`: живёт ли по префиксу `P` очередь BullMQ. Тип
 * проверяется до HEXISTS: тот же ключ мог оказаться хэшем задачи или строкой,
 * и HEXISTS по строке — отказ скрипта, а не «нет».
 */
export const IS_QUEUE_LUA = `
local function isQueue(p)
  local meta = p .. ':${K.NESTED_QUEUE_META_SUFFIX}'
  if redis.call('TYPE', meta)['ok'] == 'hash' then
    for _, field in ipairs({ ${K.NESTED_QUEUE_META_FIELDS.map((field) => `'${field}'`).join(', ')} }) do
      if redis.call('HEXISTS', meta, field) == 1 then
        return true
      end
    end
  end
  return redis.call('TYPE', p .. ':${K.NESTED_QUEUE_ID_SUFFIX}')['ok'] == 'string'
end
`;
