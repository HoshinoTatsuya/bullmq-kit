import type Redis from 'ioredis';

/**
 * Дождаться, пока очередь BullMQ запишет `meta.version`.
 *
 * Конструктор `Queue` пишет meta АСИНХРОННО после `waitUntilReady()` и не даёт
 * этого дождаться: тест, читавший meta сразу после готовности, гонялся с
 * записью и падал на `null` (флак на прогоне публикации 15.11). Ожидание —
 * ограниченное число обращений к Redis, без часов; запись не появилась —
 * отказ с причиной.
 */
export async function awaitQueueMeta(redis: Redis, queuePrefix: string): Promise<string> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const version = await redis.hget(`${queuePrefix}:meta`, 'version');
    if (version !== null) {
      return version;
    }
  }
  throw new Error(`очередь ${queuePrefix} не записала meta.version за 200 обращений к Redis`);
}
