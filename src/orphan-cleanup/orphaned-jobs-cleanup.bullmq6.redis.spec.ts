import { randomUUID } from 'node:crypto';

import * as bullmq from 'bullmq';
import Redis from 'ioredis';
import { awaitQueueMeta } from '../testing/await-queue-meta';
import { OrphanedJobsCleanup } from './orphaned-jobs-cleanup';

/**
 * Двойники, которых приносит BullMQ 6, — на НАСТОЯЩЕЙ BullMQ этого прогона
 * (`BULLMQ_UNDER_TEST`) и Redis. Уборка удаляет ключи, поэтому каждое новое
 * семейство ключей проверяется не «по документации», а тем, что живые ключи
 * после прогонов на месте, а настоящая сирота рядом — удалена (иначе зелёный
 * тест означал бы «уборка не работала вовсе»).
 */
const url = process.env.REDIS_TEST_URL;
const describeRedis = url ? describe : describe.skip;

const JOB_SHAPE = { name: 'lost', data: '{}', opts: '{}', delay: '0', priority: '0' };

/** Relay — с BullMQ 6.x (в 5 и 6.0 его нет); тип — свой: в 5 его нет и в объявлениях. */
type RelayLike = {
  subscribe(pattern: string, listener: () => void): Promise<unknown>;
  publish(topic: string, data: unknown, opts?: { retain?: number }): Promise<unknown>;
  close(): Promise<void>;
};
type RelayCtor = new (options: { connection: object; prefix?: string; namespace?: string }) => RelayLike;
const Relay = (bullmq as unknown as { Relay?: RelayCtor }).Relay;
const installedMajor = Number(
  (require(`${process.env.BULLMQ_UNDER_TEST ?? 'bullmq'}/package.json`) as { version: string }).version.split(
    '.',
  )[0],
);

describeRedis('OrphanedJobsCleanup: двойники BullMQ 6 [нужен REDIS_TEST_URL]', () => {
  let redis: Redis;
  let name: string;
  let queue: bullmq.Queue;
  let connection: { host: string; port: number; maxRetriesPerRequest: null };
  const prefix = () => `bull:${name}`;

  const orphan = async (jobId: string) => {
    await redis.hset(`${prefix()}:${jobId}`, { ...JOB_SHAPE, timestamp: String(Date.now() - 10 * 60_000) });
  };

  beforeAll(() => {
    redis = new Redis(url as string, { maxRetriesPerRequest: null });
    const { hostname, port } = new URL(url as string);
    connection = { host: hostname, port: Number(port), maxRetriesPerRequest: null };
  });
  afterAll(async () => {
    await redis.quit();
  });
  beforeEach(async () => {
    name = `q-${randomUUID()}`;
    queue = new bullmq.Queue(name, { connection });
    await queue.waitUntilReady();
    await awaitQueueMeta(redis, prefix());
  });
  afterEach(async () => {
    await queue.obliterate({ force: true });
    await queue.close();
  });

  /**
   * Relay (BullMQ 6.x) держит ключи в `<prefix>::relay:{<ns>}:…`: хэши узлов и
   * удержанных сообщений БЕЗ `timestamp`, то есть по форме — «хэш без
   * timestamp», который `deleteUntimestamped` удаляет по второму наблюдению.
   * По умолчанию пустой сегмент держит их вне пространства очередей; префикс
   * relay, совпавший с префиксом очереди, кладёт их ВНУТРЬ — и это тоже не
   * повод их удалить.
   */
  it('ключи Relay рядом с очередью и внутри её пространства не удаляются даже с deleteUntimestamped', async () => {
    if (Relay === undefined) {
      // Relay нет — нет и его ключей; прогон этой BullMQ проверяет остальное.
      expect([5, 6]).toContain(installedMajor);
      return;
    }
    const relays = [
      new Relay({ connection, namespace: name }),
      new Relay({ connection, prefix: prefix(), namespace: name }),
    ];
    try {
      for (const relay of relays) {
        await relay.subscribe('jobs.*', () => undefined);
        await relay.publish('jobs.done', { ok: true }, { retain: 600_000 });
      }
      const relayKeys = async () =>
        [...(await redis.keys(`bull::relay:*`)), ...(await redis.keys(`${prefix()}::relay:*`))].sort();
      const before = await relayKeys();
      // Контроль фикстуры: ключи обоих relay есть, внутри очереди — тоже, и среди них есть хэши.
      expect(before.filter((key) => key.startsWith(`${prefix()}::relay:`)).length).toBeGreaterThanOrEqual(4);
      expect(before.filter((key) => key.startsWith('bull::relay:')).length).toBeGreaterThanOrEqual(4);
      const hashes: string[] = [];
      for (const key of before) {
        if ((await redis.type(key)) === 'hash') {
          hashes.push(key);
        }
      }
      expect(hashes.filter((key) => key.startsWith(`${prefix()}::relay:`)).length).toBeGreaterThanOrEqual(2);
      await orphan('lost');

      const cleanup = new OrphanedJobsCleanup(redis, {
        queuePrefixes: [prefix()],
        graceMs: 0,
        deleteUntimestamped: true,
      });
      const first = await cleanup.run();
      const second = await cleanup.run();

      expect(await relayKeys()).toEqual(before);
      expect(first.deleted + second.deleted).toBe(1);
      await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(0);
    } finally {
      for (const relay of relays) {
        await relay.close();
      }
      const leftovers = await redis.keys(`${prefix()}::relay:*`);
      if (leftovers.length > 0) {
        await redis.del(...leftovers);
      }
    }
  }, 30_000);

  /**
   * Пауза: BullMQ 5 переносила `wait` в список `paused`, BullMQ 6 оставляет
   * задачи в `wait`, а `paused` читает только как наследство очередей,
   * поставленных на паузу пятой версией (`resume` возвращает его в `wait`).
   * Задачи в обоих местах живы — и до возобновления, и после.
   */
  it('пауза: задачи приостановленной очереди и наследство списка paused живы до и после resume', async () => {
    await queue.add('n', {}, { jobId: 'before-pause' });
    await queue.pause();
    await queue.add('n', {}, { jobId: 'after-pause' });
    // Наследство BullMQ 5: хэш задачи и её id в списке paused.
    await redis.hset(`${prefix()}:legacy`, { ...JOB_SHAPE, timestamp: String(Date.now() - 600_000) });
    await redis.rpush(`${prefix()}:paused`, 'legacy');
    await orphan('lost');
    const where = async (id: string) =>
      (await redis.lpos(`${prefix()}:wait`, id)) !== null
        ? 'wait'
        : (await redis.lpos(`${prefix()}:paused`, id)) !== null
          ? 'paused'
          : 'нигде';
    // Контроль фикстуры: раскладка паузы — та, что у мажора этого прогона.
    const paused = installedMajor >= 6 ? 'wait' : 'paused';
    expect([await where('before-pause'), await where('after-pause')]).toEqual([paused, paused]);

    const whilePaused = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], graceMs: 0 }).run();
    await queue.resume();
    await orphan('lost-after-resume');
    const afterResume = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], graceMs: 0 }).run();

    expect([whilePaused.deleted, afterResume.deleted]).toEqual([1, 1]);
    for (const id of ['before-pause', 'after-pause', 'legacy']) {
      await expect(redis.type(`${prefix()}:${id}`)).resolves.toBe('hash');
    }
    await expect(redis.exists(`${prefix()}:lost`, `${prefix()}:lost-after-resume`)).resolves.toBe(0);
    // После resume наследство переехало в wait (обе версии) — и всё ещё живо.
    await expect(where('legacy')).resolves.toBe('wait');
  });
});
