import { randomUUID } from 'node:crypto';

import { Queue } from 'bullmq';
import Redis from 'ioredis';
import { awaitQueueMeta } from '../testing/await-queue-meta';
import { OrphanedJobsCleanup } from './orphaned-jobs-cleanup';
import type { CleanupScanner } from './types/cleanup-redis.type';

/**
 * Прогон, оборванный потолком, и следующий за ним: собранные кандидаты не
 * теряются, а узлы обхода не голодают. На НАСТОЯЩЕМ Redis и BullMQ; узлы и
 * порции SCAN — детерминированные двойники поверх настоящего SCAN, чтобы
 * граница порции не зависела от раскладки хэш-таблицы Redis.
 */
const url = process.env.REDIS_TEST_URL;
const describeRedis = url ? describe : describe.skip;

/** Поля, которые storeJob.lua BullMQ пишет в хэш задачи (кроме `timestamp`). */
const JOB_SHAPE = { name: 'lost', data: '{}', opts: '{}', delay: '0', priority: '0' };

/**
 * Узел обхода: ключи очереди, отобранные `owns`, порциями по `size` в
 * алфавитном порядке; курсор — номер следующей порции. `onScan` — после
 * каждой порции (двигать часы).
 */
const node = (
  redis: Redis,
  owns: (key: string) => boolean,
  size: number,
  onScan: () => void = () => undefined,
): CleanupScanner => ({
  scan: async (cursor, _match, pattern) => {
    const all: string[] = [];
    let real = '0';
    do {
      const [next, keys] = await redis.scan(real, 'MATCH', pattern, 'COUNT', 1_000);
      real = next;
      all.push(...keys);
    } while (real !== '0');
    const mine = all.filter(owns).sort();
    const start = Number(cursor) * size;
    const next = start + size >= mine.length ? '0' : String(Number(cursor) + 1);
    onScan();
    return [next, mine.slice(start, start + size)];
  },
});

describeRedis('OrphanedJobsCleanup: бюджет прогона [нужен REDIS_TEST_URL]', () => {
  let redis: Redis;
  let name: string;
  let queue: Queue;
  const prefix = () => `bull:${name}`;

  beforeAll(() => {
    redis = new Redis(url as string, { maxRetriesPerRequest: null });
  });
  afterAll(async () => {
    await redis.quit();
  });
  beforeEach(async () => {
    name = `q-${randomUUID()}`;
    const { hostname, port } = new URL(url as string);
    queue = new Queue(name, {
      connection: { host: hostname, port: Number(port), maxRetriesPerRequest: null },
    });
    await queue.waitUntilReady();
    await awaitQueueMeta(redis, prefix());
  });
  afterEach(async () => {
    await queue.obliterate({ force: true });
    await queue.close();
  });

  const orphan = async (jobId: string) => {
    await redis.hset(`${prefix()}:${jobId}`, { ...JOB_SHAPE, timestamp: String(Date.now() - 600_000) });
  };
  /** Двойник живой задачи: хэш старше грейса и id в `completed`. */
  const live = async (jobId: string) => {
    await orphan(jobId);
    await redis.zadd(`${prefix()}:completed`, 1, jobId);
  };
  const isJob = (key: string) => !key.slice(prefix().length + 1).includes(':') && !key.endsWith(':meta');
  const exists = async (ids: readonly string[]) =>
    Promise.all(ids.map(async (id) => [id, await redis.exists(`${prefix()}:${id}`)] as const));

  /**
   * Время вышло на границе порций SCAN: курсор уже сохранён за порцией, а её
   * кандидаты не проверены. Прежде они пропускались до следующего полного
   * обхода (курсор их перешагнул); теперь переходят в следующий прогон.
   */
  it('кандидаты, не проверенные до потолка времени, проверяются следующим прогоном', async () => {
    // Порция 1: a-live, a-lost-1, a-lost-2; порция 2: b-live, b-lost-1, b-lost-2.
    for (const id of ['a-live', 'b-live']) {
      await live(id);
    }
    const lost = ['a-lost-1', 'a-lost-2', 'b-lost-1', 'b-lost-2'];
    for (const id of lost) {
      await orphan(id);
    }
    let clock = 0;
    let advance = true;
    const scanner = node(redis, isJob, 3, () => {
      if (advance) {
        clock += 60_000;
      }
    });
    const cleanup = new OrphanedJobsCleanup(redis, {
      queuePrefixes: [prefix()],
      maxRunMs: 30_000,
      scanClients: [scanner],
      now: () => clock,
    });

    const first = await cleanup.run();
    expect(first).toMatchObject({ truncatedBy: 'time', deleted: 0, scanned: 3 });

    // Между прогонами один кандидат ожил — его удалять нельзя.
    await redis.zadd(`${prefix()}:completed`, 2, 'a-lost-2');
    advance = false;
    const second = await cleanup.run();

    expect(second).toMatchObject({ truncatedBy: null, deleted: 3 });
    expect(await exists(['a-live', 'b-live', ...lost])).toEqual([
      ['a-live', 1],
      ['b-live', 1],
      ['a-lost-1', 0],
      ['a-lost-2', 1],
      ['b-lost-1', 0],
      ['b-lost-2', 0],
    ]);
  });

  /**
   * Потолок оборвал прогон на ВТОРОМ узле: прежде следующий прогон начинал с
   * первого узла с нуля (его курсор удалён как пройденный), снова упирался в
   * тот же потолок, и второй узел не дообходился никогда.
   */
  it('прогон, оборванный на втором узле, следующим прогоном продолжает его, а не первый с нуля', async () => {
    for (const id of ['n1-live-1', 'n1-live-2', 'n1-live-3']) {
      await live(id);
    }
    const lost = ['n2-lost-1', 'n2-lost-2', 'n2-lost-3'];
    for (const id of lost) {
      await orphan(id);
    }
    const n1 = node(redis, (key) => isJob(key) && key.includes(':n1-'), 100);
    const n2 = node(redis, (key) => isJob(key) && key.includes(':n2-'), 100);
    const cleanup = new OrphanedJobsCleanup(redis, {
      queuePrefixes: [prefix()],
      maxJobsPerRun: 3,
      scanClients: [n1, n2],
    });
    let deleted = 0;
    for (let run = 0; run < 4; run += 1) {
      deleted += (await cleanup.run()).deleted;
    }
    expect(deleted).toBe(3);
    expect(await exists(lost)).toEqual(lost.map((id) => [id, 0]));
    expect(await redis.zcard(`${prefix()}:completed`)).toBe(3);
    expect(await exists(['n1-live-1', 'n1-live-2', 'n1-live-3'])).toEqual([
      ['n1-live-1', 1],
      ['n1-live-2', 1],
      ['n1-live-3', 1],
    ]);
  });

  it('пройдя все узлы, цикл обхода начинается заново: сирота на первом узле находится снова', async () => {
    const n1 = node(redis, (key) => isJob(key) && key.includes(':n1-'), 100);
    const n2 = node(redis, (key) => isJob(key) && key.includes(':n2-'), 100);
    const cleanup = new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], scanClients: [n1, n2] });
    await orphan('n1-lost-1');
    expect((await cleanup.run()).deleted).toBe(1);
    await orphan('n1-lost-2');
    expect((await cleanup.run()).deleted).toBe(1);
  });

  /**
   * Класс находки «индекс не своего типа ронял весь снимок инспектора»: в
   * уборке тот же WRONGTYPE на снимке индексов ронял весь прогон, и очереди
   * после сломанной не убирались никогда.
   */
  it('индекс не своего типа — очередь не убирается и названа в отчёте, следующие убираются', async () => {
    const broken = `${prefix()}-broken`;
    const version = await redis.hget(`${prefix()}:meta`, 'version');
    try {
      await redis.hset(`${broken}:meta`, 'version', version as string);
      await redis.set(`${broken}:wait`, 'x');
      await redis.hset(`${broken}:lost`, { ...JOB_SHAPE, timestamp: String(Date.now() - 600_000) });
      await orphan('lost');
      const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [broken, prefix()] }).run();
      expect(report.deleted).toBe(1);
      expect(report.unsupportedQueues.map((queue) => queue.prefix)).toEqual([broken]);
      expect(report.unsupportedQueues[0]?.reason).toContain('WRONGTYPE');
      await expect(redis.exists(`${broken}:lost`)).resolves.toBe(1);
      await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(0);
    } finally {
      await redis.del(`${broken}:meta`, `${broken}:wait`, `${broken}:lost`);
    }
  });
});
