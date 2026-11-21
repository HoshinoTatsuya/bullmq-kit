import { randomUUID } from 'node:crypto';

import { Queue } from 'bullmq';
import Redis from 'ioredis';
import { awaitQueueMeta } from '../testing/await-queue-meta';
import { CLEANUP_DEFAULTS } from './constants/cleanup-defaults.constant';
import { CLEANUP_SCRIPTS } from './constants/cleanup-scripts.constant';
import { OrphanedJobsCleanup } from './orphaned-jobs-cleanup';
import type { CleanupPipeline, CleanupRedis, CleanupScanner } from './types/cleanup-redis.type';

/**
 * Прогон уборки как процесс: лок и его срок, курсор между прогонами, узлы
 * обхода функцией, одновременные `run()`, скрипты по SHA, батч по длине
 * списков, ошибки в ответах, пагинация снимка, выборка причин «не удалили».
 * На НАСТОЯЩЕМ Redis и BullMQ; без REDIS_TEST_URL набор пропускается, и
 * jest-assert-no-skipped валит прогон.
 */
const url = process.env.REDIS_TEST_URL;
const describeRedis = url ? describe : describe.skip;

/** Поля, которые storeJob.lua BullMQ пишет в хэш задачи (кроме `timestamp`). */
const JOB_SHAPE = { name: 'lost', data: '{}', opts: '{}', delay: '0', priority: '0' };

/** Клиент, у которого методы подменены по имени, остальное — настоящее. */
const patched = <T extends object>(target: T, overrides: Record<string, unknown>): T =>
  new Proxy(target, {
    get(inner, property, receiver) {
      if (typeof property === 'string' && property in overrides) {
        return overrides[property];
      }
      const value = Reflect.get(inner, property, receiver);
      return typeof value === 'function' ? value.bind(inner) : value;
    },
  });

describeRedis('OrphanedJobsCleanup: прогон [нужен REDIS_TEST_URL]', () => {
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
    // Запись meta асинхронна — см. awaitQueueMeta.
    await awaitQueueMeta(redis, prefix());
  });
  afterEach(async () => {
    await queue.obliterate({ force: true });
    await queue.close();
  });

  const orphan = async (jobId: string, fields: Record<string, string> = {}) => {
    await redis.hset(`${prefix()}:${jobId}`, {
      ...JOB_SHAPE,
      timestamp: String(Date.now() - 600_000),
      ...fields,
    });
  };
  /** Живая задача: хэш старше грейса и id в `completed`. */
  const live = async (jobId: string) => {
    await orphan(jobId);
    await redis.zadd(`${prefix()}:completed`, 1, jobId);
  };

  it('хэш вне индексов с живым :lock — «под локом», не удаляется; без лока — удаляется', async () => {
    await orphan('held');
    await redis.set(`${prefix()}:held:lock`, 'worker-token', 'PX', 60_000);
    await orphan('lost');
    const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], graceMs: 0 }).run();
    expect(report).toMatchObject({ deleted: 1, kept: { locked: 1, alive: 0 } });
    expect(report.keptKeys.locked).toEqual([`${prefix()}:held`]);
    await expect(redis.type(`${prefix()}:held`)).resolves.toBe('hash');
    await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(0);
  });

  /**
   * Одна порция SCAN, хэшей в ней больше `maxJobsPerRun`, и живые идут
   * первыми — прежде каждый прогон начинал порцию с начала и упирался в те же
   * живые: сироты дальше по порции не убирались никогда.
   */
  it('порция SCAN больше потолка хэшей: следующий прогон продолжает с места внутри порции', async () => {
    for (let i = 0; i < 100; i += 1) {
      await live(`live-${i}`);
    }
    for (const id of ['lost-a', 'lost-b', 'lost-c']) {
      await orphan(id);
    }
    // Контроль фикстуры: все ключи очереди приходят одной порцией.
    const [cursor] = await redis.scan('0', 'MATCH', `${prefix()}:*`, 'COUNT', 10_000);
    expect(cursor).toBe('0');
    const cleanup = new OrphanedJobsCleanup(redis, {
      queuePrefixes: [prefix()],
      maxJobsPerRun: 10,
      scanCount: 10_000,
    });
    let deleted = 0;
    let runs = 0;
    for (; runs < 15 && deleted < 3; runs += 1) {
      deleted += (await cleanup.run()).deleted;
    }
    expect(deleted).toBe(3);
    // Прогонов не больше, чем нужно пройти порцию по 10: 103 хэша → 11.
    expect(runs).toBeLessThanOrEqual(11);
    expect(await redis.zcard(`${prefix()}:completed`)).toBe(100);
  });

  it('scanClients функцией — зовётся в каждом прогоне (после failover обход идёт по новым узлам)', async () => {
    await orphan('lost-1');
    let calls = 0;
    const nodes = (): CleanupScanner[] => {
      calls += 1;
      return [redis];
    };
    const cleanup = new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], scanClients: nodes });
    expect((await cleanup.run()).deleted).toBe(1);
    await orphan('lost-2');
    expect((await cleanup.run()).deleted).toBe(1);
    expect(calls).toBe(2);
    const none = new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], scanClients: () => [] });
    await expect(none.run()).rejects.toThrow('scanClients');
  });

  it('одновременные run() одного экземпляра — один прогон, один отчёт', async () => {
    await orphan('lost');
    let scans = 0;
    const counted = patched(redis, {
      scan: (...args: Parameters<CleanupScanner['scan']>) => {
        scans += 1;
        return redis.scan(...args);
      },
    });
    const cleanup = new OrphanedJobsCleanup(counted, { queuePrefixes: [prefix()] });
    const [first, second] = await Promise.all([cleanup.run(), cleanup.run()]);
    expect(second).toBe(first);
    expect(first.deleted).toBe(1);
    expect(scans).toBe(1);
    // После завершения — новый прогон, а не прежний отчёт.
    const third = await cleanup.run();
    expect(third).not.toBe(first);
  });

  /**
   * Срок лока прогона — `maxRunMs` плюс запас: прогон, дошедший до потолка
   * длительности, ещё дорабатывает последний батч, и лок не должен истечь
   * раньше — иначе стартовал бы второй прогон параллельно.
   */
  it('лок прогона держится дольше maxRunMs: срок — maxRunMs плюс запас', async () => {
    await orphan('lost');
    const lockKey = `${prefix()}-cleanup-lock`;
    let pttl = 0;
    const observed = patched(redis, {
      scan: async (...args: Parameters<CleanupScanner['scan']>) => {
        pttl = await redis.pttl(lockKey);
        return await redis.scan(...args);
      },
    });
    await new OrphanedJobsCleanup(observed, {
      queuePrefixes: [prefix()],
      runLockKey: lockKey,
      maxRunMs: 5_000,
    }).run();
    expect(pttl).toBeGreaterThan(5_000 + CLEANUP_DEFAULTS.RUN_LOCK_MARGIN_MS - 1_000);
    expect(pttl).toBeLessThanOrEqual(5_000 + CLEANUP_DEFAULTS.RUN_LOCK_MARGIN_MS);
  });

  /**
   * Отказ Redis без прототипа (или с бросающим `toString`): текст причины
   * строится без `String()` в лоб, иначе сам разбор отказа бросал бы TypeError
   * и ронял весь прогон — ровно то, от чего ветка отказа защищает. `evalsha`
   * снят, чтобы скрипты шли через EVAL и отказ подставлялся по тексту скрипта.
   */
  const REASONS = [
    ['без прототипа', (): unknown => Object.create(null)],
    [
      'с бросающим toString',
      (): unknown => ({
        toString: () => {
          throw new Error('toString');
        },
      }),
    ],
  ] as const;
  const failingOn = (script: string, reason: () => unknown) =>
    patched(redis, {
      evalsha: undefined,
      eval: (body: string, ...args: unknown[]) => {
        if (body === script) {
          return Promise.reject(reason());
        }
        return (redis.eval as (...all: unknown[]) => Promise<unknown>).call(redis, body, ...args);
      },
    });

  it.each(REASONS)(
    'meta не читается, отказ %s: очередь — в unsupportedQueues, прогон не падает',
    async (_l, reason) => {
      await orphan('lost');
      const report = await new OrphanedJobsCleanup(failingOn(CLEANUP_SCRIPTS.readQueueVersion, reason), {
        queuePrefixes: [prefix()],
        graceMs: 0,
      }).run();
      expect(report.deleted).toBe(0);
      expect(report.unsupportedQueues).toHaveLength(1);
      expect(report.unsupportedQueues[0]?.reason).toMatch(/^meta очереди не читается: \S/);
    },
  );

  it.each(REASONS)(
    'индексы не читаются, отказ %s: очередь — в unsupportedQueues, прогон не падает',
    async (_l, reason) => {
      await orphan('lost');
      const report = await new OrphanedJobsCleanup(
        patched(redis, { lrange: () => Promise.reject(reason()) }),
        {
          queuePrefixes: [prefix()],
          graceMs: 0,
        },
      ).run();
      expect(report.deleted).toBe(0);
      expect(report.unsupportedQueues).toHaveLength(1);
      expect(report.unsupportedQueues[0]?.reason).toMatch(/^индексы очереди не читаются: \S/);
    },
  );

  it.each(REASONS)(
    'лок прогона не снят, отказ %s: предупреждение в журнал, отчёт возвращён',
    async (_l, reason) => {
      await orphan('lost');
      const warnings: string[] = [];
      const report = await new OrphanedJobsCleanup(failingOn(CLEANUP_SCRIPTS.releaseRunLock, reason), {
        queuePrefixes: [prefix()],
        runLockKey: `${prefix()}-cleanup-lock`,
        graceMs: 0,
        logger: { log: () => undefined, warn: (message: string) => warnings.push(message) },
      }).run();
      expect(report.deleted).toBe(1);
      expect(warnings).toEqual([
        expect.stringMatching(/^\[OrphanCleanup\] лок прогона не снят \(истечёт сам\): \S/),
      ]);
    },
  );

  it('ошибка в ответе пайплайна — «не смогли проверить» (errors), а не удалено', async () => {
    await orphan('lost-1');
    await orphan('lost-2');
    const broken = patched(redis, {
      pipeline: (): CleanupPipeline => {
        const real = redis.pipeline();
        const wrapper: CleanupPipeline = {
          eval: (...args) => {
            real.eval(...args);
            return wrapper;
          },
          evalsha: (...args) => {
            real.evalsha(...args);
            return wrapper;
          },
          // Первый ответ — ошибка команды (скрипт исполнился у второго, не у первого).
          exec: async () => {
            const replies = (await real.exec()) ?? [];
            return replies.map((reply, index) =>
              index === 0 ? [new Error('BUSY Redis is busy running a script'), null] : reply,
            ) as Array<[Error | null, unknown]>;
          },
        };
        return wrapper;
      },
    });
    const report = await new OrphanedJobsCleanup(broken, {
      queuePrefixes: [prefix()],
      graceMs: 0,
      dryRun: true,
    }).run();
    expect(report).toMatchObject({ orphans: 1, deleted: 0, kept: { errors: 1 } });
    expect(report.keptKeys.errors).toHaveLength(1);
  });

  it('снимок длиннее страницы читается целиком: задачи со второй страницы не становятся кандидатами', async () => {
    for (let i = 0; i < 5; i += 1) {
      await orphan(`w-${i}`);
      await redis.rpush(`${prefix()}:wait`, `w-${i}`);
    }
    for (let i = 0; i < 5; i += 1) {
      await live(`c-${i}`);
    }
    const report = await new OrphanedJobsCleanup(redis, {
      queuePrefixes: [prefix()],
      graceMs: 0,
      snapshotPage: 2,
    }).run();
    // Кандидатом не стал никто: alive — счёт скрипта, а до скрипта дошли бы только пропущенные снимком.
    expect(report).toMatchObject({ scanned: 10, deleted: 0, kept: { alive: 0 } });
  });

  /**
   * Скрипт проверки — ~2.5 КБ текста на каждого кандидата при EVAL. По SHA он
   * идёт EVALSHA; после SCRIPT FLUSH (рестарт Redis, failover) — NOSCRIPT, и
   * вызовы повторяются через EVAL, который заодно кладёт скрипт в кэш.
   */
  it('кандидаты — EVALSHA; после SCRIPT FLUSH — повтор через EVAL, следующий прогон снова по SHA', async () => {
    const calls = { eval: 0, evalsha: 0 };
    const counted = patched(redis, {
      pipeline: (): CleanupPipeline => {
        const real = redis.pipeline();
        const wrapper: CleanupPipeline = {
          eval: (...args) => {
            calls.eval += 1;
            real.eval(...args);
            return wrapper;
          },
          evalsha: (...args) => {
            calls.evalsha += 1;
            real.evalsha(...args);
            return wrapper;
          },
          exec: () => real.exec() as Promise<Array<[Error | null, unknown]>>,
        };
        return wrapper;
      },
    });
    const cleanup = new OrphanedJobsCleanup(counted, { queuePrefixes: [prefix()] });
    for (const id of ['a', 'b', 'c']) {
      await orphan(id);
    }
    await redis.script('FLUSH');
    expect((await cleanup.run()).deleted).toBe(3);
    expect(calls).toEqual({ eval: 3, evalsha: 3 });

    for (const id of ['d', 'e']) {
      await orphan(id);
    }
    expect((await cleanup.run()).deleted).toBe(2);
    expect(calls).toEqual({ eval: 3, evalsha: 5 });
  });

  it('батч удаления — по длине списков: «кандидатов × длина wait» не больше maxListWorkPerBatch', async () => {
    await redis.rpush(`${prefix()}:wait`, ...Array.from({ length: 30 }, (_, i) => `foreign-${i}`));
    for (let i = 0; i < 5; i += 1) {
      await orphan(`lost-${i}`);
    }
    let pipelines = 0;
    const counted = patched(redis, {
      pipeline: () => {
        pipelines += 1;
        return redis.pipeline();
      },
    });
    const report = await new OrphanedJobsCleanup(counted, {
      queuePrefixes: [prefix()],
      maxListWorkPerBatch: 60,
    }).run();
    expect(report.deleted).toBe(5);
    // 60 / 30 = 2 кандидата на пайплайн: 5 кандидатов — 3 пайплайна (без потолка был бы один).
    expect(pipelines).toBe(3);
  });

  it('потолок снимка списков по умолчанию — конечный: список длиннее не читается в память', () => {
    expect(CLEANUP_DEFAULTS.MAX_SNAPSHOT_LIST_SIZE).toBe(1_000_000);
    const withoutLlen = patched(redis as unknown as CleanupRedis, { llen: undefined });
    expect(() => new OrphanedJobsCleanup(withoutLlen, { queuePrefixes: [prefix()] })).toThrow('llen');
    expect(
      () =>
        new OrphanedJobsCleanup(withoutLlen, {
          queuePrefixes: [prefix()],
          maxSnapshotListSize: Number.POSITIVE_INFINITY,
        }),
    ).not.toThrow();
  });

  /**
   * Прогон, остановленный потолком во ВТОРОЙ очереди, следующий начинает с неё
   * же: иначе первая очередь каждый раз съедала бы бюджет, и до второй уборка
   * не доходила бы никогда.
   */
  it('прогон, усечённый во второй очереди, продолжает с неё, а не с первой', async () => {
    const second = `${prefix()}-second`;
    const version = await redis.hget(`${prefix()}:meta`, 'version');
    await redis.hset(`${second}:meta`, 'version', version as string);
    try {
      // Первая очередь без задач, во второй две сироты: потолок 1 обрывает прогон во второй.
      for (const id of ['lost-a', 'lost-b']) {
        await redis.hset(`${second}:${id}`, { ...JOB_SHAPE, timestamp: String(Date.now() - 600_000) });
      }
      const patterns: string[] = [];
      const recorded = patched(redis, {
        scan: (...args: Parameters<CleanupScanner['scan']>) => {
          patterns.push(String(args[2]));
          return redis.scan(...args);
        },
      });
      const cleanup = new OrphanedJobsCleanup(recorded, {
        queuePrefixes: [prefix(), second],
        maxJobsPerRun: 1,
      });
      const first = await cleanup.run();
      expect(first).toMatchObject({ truncatedBy: 'jobs', deleted: 1 });
      patterns.length = 0;

      const next = await cleanup.run();

      expect(patterns[0]).toBe(`${second}:*`);
      expect(next.deleted).toBe(1);
    } finally {
      await redis.del(`${second}:meta`, `${second}:lost-a`, `${second}:lost-b`);
    }
  });

  it('клиент с keyPrefix — отказ в конструкторе: ключи, собранные в скрипте, его не получат', () => {
    const prefixed = patched(redis, { options: { ...redis.options, keyPrefix: 'app:' } });
    expect(() => new OrphanedJobsCleanup(prefixed, { queuePrefixes: [prefix()] })).toThrow('keyPrefix');
  });

  it('режим отчёта: dryRun с выборкой ключей по каждой причине «не удалили»', async () => {
    await orphan('lost');
    await orphan('young', { timestamp: String(Date.now()) });
    await redis.hset(`${prefix()}:late-write`, 'progress', '50');
    await redis.hset(`${prefix()}:foreign`, { name: 'x', timestamp: String(Date.now() - 600_000) });
    const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], dryRun: true }).run();
    expect(report.orphanKeys).toEqual([`${prefix()}:lost`]);
    expect(report.keptKeys).toMatchObject({
      young: [`${prefix()}:young`],
      untimestamped: [`${prefix()}:late-write`],
      notJob: [`${prefix()}:foreign`],
      alive: [],
    });
    await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(1);
  });
});
