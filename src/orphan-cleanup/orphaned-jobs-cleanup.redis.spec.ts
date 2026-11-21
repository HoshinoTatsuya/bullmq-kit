import { randomUUID } from 'node:crypto';

import { FlowProducer, Queue, QueueKeys, Worker } from 'bullmq';
import Redis from 'ioredis';
import { awaitQueueMeta } from '../testing/await-queue-meta';
import { BULLMQ_KEYS } from './constants/bullmq-keys.constant';
import { CLEANUP_DEFAULTS } from './constants/cleanup-defaults.constant';
import { CLEANUP_SCRIPTS, LIST_SCAN_ARG } from './constants/cleanup-scripts.constant';
import { OrphanedJobsCleanup } from './orphaned-jobs-cleanup';
import { unsupportedBullmqVersion } from './unsupported-bullmq-version';

/**
 * На НАСТОЯЩЕМ BullMQ и Redis: раскладку ключей пишет сама библиотека BullMQ,
 * и проверка «эта задача жива» имеет смысл только против неё. Адрес —
 * REDIS_TEST_URL (`pnpm test` поднимает redis в docker); без адреса набор
 * пропускается, и jest-assert-no-skipped валит прогон.
 */
const url = process.env.REDIS_TEST_URL;
const describeRedis = url ? describe : describe.skip;

/** Индексы статусов — из словаря пакета, с типом ключа, которым их пишет BullMQ. */
const INDEXES: ReadonlyArray<readonly [string, 'zset' | 'list' | 'set']> = [
  ...BULLMQ_KEYS.ZSET_STATUSES.map((status) => [status, 'zset'] as const),
  ...BULLMQ_KEYS.LIST_STATUSES.map((status) => [status, 'list'] as const),
  ...BULLMQ_KEYS.SET_STATUSES.map((status) => [status, 'set'] as const),
];

/**
 * `meta.version`, который пишет BullMQ ЭТОГО прогона: `BULLMQ_UNDER_TEST`
 * подменяет `bullmq` псевдонимом (5.72, 5.81, 6.0, последняя 6).
 */
const BULLMQ_META_VERSION = `bullmq:${(require(`${process.env.BULLMQ_UNDER_TEST ?? 'bullmq'}/package.json`) as { version: string }).version}`;

/** Поля, которые storeJob.lua BullMQ пишет в хэш задачи (кроме `timestamp`). */
const JOB_SHAPE = { name: 'lost', data: '{}', opts: '{}', delay: '0', priority: '0' };

describeRedis('OrphanedJobsCleanup на BullMQ [нужен REDIS_TEST_URL]', () => {
  let redis: Redis;
  let name: string;
  let queue: Queue;
  let flows: FlowProducer;
  const prefix = () => `bull:${name}`;

  beforeAll(() => {
    redis = new Redis(url as string, { maxRetriesPerRequest: null });
  });
  afterAll(async () => {
    await redis.quit();
  });
  beforeEach(async () => {
    name = `q-${randomUUID()}`;
    // Параметры, а не экземпляр клиента: у bullmq своя копия ioredis, и типы
    // экземпляров разных копий несовместимы.
    const { hostname, port } = new URL(url as string);
    const connection = { host: hostname, port: Number(port), maxRetriesPerRequest: null };
    queue = new Queue(name, { connection });
    flows = new FlowProducer({ connection });
    // Очередь пишет `meta.version` — как в проде, где очередь создаёт
    // приложение: без него уборка очередь не трогает (гейт версии BullMQ).
    // Запись meta асинхронна — см. awaitQueueMeta.
    await queue.waitUntilReady();
    await awaitQueueMeta(redis, prefix());
  });
  afterEach(async () => {
    await queue.obliterate({ force: true });
    await queue.close();
    await flows.close();
  });

  /**
   * Сирота — СТАРЫЙ хэш (старше грейса по умолчанию): настоящая сирота
   * остаётся после обрыва давно. Свежий хэш держал бы грейс, и тесты на гонки
   * были бы зелёными не по той причине.
   */
  /** Ещё одна «очередь» той же версии BullMQ: meta.version копируется у настоящей очереди теста. */
  const sameVersionAs = async (other: string): Promise<void> => {
    const version = await redis.hget(`${prefix()}:meta`, 'version');
    expect(version).toBe(BULLMQ_META_VERSION);
    await redis.hset(`${other}:meta`, 'version', version as string);
  };

  const orphan = async (jobId: string, fields: Record<string, string> = {}) => {
    await redis.hset(`${prefix()}:${jobId}`, {
      ...JOB_SHAPE,
      timestamp: String(Date.now() - 10 * 60_000),
      ...fields,
    });
  };

  it('удаляет хэш без индекса и не трогает свежие задачи (грейс по умолчанию; без грейса — ниже, по каждому индексу)', async () => {
    await queue.add('waiting', {}, { jobId: 'w1' });
    await queue.add('delayed', {}, { jobId: 'd1', delay: 60_000 });
    await queue.add('prioritized', {}, { jobId: 'p1', priority: 5 });
    await flows.add({
      name: 'parent',
      queueName: name,
      opts: { jobId: 'parent1' },
      children: [{ name: 'child', queueName: name, opts: { jobId: 'c1' } }],
    });
    await queue.pause();
    await queue.add('paused', {}, { jobId: 'ps1' });
    await orphan('lost1');
    await orphan('lost2');

    // Выборка не выродилась: родитель flow действительно в waiting-children,
    // а сироты действительно лежат хэшами.
    await expect(redis.zscore(`${prefix()}:waiting-children`, 'parent1')).resolves.not.toBeNull();
    await expect(redis.type(`${prefix()}:lost1`)).resolves.toBe('hash');

    const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()] }).run();

    expect(report.deleted).toBe(2);
    await expect(redis.exists(`${prefix()}:lost1`, `${prefix()}:lost2`)).resolves.toBe(0);
    for (const id of ['w1', 'd1', 'p1', 'parent1', 'c1', 'ps1']) {
      await expect(redis.type(`${prefix()}:${id}`)).resolves.toBe('hash');
    }
  });

  it('jobId с двоеточиями — тоже задача: живая остаётся, сирота удаляется', async () => {
    await queue.add('live', {}, { jobId: `${randomUUID()}::project-7` });
    const lost = `${randomUUID()}::project-8`;
    await orphan(lost);
    const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()] }).run();
    expect(report.scanned).toBe(2);
    expect(report.deleted).toBe(1);
    await expect(redis.exists(`${prefix()}:${lost}`)).resolves.toBe(0);
  });

  it('пер-задачные ключи (:lock, :logs) не трогаются: удалённый :lock отдал бы задачу второму воркеру', async () => {
    await queue.add('live', {}, { jobId: 'a1' });
    await redis.set(`${prefix()}:a1:lock`, 'token');
    await redis.rpush(`${prefix()}:a1:logs`, 'line');
    await redis.set(`${prefix()}:gone:lock`, 'token');
    const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()] }).run();
    expect(report.deleted).toBe(0);
    await expect(
      redis.exists(`${prefix()}:a1:lock`, `${prefix()}:a1:logs`, `${prefix()}:gone:lock`),
    ).resolves.toBe(3);
  });

  it('живым хэшам TTL НЕ ставится: задача, простоявшая сутки, не теряет тело', async () => {
    await queue.add('waiting', {}, { jobId: 'w1' });
    await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()] }).run();
    await expect(redis.pttl(`${prefix()}:w1`)).resolves.toBe(-1);
  });

  it('задача, попавшая в индекс ПОСЛЕ свежего снимка, переживает поштучную проверку', async () => {
    // Задача появляется в индексе после снимка — в момент сборки пайплайна
    // удаления, — и держит её только проверка внутри скрипта удаления.
    await orphan('mover');
    let injected = false;
    const racing = new Proxy(redis, {
      get(target, property, receiver) {
        if (property === 'pipeline' && !injected) {
          return () => {
            injected = true;
            void target.rpush(`${prefix()}:wait`, 'mover');
            return target.pipeline();
          };
        }
        const value = Reflect.get(target, property, receiver);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    const report = await new OrphanedJobsCleanup(racing, { queuePrefixes: [prefix()] }).run();
    expect(injected).toBe(true);
    expect(report.deleted).toBe(0);
    await expect(redis.type(`${prefix()}:mover`)).resolves.toBe('hash');
  });

  /**
   * Цена прогона. Прежняя версия слала 7 команд на
   * КАЖДЫЙ хэш: 1000 задач — ~7000 команд в одном exec, инструментация ioredis
   * делала спан на команду, и экспортёр OTel ронял очередь («Dropped 4303
   * spans» на проде), выбивая трейсы всего сервиса. Живые задачи не
   * должны стоить команд: снимок индексов читает каждый индекс страницами, а
   * поштучная проверка — только для кандидатов в сироты. Утверждение — про
   * порядок величины, а не про точное число.
   */
  it('число команд не растёт вместе с числом живых задач', async () => {
    await queue.addBulk(
      Array.from({ length: 1000 }, (_, i) => ({
        name: 'n',
        data: {},
        opts: { jobId: `job-${i}::1`, delay: 600_000 },
      })),
    );
    let calls = 0;
    let pipelines = 0;
    const counted = new Proxy(redis, {
      get(target, property, receiver) {
        const value = Reflect.get(target, property, receiver) as unknown;
        if (typeof value !== 'function') {
          return value;
        }
        return (...args: unknown[]) => {
          calls += 1;
          if (property === 'pipeline') {
            pipelines += 1;
          }
          return (value as (...a: unknown[]) => unknown).apply(target, args);
        };
      },
    });

    const report = await new OrphanedJobsCleanup(counted, { queuePrefixes: [prefix()] }).run();

    // Контроль: задачи действительно просмотрены, а не пропущены.
    expect(report.scanned).toBeGreaterThanOrEqual(1000);
    expect(report.deleted).toBe(0);
    expect(pipelines).toBe(0);
    expect(calls).toBeLessThan(50);
  });

  /**
   * Дедупликация с `keepLastIfActive`: пока первая задача активна, вторая с тем
   * же id ждёт в ХЭШЕ `dn:<id>`, которого нет ни в одном индексе статусов.
   * Без `dn` среди вложенных служебных пространств уборка удаляла его как
   * сироту, и последняя задача молча не выполнялась.
   */
  it('отложенная задача дедупликации (dn:<id>) не удаляется', async () => {
    const { hostname, port } = new URL(url as string);
    const connection = { host: hostname, port: Number(port), maxRetriesPerRequest: null };
    let release: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const worker = new Worker(name, async () => await gate, { connection });
    try {
      const first = await queue.add('d', {}, { deduplication: { id: 'dedup-1', keepLastIfActive: true } });
      for (let i = 0; i < 100 && !(await first.isActive()); i++) {
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      expect(await first.isActive()).toBe(true);
      await queue.add('d', {}, { deduplication: { id: 'dedup-1', keepLastIfActive: true } });
      const next = `${prefix()}:dn:dedup-1`;
      // Контроль фикстуры: отложенная задача — хэш вне индексов.
      expect(await redis.type(next)).toBe('hash');

      await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()] }).run();

      expect(await redis.type(next)).toBe('hash');
    } finally {
      release();
      await worker.close();
    }
  });

  /**
   * Задача в КАЖДОМ индексе статуса со старым `timestamp` и без грейса: держит
   * её только индекс. Два пути: индекс виден снимку (задача не становится
   * кандидатом) и индекс появился после снимка (держит проверка в скрипте).
   * Прежний тест «все состояния» был зелёным из-за грейса: задачи свежие.
   */
  describe('задача в каждом индексе статуса — жива и без грейса', () => {
    const place = async (key: string, kind: 'zset' | 'list' | 'set', id: string) => {
      if (kind === 'zset') {
        await redis.zadd(key, 1, id);
      } else if (kind === 'list') {
        await redis.rpush(key, id);
      } else {
        await redis.sadd(key, id);
      }
    };

    it('выборка не выродилась: индексов столько, сколько у BullMQ 5', () => {
      expect(INDEXES).toHaveLength(9);
    });

    it.each(INDEXES)('%s (%s) — в снимке', async (status, kind) => {
      await orphan('held');
      await orphan('lost');
      await place(`${prefix()}:${status}`, kind, 'held');
      const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], graceMs: 0 }).run();
      // Снимок отсёк задачу до скрипта: кандидатом она не стала (alive — счёт скрипта).
      // Множество stalled в снимок не входит — его держит только скрипт.
      expect(report).toMatchObject({ deleted: 1, kept: { alive: status === 'stalled' ? 1 : 0 } });
      await expect(redis.type(`${prefix()}:held`)).resolves.toBe('hash');
      await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(0);
    });

    it.each(INDEXES)('%s (%s) — после снимка, держит скрипт', async (status, kind) => {
      await orphan('held');
      await orphan('lost');
      const racing = new Proxy(redis, {
        get(target, property, receiver) {
          if (property === 'pipeline') {
            return () => {
              void place(`${prefix()}:${status}`, kind, 'held');
              return target.pipeline();
            };
          }
          const value = Reflect.get(target, property, receiver);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      });
      const report = await new OrphanedJobsCleanup(racing, { queuePrefixes: [prefix()], graceMs: 0 }).run();
      expect(report).toMatchObject({ deleted: 1, kept: { alive: 1 } });
      await expect(redis.type(`${prefix()}:held`)).resolves.toBe('hash');
    });

    /**
     * Настоящие задачи во всех состояниях, которые даёт BullMQ, — и сверка
     * словаря с тем, что BullMQ действительно пишет: каждый ключ очереди
     * (из `QueueKeys`), в котором лежит id задачи, обязан быть индексом
     * статуса словаря. Новый индекс в BullMQ или снятый из словаря статус —
     * красный здесь, а не удалённые живые задачи на проде.
     */
    it('настоящие задачи во всех состояниях живы без грейса, а словарь индексов покрывает все ключи с их id', async () => {
      const { hostname, port } = new URL(url as string);
      const connection = { host: hostname, port: Number(port), maxRetriesPerRequest: null };
      let open: () => void = () => undefined;
      const gate = new Promise<void>((resolve) => {
        open = resolve;
      });
      const worker = new Worker(
        name,
        async (job) => {
          if (job.name === 'fail') {
            throw new Error('упала');
          }
          if (job.name === 'hold') {
            await gate;
          }
          return 'ok';
        },
        { connection, autorun: false },
      );
      try {
        await queue.add('done', {}, { jobId: 'j-done' });
        await queue.add('fail', {}, { jobId: 'j-failed' });
        await queue.add('hold', {}, { jobId: 'j-active' });
        void worker.run();
        const settled = async (id: string, state: string) => {
          for (let i = 0; i < 250 && (await queue.getJobState(id)) !== state; i += 1) {
            await new Promise((resolve) => setTimeout(resolve, 20));
          }
          expect({ id, state: await queue.getJobState(id) }).toEqual({ id, state });
        };
        await settled('j-done', 'completed');
        await settled('j-failed', 'failed');
        await settled('j-active', 'active');
        await worker.pause(true);
        await queue.add('n', {}, { jobId: 'j-waiting' });
        await queue.add('n', {}, { jobId: 'j-delayed', delay: 600_000 });
        await queue.add('n', {}, { jobId: 'j-prioritized', priority: 3 });
        await flows.add({
          name: 'parent',
          queueName: name,
          opts: { jobId: 'j-parent' },
          children: [{ name: 'child', queueName: name, opts: { jobId: 'j-child', delay: 600_000 } }],
        });
        await queue.pause();
        await queue.add('n', {}, { jobId: 'j-paused' });
        // id не совпадают с именами индексов: хэш `<prefix>:failed` и есть индекс failed.
        const ids = [
          'done',
          'failed',
          'active',
          'waiting',
          'delayed',
          'prioritized',
          'parent',
          'child',
          'paused',
        ].map((state) => `j-${state}`);

        // Ключи очереди, в которых лежит id задачи, — по типу, как их пишет BullMQ.
        const holding: string[] = [];
        for (const suffix of Object.keys(new QueueKeys('bull').getKeys(name))) {
          const key = `${prefix()}:${suffix}`;
          const kind = await redis.type(key);
          const members =
            kind === 'list'
              ? await redis.lrange(key, 0, -1)
              : kind === 'zset'
                ? await redis.zrange(key, 0, '-1')
                : kind === 'set'
                  ? await redis.smembers(key)
                  : [];
          if (members.some((member) => ids.includes(member))) {
            holding.push(suffix);
          }
        }
        // Контроль фикстуры: состояния действительно разные — ключей с id не меньше семи.
        expect(holding.length).toBeGreaterThanOrEqual(7);
        expect(holding.filter((suffix) => !INDEXES.some(([status]) => status === suffix))).toEqual([]);

        const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], graceMs: 0 }).run();

        expect(report.deleted).toBe(0);
        expect(report.scanned).toBe(ids.length);
        for (const id of ids) {
          await expect(redis.type(`${prefix()}:${id}`)).resolves.toBe('hash');
        }
        // Активная задача доработала после уборки — её хэш был цел.
        open();
        await settled('j-active', 'completed');
      } finally {
        open();
        await worker.close();
      }
    });
  });

  describe('гейт версии BullMQ', () => {
    it('очередь незнакомой версии и без версии не убирается и названа; знакомая в том же прогоне — убирается', async () => {
      const newer = `${prefix()}-v7`;
      const bare = `${prefix()}-bare`;
      await redis.hset(`${newer}:meta`, 'version', 'bullmq:7.0.0');
      for (const target of [prefix(), newer, bare]) {
        await redis.hset(`${target}:lost`, { ...JOB_SHAPE, timestamp: String(Date.now() - 600_000) });
      }
      try {
        const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix(), newer, bare] }).run();
        expect(report.unsupportedQueues).toEqual([
          { prefix: newer, version: 'bullmq:7.0.0', reason: expect.stringContaining('мажор 7') },
          {
            prefix: bare,
            version: null,
            reason: expect.stringContaining('версии BullMQ в meta очереди нет'),
          },
        ]);
        await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(0);
        await expect(redis.exists(`${newer}:lost`, `${bare}:lost`)).resolves.toBe(2);
        expect(report.deleted).toBe(1);
      } finally {
        await redis.del(`${newer}:meta`, `${newer}:lost`, `${bare}:lost`);
      }
    });

    it('meta не хэш (WRONGTYPE) — эта очередь отказана, следующая за ней в том же прогоне убирается', async () => {
      const broken = `${prefix()}-broken`;
      await redis.set(`${broken}:meta`, 'not-a-hash');
      await orphan('lost');
      try {
        const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [broken, prefix()] }).run();
        expect(report.unsupportedQueues).toEqual([
          { prefix: broken, version: null, reason: expect.stringContaining('WRONGTYPE') },
        ]);
        expect(report.deleted).toBe(1);
      } finally {
        await redis.del(`${broken}:meta`);
      }
    });

    it('версия настоящей очереди этого набора — поддержанная (иначе набор проверял бы отказ, а не уборку)', async () => {
      const version = await redis.hget(`${prefix()}:meta`, 'version');
      expect(unsupportedBullmqVersion(version)).toBeNull();
      // Прогон против другой BullMQ — действительно против неё, а не против той же последней.
      expect(version).toBe(BULLMQ_META_VERSION);
    });
  });

  it('общий бюджет на несколько очередей: потолок — на прогон, а не на каждую очередь', async () => {
    const second = `${prefix()}-second`;
    await sameVersionAs(second);
    for (let i = 0; i < 2; i++) {
      await orphan(`lost${i}`);
      await redis.hset(`${second}:lost${i}`, { ...JOB_SHAPE, timestamp: String(Date.now() - 600_000) });
    }
    const report = await new OrphanedJobsCleanup(redis, {
      queuePrefixes: [prefix(), second],
      maxJobsPerRun: 3,
    }).run();
    // Бюджет на очередь дал бы 2 + 2 = 4; общий — ровно 3.
    expect(report.scanned).toBe(3);
    expect(report.deleted).toBe(3);
    expect(report.truncatedBy).toBe('jobs');
    await redis.del(`${second}:lost0`, `${second}:lost1`, `${second}:meta`);
  });

  it('dryRun: отчёт о сиротах, не удалено ничего; доля сирот — по найденным', async () => {
    await queue.add('live', {}, { jobId: 'a1' });
    await orphan('lost1');
    await redis.rpush(`${prefix()}:lost1:logs`, 'line');

    const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], dryRun: true }).run();

    expect(report).toMatchObject({ dryRun: true, scanned: 2, orphans: 1, deleted: 0, highOrphanShare: true });
    expect(report.orphanKeys).toEqual([`${prefix()}:lost1`]);
    await expect(redis.exists(`${prefix()}:lost1`, `${prefix()}:lost1:logs`)).resolves.toBe(2);

    const real = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()] }).run();
    expect(real).toMatchObject({ dryRun: false, orphans: 1, deleted: 1 });
    await expect(redis.exists(`${prefix()}:lost1`)).resolves.toBe(0);
  });

  it('хвосты сироты удаляются вместе с хэшем; сирота под :lock не удаляется; хвосты живой задачи не трогаются', async () => {
    const key = `${prefix()}:lost1`;
    await orphan('lost1');
    await redis.rpush(`${key}:logs`, 'line');
    await redis.sadd(`${key}:dependencies`, `${prefix()}:child`);
    await redis.hset(`${key}:processed`, 'child', '1');
    await redis.hset(`${key}:failed`, 'child', 'boom');
    await redis.zadd(`${key}:unsuccessful`, 1, 'child');
    // Вторая «сирота» с живым локом: её держит воркер — ни хэш, ни лок не удаляются.
    await orphan('locked1');
    await redis.set(`${prefix()}:locked1:lock`, 'token');
    await queue.add('live', {}, { jobId: 'a1' });
    await redis.rpush(`${prefix()}:a1:logs`, 'line');
    // Контроль фикстуры: все хвосты на месте до прогона.
    await expect(
      redis.exists(
        `${key}:logs`,
        `${key}:dependencies`,
        `${key}:processed`,
        `${key}:failed`,
        `${key}:unsuccessful`,
      ),
    ).resolves.toBe(5);

    const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()] }).run();

    expect(report).toMatchObject({ deleted: 1, kept: { locked: 1 } });
    await expect(
      redis.exists(
        key,
        `${key}:logs`,
        `${key}:dependencies`,
        `${key}:processed`,
        `${key}:failed`,
        `${key}:unsuccessful`,
      ),
    ).resolves.toBe(0);
    await expect(redis.get(`${prefix()}:locked1:lock`)).resolves.toBe('token');
    await expect(redis.type(`${prefix()}:locked1`)).resolves.toBe('hash');
    await expect(redis.exists(`${prefix()}:a1`, `${prefix()}:a1:logs`)).resolves.toBe(2);
  });

  it('журнал и выборка ключей: строка на каждую сироту, итог одной строкой, в отчёте — не больше потолка', async () => {
    for (const id of ['lost1', 'lost2', 'lost3']) {
      await orphan(id);
    }
    const lines: Record<'log' | 'warn' | 'debug', string[]> = { log: [], warn: [], debug: [] };
    const report = await new OrphanedJobsCleanup(redis, {
      queuePrefixes: [prefix()],
      reportKeysLimit: 2,
      logger: {
        log: (message) => lines.log.push(message),
        warn: (message) => lines.warn.push(message),
        debug: (message) => lines.debug.push(message),
      },
    }).run();

    expect(report.deleted).toBe(3);
    expect(report.orphanKeys).toHaveLength(2);
    for (const key of report.orphanKeys) {
      expect([`${prefix()}:lost1`, `${prefix()}:lost2`, `${prefix()}:lost3`]).toContain(key);
    }
    expect(lines.debug).toHaveLength(3);
    expect(lines.log).toHaveLength(1);
    expect(lines.log[0]).toContain('deleted=3');
    expect(lines.warn).toEqual([]);
  });

  /**
   * Проверка членства и удаление — один скрипт. Задача появляется в индексе
   * ПОСЛЕ снимка и после того, как уборка решила, что это кандидат, — в момент
   * сборки пайплайна удаления. По индексу каждого типа: список, sorted set,
   * множество (`stalled` — только поштучная проверка, в снимке его нет).
   */
  it.each([
    ['wait', (key: string, id: string) => redis.rpush(key, id)],
    ['paused', (key: string, id: string) => redis.rpush(key, id)],
    ['delayed', (key: string, id: string) => redis.zadd(key, 1, id)],
    ['waiting-children', (key: string, id: string) => redis.zadd(key, 1, id)],
    ['stalled', (key: string, id: string) => redis.sadd(key, id)],
  ])('задача, попавшая в %s между решением и удалением, не удаляется', async (status, inject) => {
    await orphan('mover');
    await orphan('lost');
    const racing = new Proxy(redis, {
      get(target, property, receiver) {
        if (property === 'pipeline') {
          return () => {
            void inject(`${prefix()}:${status}`, 'mover');
            return target.pipeline();
          };
        }
        const value = Reflect.get(target, property, receiver);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    const report = await new OrphanedJobsCleanup(racing, { queuePrefixes: [prefix()] }).run();
    // Выборка не выродилась: настоящая сирота рядом удалена.
    expect(report.deleted).toBe(1);
    expect(report.kept.alive).toBe(1);
    await expect(redis.type(`${prefix()}:mover`)).resolves.toBe('hash');
    await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(0);
  });

  it('настоящая задача BullMQ, добавленная после снимка, не удаляется и без грейса', async () => {
    // Грейс выключен: иначе свежую задачу держал бы он, и проверка индексов не проверялась бы.
    await orphan('lost');
    let added = false;
    const racing = new Proxy(redis, {
      get(target, property, receiver) {
        if (property === 'scan' && !added) {
          return async (...args: Parameters<Redis['scan']>) => {
            added = true;
            await queue.add('fresh', {}, { jobId: 'fresh' });
            return await target.scan(...args);
          };
        }
        const value = Reflect.get(target, property, receiver);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    const report = await new OrphanedJobsCleanup(racing, { queuePrefixes: [prefix()], graceMs: 0 }).run();
    expect(added).toBe(true);
    expect(report.deleted).toBe(1);
    expect(report.kept.alive).toBe(1);
    await expect(redis.type(`${prefix()}:fresh`)).resolves.toBe('hash');
  });

  it('индекс неожиданного типа — «не смогли проверить», хэш не удаляется', async () => {
    await orphan('lost');
    await redis.set(`${prefix()}:stalled`, 'not-a-set');
    const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()] }).run();
    expect(report.deleted).toBe(0);
    expect(report.kept.unverifiable).toBe(1);
    await expect(redis.type(`${prefix()}:lost`)).resolves.toBe('hash');
  });

  it('грейс по timestamp: моложе и из будущего — остаются, старше — удаляется, не число — остаётся', async () => {
    const now = Date.now();
    await orphan('young', { timestamp: String(now - 50_000) });
    await orphan('future', { timestamp: String(now + 60_000) });
    await orphan('old', { timestamp: String(now - 70_000) });
    await orphan('garbled', { timestamp: 'abc' });

    const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], graceMs: 60_000 }).run();

    expect(report.deleted).toBe(1);
    expect(report.kept).toMatchObject({ young: 2, unknownAge: 1 });
    await expect(redis.exists(`${prefix()}:old`)).resolves.toBe(0);
    await expect(
      redis.exists(`${prefix()}:young`, `${prefix()}:future`, `${prefix()}:garbled`),
    ).resolves.toBe(3);
  });

  /**
   * Положительная форма: хэш с `timestamp` удаляется, только если в нём есть и
   * поля, которые BullMQ пишет тем же HMSET. Фикстура содержит обе формы:
   * без любого из полей — остаётся, с полями — удаляется.
   */
  it.each(['name', 'opts'] as const)(
    'хэш с timestamp, но без поля %s — не задача, не удаляется',
    async (field) => {
      const { [field]: _dropped, ...rest } = JOB_SHAPE;
      await redis.hset(`${prefix()}:foreign`, { ...rest, timestamp: String(Date.now() - 600_000) });
      await orphan('lost');
      const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], graceMs: 0 }).run();
      expect(report.kept.notJob).toBe(1);
      expect(report.deleted).toBe(1);
      await expect(redis.type(`${prefix()}:foreign`)).resolves.toBe('hash');
      await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(0);
    },
  );

  /**
   * Хэш без `timestamp` — обычно поздняя запись в удалённую задачу, но так же
   * выглядит служебный хэш будущей версии BullMQ с незнакомым именем. Удалять
   * сразу — «не смогли определить = удалили»; удаляется по второму наблюдению.
   */
  describe('хэш без timestamp — только опцией deleteUntimestamped и по повторному наблюдению', () => {
    const clocked = (
      clock: { now: number },
      extra: { deleteUntimestamped?: boolean } = { deleteUntimestamped: true },
    ) =>
      new OrphanedJobsCleanup(redis, {
        queuePrefixes: [prefix()],
        graceMs: 60_000,
        ...extra,
        now: () => clock.now,
      });

    it('по умолчанию не удаляется никогда: и после грейса, и на десятом наблюдении', async () => {
      await redis.hset(`${prefix()}:late-write`, 'progress', '50');
      const clock = { now: Date.now() };
      // Настройка не задана — поведение по умолчанию.
      const cleanup = clocked(clock, {});
      for (let run = 0; run < 10; run += 1) {
        const report = await cleanup.run();
        expect(report).toMatchObject({ deleted: 0, kept: { untimestamped: 1 } });
        clock.now += 120_000;
      }
      await expect(redis.exists(`${prefix()}:late-write`)).resolves.toBe(1);
    });

    it('первый прогон оставляет, второй не раньше грейса — удаляет', async () => {
      await redis.hset(`${prefix()}:late-write`, 'progress', '50');
      const clock = { now: Date.now() };
      const cleanup = clocked(clock);

      const first = await cleanup.run();
      expect(first).toMatchObject({ deleted: 0, kept: { untimestamped: 1 } });
      await expect(redis.exists(`${prefix()}:late-write`)).resolves.toBe(1);

      clock.now += 30_000;
      const early = await cleanup.run();
      expect(early).toMatchObject({ deleted: 0, kept: { untimestamped: 1 } });

      clock.now += 30_000;
      const confirmed = await cleanup.run();
      expect(confirmed.deleted).toBe(1);
      await expect(redis.exists(`${prefix()}:late-write`)).resolves.toBe(0);
    });

    it('другой экземпляр уборки (рестарт процесса) начинает наблюдение заново', async () => {
      await redis.hset(`${prefix()}:late-write`, 'progress', '50');
      const clock = { now: Date.now() };
      await clocked(clock).run();
      clock.now += 120_000;

      const restarted = await clocked(clock).run();
      expect(restarted).toMatchObject({ deleted: 0, kept: { untimestamped: 1 } });
    });

    it('между наблюдениями хэш попал в индекс — «жива», наблюдение забыто', async () => {
      await redis.hset(`${prefix()}:late-write`, 'progress', '50');
      const clock = { now: Date.now() };
      const cleanup = clocked(clock);
      await cleanup.run();
      await redis.zadd(`${prefix()}:completed`, 1, 'late-write');
      clock.now += 120_000;

      const second = await cleanup.run();
      expect(second.deleted).toBe(0);
      await redis.zrem(`${prefix()}:completed`, 'late-write');
      clock.now += 120_000;
      // Снова сирота: наблюдение начинается заново, а не продолжает прежнее.
      const third = await cleanup.run();
      expect(third).toMatchObject({ deleted: 0, kept: { untimestamped: 1 } });
    });
  });

  it('грейс по умолчанию — минута: хэш тридцатисекундной давности не удаляется', async () => {
    await orphan('recent', { timestamp: String(Date.now() - 30_000) });
    const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()] }).run();
    expect(report.kept.young).toBe(1);
    await expect(redis.exists(`${prefix()}:recent`)).resolves.toBe(1);
  });

  it('хэш, чей id ждёт отложенная задача дедупликации (dn:<id>.jid), не удаляется', async () => {
    await orphan('awaited', { deid: 'd1' });
    await redis.hset(`${prefix()}:dn:d1`, { name: 'n', data: '{}', opts: '{}', jid: 'awaited' });
    await orphan('lost', { deid: 'd2' });
    await redis.hset(`${prefix()}:dn:d2`, { name: 'n', data: '{}', opts: '{}', jid: 'someone-else' });

    const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()] }).run();

    expect(report.deleted).toBe(1);
    expect(report.kept.alive).toBe(1);
    await expect(redis.type(`${prefix()}:awaited`)).resolves.toBe('hash');
    await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(0);
  });

  it('ключ дедупликации, указывающий на удалённую сироту, снимается — id дедупликации снова свободен', async () => {
    await queue.add('live', {}, { jobId: 'z1' });
    await orphan('x1', { deid: 'd1' });
    await redis.set(`${prefix()}:de:d1`, 'x1');
    await redis.hset(`${prefix()}:dn:d1`, { name: 'n', data: '{}', opts: '{}', jid: 'next' });
    await orphan('y1', { deid: 'd2' });
    await redis.set(`${prefix()}:de:d2`, 'z1');
    // Контроль фикстуры: пока ключ указывает на сироту, BullMQ дедуплицирует в неё.
    const blocked = await queue.add('n', {}, { deduplication: { id: 'd1' } });
    expect(blocked.id).toBe('x1');

    const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()] }).run();

    expect(report.deleted).toBe(2);
    await expect(redis.exists(`${prefix()}:de:d1`, `${prefix()}:dn:d1`)).resolves.toBe(0);
    // Чужой ключ дедупликации (указывает на живую задачу) не тронут.
    await expect(redis.get(`${prefix()}:de:d2`)).resolves.toBe('z1');
    const fresh = await queue.add('n', {}, { deduplication: { id: 'd1' } });
    expect(fresh.id).not.toBe('x1');
    await expect(redis.type(`${prefix()}:${fresh.id}`)).resolves.toBe('hash');
  });

  it('огромный sorted set не читается в снимок: его задачи проверены поштучно, сирота удалена', async () => {
    await queue.addBulk(
      Array.from({ length: 10 }, (_, i) => ({
        name: 'n',
        data: {},
        opts: { jobId: `d${i}`, delay: 600_000 },
      })),
    );
    await orphan('lost');
    const ranged: string[] = [];
    const counted = new Proxy(redis, {
      get(target, property, receiver) {
        const value = Reflect.get(target, property, receiver) as unknown;
        if (property === 'zrange') {
          return (key: string, start: number, stop: string) => {
            ranged.push(key);
            return target.zrange(key, start, stop);
          };
        }
        return typeof value === 'function' ? (value as (...a: unknown[]) => unknown).bind(target) : value;
      },
    });

    const report = await new OrphanedJobsCleanup(counted, {
      queuePrefixes: [prefix()],
      maxSnapshotZsetSize: 5,
    }).run();

    expect(ranged).not.toContain(`${prefix()}:delayed`);
    // Контроль: остальные (маленькие) индексы снимок читал.
    expect(ranged).toContain(`${prefix()}:completed`);
    expect(report.unsnapshottedIndexes).toEqual([`${prefix()}:delayed`]);
    expect(report.deleted).toBe(1);
    expect(report.kept.alive).toBe(10);
    for (let i = 0; i < 10; i++) {
      await expect(redis.type(`${prefix()}:d${i}`)).resolves.toBe('hash');
    }
  });

  it('кандидаты проверяются пайплайном скриптов: ни одного TYPE поштучно, пайплайн на батч', async () => {
    for (let i = 0; i < 250; i++) {
      await orphan(`lost${i}`);
    }
    let types = 0;
    let pipelines = 0;
    const counted = new Proxy(redis, {
      get(target, property, receiver) {
        const value = Reflect.get(target, property, receiver) as unknown;
        if (typeof value !== 'function') {
          return value;
        }
        return (...args: unknown[]) => {
          if (property === 'type') {
            types += 1;
          }
          if (property === 'pipeline') {
            pipelines += 1;
          }
          return (value as (...a: unknown[]) => unknown).apply(target, args);
        };
      },
    });
    const report = await new OrphanedJobsCleanup(counted, {
      queuePrefixes: [prefix()],
      batchSize: 100,
    }).run();
    expect(report.deleted).toBe(250);
    expect(types).toBe(0);
    expect(pipelines).toBe(3);
  });

  it('потолок ключей SCAN: обход останавливается, truncatedBy = keys', async () => {
    for (let i = 0; i < 10; i++) {
      await orphan(`lost${i}`);
      await redis.rpush(`${prefix()}:lost${i}:logs`, 'line');
    }
    const report = await new OrphanedJobsCleanup(redis, {
      queuePrefixes: [prefix()],
      maxScannedKeysPerRun: 6,
      scanCount: 2,
    }).run();
    expect(report.truncatedBy).toBe('keys');
    expect(report.scannedKeys).toBe(6);
    expect(report.scanned).toBeLessThanOrEqual(6);
    // Сколько из шести ключей — хэши, решает порядок SCAN; каждый просмотренный хэш — сирота.
    expect(report.deleted).toBe(report.scanned);
    expect(report.deleted).toBeLessThan(10);
  });

  it('потолок длительности: обход останавливается, truncatedBy = time', async () => {
    for (let i = 0; i < 20; i++) {
      await orphan(`lost${i}`);
    }
    let clock = 0;
    const report = await new OrphanedJobsCleanup(redis, {
      queuePrefixes: [prefix()],
      maxRunMs: 2_500,
      scanCount: 1,
      now: () => {
        clock += 1_000;
        return clock;
      },
    }).run();
    expect(report.truncatedBy).toBe('time');
    expect(report.scanned).toBeLessThan(20);
    expect(await redis.keys(`${prefix()}:lost*`)).not.toHaveLength(0);
  });

  it('лок прогона: при занятом локе уборки нет, свой лок снимается после прогона', async () => {
    await orphan('lost');
    const lockKey = `${prefix()}-cleanup-lock`;
    await redis.set(lockKey, 'other-replica', 'PX', 60_000);

    const locked = await new OrphanedJobsCleanup(redis, {
      queuePrefixes: [prefix()],
      runLockKey: lockKey,
    }).run();
    expect(locked).toMatchObject({ lockedOut: true, scanned: 0, deleted: 0 });
    await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(1);
    await expect(redis.get(lockKey)).resolves.toBe('other-replica');

    await redis.del(lockKey);
    const free = await new OrphanedJobsCleanup(redis, {
      queuePrefixes: [prefix()],
      runLockKey: lockKey,
    }).run();
    expect(free).toMatchObject({ lockedOut: false, deleted: 1 });
    await expect(redis.exists(lockKey)).resolves.toBe(0);
  });

  it('scanClients: обход идёт по ним, а не по основному клиенту (кластер — по мастерам)', async () => {
    await orphan('lost');
    const noScan = new Proxy(redis, {
      get(target, property, receiver) {
        if (property === 'scan') {
          return () => Promise.reject(new Error('SCAN через клиент кластера обошёл бы случайный узел'));
        }
        const value = Reflect.get(target, property, receiver);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    const report = await new OrphanedJobsCleanup(noScan, {
      queuePrefixes: [prefix()],
      scanClients: [redis],
    }).run();
    expect(report.deleted).toBe(1);
    expect(() => new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], scanClients: [] })).toThrow(
      'scanClients',
    );
  });

  it('нулевой шаг батча или страницы — отказ в конструкторе, а не вечный цикл', () => {
    expect(() => new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], batchSize: 0 })).toThrow(
      'batchSize',
    );
    expect(() => new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], snapshotPage: 0 })).toThrow(
      'snapshotPage',
    );
    expect(() => new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], graceMs: -1 })).toThrow(
      'graceMs',
    );
    expect(
      () =>
        new OrphanedJobsCleanup(redis, {
          queuePrefixes: [prefix()],
          runLockKey: 'k',
          maxRunMs: Number.POSITIVE_INFINITY,
        }),
    ).toThrow('maxRunMs');
  });

  /**
   * LPOS отсутствующего id проходит список целиком: на миллионе элементов —
   * 15 мс, когда Redis не отвечает никому. Потолок `maxListScan` ограничивает
   * проход; id, не найденный в начале списка длиннее потолка, — «не смогли
   * проверить», а не «нет в списке». Фикстура содержит оба случая: тот же
   * список при потолке выше его длины — сирота удаляется.
   */
  it('maxListScan: сирота при списке длиннее потолка остаётся (unverifiable), при списке в потолке — удаляется', async () => {
    await redis.lpush(`${prefix()}:wait`, ...Array.from({ length: 20 }, (_, i) => `foreign-${i}`));
    await orphan('lost');

    const bounded = await new OrphanedJobsCleanup(redis, {
      queuePrefixes: [prefix()],
      maxListScan: 10,
    }).run();

    expect(bounded.deleted).toBe(0);
    expect(bounded.kept.unverifiable).toBe(1);
    await expect(redis.type(`${prefix()}:lost`)).resolves.toBe('hash');

    const roomy = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], maxListScan: 20 }).run();

    expect(roomy.deleted).toBe(1);
    await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(0);
  });

  it('maxListScan: задача, найденная в начале длинного списка, — «жива», а не «не смогли проверить»', async () => {
    await redis.rpush(`${prefix()}:wait`, ...Array.from({ length: 20 }, (_, i) => `foreign-${i}`));
    await orphan('mover');
    // Задача встаёт в голову списка после снимка — как новая задача BullMQ (LPUSH).
    let injected = false;
    const racing = new Proxy(redis, {
      get(target, property, receiver) {
        if (property === 'pipeline' && !injected) {
          return () => {
            injected = true;
            void target.lpush(`${prefix()}:wait`, 'mover');
            return target.pipeline();
          };
        }
        const value = Reflect.get(target, property, receiver);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });

    const report = await new OrphanedJobsCleanup(racing, { queuePrefixes: [prefix()], maxListScan: 5 }).run();

    expect(injected).toBe(true);
    expect(report.kept).toMatchObject({ alive: 1, unverifiable: 0 });
    await expect(redis.type(`${prefix()}:mover`)).resolves.toBe('hash');
  });

  /**
   * Порядок проверок в скрипте: sorted set'ы и множества раньше списков.
   * Задача огромного `completed` вне снимка — кандидат; при обратном порядке
   * каждая платила бы LPOS по всему `wait`. Мерится счётчиком вызовов LPOS
   * сервера (`INFO commandstats` считает и команды из скриптов). Сирота рядом —
   * контроль, что счётчик видит LPOS из скрипта: она списки проходит, задача
   * из `completed` — нет.
   */
  it('задача из sorted set вне снимка находится ZSCORE, не проходя списки', async () => {
    await redis.lpush(`${prefix()}:wait`, ...Array.from({ length: 20 }, (_, i) => `foreign-${i}`));
    await orphan('done1');
    await redis.zadd(`${prefix()}:completed`, Date.now(), 'done1');
    await orphan('lost');
    const lposCalls = async (): Promise<number> => {
      const stats = await redis.info('commandstats');
      return Number(/cmdstat_lpos:calls=(\d+)/.exec(stats)?.[1] ?? 0);
    };
    const before = await lposCalls();

    const report = await new OrphanedJobsCleanup(redis, {
      queuePrefixes: [prefix()],
      maxSnapshotZsetSize: 0,
    }).run();

    // Контроль фикстуры: completed действительно не попал в снимок.
    expect(report.unsnapshottedIndexes).toContain(`${prefix()}:completed`);
    expect(report.kept.alive).toBe(1);
    expect(report.deleted).toBe(1);
    // Один LPOS — сироты по `wait` (active и paused не существуют); у done1 — ни одного.
    expect((await lposCalls()) - before).toBe(1);
    await expect(redis.type(`${prefix()}:done1`)).resolves.toBe('hash');
  });

  /**
   * 0.9.0: потолок LPOS по умолчанию конечный. Отсутствие настройки — не
   * «без потолка»: на списке длиннее снимка по умолчанию (`maxSnapshotListSize`
   * снят) один скрипт без потолка держал Redis десятки мс (замер в README).
   * Утверждение — о том, что уходит в скрипт: аргумент MAXLEN, а не «0» (у Redis
   * это «весь список»). Явный Infinity — единственный путь к «без потолка».
   */
  it('0.9.0: maxListScan по умолчанию — CLEANUP_DEFAULTS.MAX_LIST_SCAN (миллион), Infinity — только явно', async () => {
    expect(CLEANUP_DEFAULTS.MAX_LIST_SCAN).toBe(1_000_000);
    const sent = async (options: { maxListScan?: number }): Promise<string[]> => {
      await orphan(`lost-${randomUUID()}`);
      const listScanArgs: string[] = [];
      const capture = (args: readonly unknown[]) => {
        // [команда, sha или скрипт, numKeys, ...KEYS, ...ARGV]; ARGV[6] checkAndDelete — потолок LPOS.
        const numKeys = Number(args[2]);
        listScanArgs.push(String(args[3 + numKeys + 5]));
      };
      const spying = new Proxy(redis, {
        get(target, property, receiver) {
          if (property === 'pipeline') {
            return () => {
              const pipeline = target.pipeline();
              return new Proxy(pipeline, {
                get(inner, name, innerReceiver) {
                  const value = Reflect.get(inner, name, innerReceiver);
                  if ((name === 'evalsha' || name === 'eval') && typeof value === 'function') {
                    return (...args: unknown[]) => {
                      capture([name, ...args]);
                      return value.apply(inner, args);
                    };
                  }
                  return typeof value === 'function' ? value.bind(inner) : value;
                },
              });
            };
          }
          const value = Reflect.get(target, property, receiver);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      });
      await new OrphanedJobsCleanup(spying, { queuePrefixes: [prefix()], ...options }).run();
      return [...new Set(listScanArgs)];
    };
    expect(await sent({})).toEqual(['1000000']);
    expect(await sent({ maxListScan: Number.POSITIVE_INFINITY })).toEqual([LIST_SCAN_ARG.unbounded]);
    expect(await sent({ maxListScan: 7 })).toEqual(['7']);
  });

  it('maxListScan 0 — отказ в конструкторе: MAXLEN 0 в Redis значит «весь список»', () => {
    expect(() => new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], maxListScan: 0 })).toThrow(
      'maxListScan',
    );
    expect(() => new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], maxListScan: 1.5 })).toThrow(
      'maxListScan',
    );
    expect(
      () =>
        new OrphanedJobsCleanup(redis, {
          queuePrefixes: [prefix()],
          maxListScan: Number.POSITIVE_INFINITY,
        }),
    ).not.toThrow();
  });

  /**
   * Дерево flow на настоящем BullMQ: внук выполнен, ребёнок выполнен (у
   * родителя `:processed`), второй ребёнок отложен (у родителя
   * `:dependencies`), родитель ждёт в waiting-children. Ключей `:children` и
   * `:parent` BullMQ 5.81 не создаёт (связь — поля `parentKey`/`parent` в хэше
   * ребёнка и хвосты `:dependencies`/`:processed`/`:failed`/`:unsuccessful`
   * родителя), а каждый хэш дерева лежит в индексе статуса. Утверждение — про
   * ВСЕ ключи очереди: кандидатами в хэши задач фильтр делает ровно четыре
   * хэша дерева, ни одного хвоста.
   */
  it('дерево flow: ключи очереди — ровно хэши задач и известные хвосты, ничего не удаляется и без грейса', async () => {
    const { hostname, port } = new URL(url as string);
    const connection = { host: hostname, port: Number(port), maxRetriesPerRequest: null };
    await flows.add({
      name: 'parent',
      queueName: name,
      opts: { jobId: 'parent' },
      children: [
        {
          name: 'child',
          queueName: name,
          opts: { jobId: 'c1' },
          children: [{ name: 'grandchild', queueName: name, opts: { jobId: 'g1' } }],
        },
        { name: 'child', queueName: name, opts: { jobId: 'c2', delay: 600_000 } },
      ],
    });
    const worker = new Worker(name, async () => 'done', { connection });
    try {
      for (let i = 0; i < 250 && (await queue.getJobState('c1')) !== 'completed'; i++) {
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
    } finally {
      await worker.close();
    }
    // Контроль фикстуры: дерево в нужных состояниях, хвосты родителя есть.
    expect(await queue.getJobState('g1')).toBe('completed');
    expect(await queue.getJobState('c1')).toBe('completed');
    expect(await queue.getJobState('c2')).toBe('delayed');
    expect(await queue.getJobState('parent')).toBe('waiting-children');
    await expect(redis.hlen(`${prefix()}:parent:processed`)).resolves.toBe(1);
    await expect(redis.scard(`${prefix()}:parent:dependencies`)).resolves.toBe(1);
    await expect(redis.hlen(`${prefix()}:c1:processed`)).resolves.toBe(1);

    const keys = await redis.keys(`${prefix()}:*`);
    const suffixes = keys.map((key) => key.slice(prefix().length + 1));
    expect(suffixes.filter((suffix) => /:(children|parent)$/.test(suffix))).toEqual([]);
    expect(suffixes.filter((suffix) => OrphanedJobsCleanup.isJobKeySuffix(suffix)).sort()).toEqual([
      'c1',
      'c2',
      'g1',
      'parent',
    ]);

    const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], graceMs: 0 }).run();

    expect(report.deleted).toBe(0);
    expect(report.scanned).toBe(4);
    await expect(redis.exists(...keys)).resolves.toBe(keys.length);
  });

  describe('0.4.0: курсор между прогонами, Cluster, glob, потолок списка, время снимка', () => {
    const live = async (count: number) => {
      for (let i = 0; i < count; i += 1) {
        await redis.hset(`${prefix()}:live-${i}`, { ...JOB_SHAPE, timestamp: String(Date.now() - 600_000) });
        await redis.zadd(`${prefix()}:completed`, i, `live-${i}`);
      }
    };

    it('потолок ключей меньше пространства: прогоны продолжают курсор и за K прогонов убирают всех сирот', async () => {
      await live(200);
      for (const id of ['lost-a', 'lost-b', 'lost-c']) {
        await orphan(id);
      }
      const cleanup = new OrphanedJobsCleanup(redis, {
        queuePrefixes: [prefix()],
        maxScannedKeysPerRun: 20,
        scanCount: 10,
      });
      let deleted = 0;
      for (let run = 0; run < 15 && deleted < 3; run += 1) {
        deleted += (await cleanup.run()).deleted;
      }
      expect(deleted).toBe(3);
      await expect(
        redis.exists(`${prefix()}:lost-a`, `${prefix()}:lost-b`, `${prefix()}:lost-c`),
      ).resolves.toBe(0);
      // Контроль: живые не тронуты.
      expect(await redis.zcard(`${prefix()}:completed`)).toBe(200);
      await expect(redis.exists(`${prefix()}:live-0`, `${prefix()}:live-199`)).resolves.toBe(2);
    });

    it('клиент Redis Cluster без scanClients — отказ в конструкторе', () => {
      const clusterLike = Object.assign(Object.create(redis) as Redis, { isCluster: true });
      expect(() => new OrphanedJobsCleanup(clusterLike, { queuePrefixes: [prefix()] })).toThrow(
        'scanClients',
      );
      expect(
        () => new OrphanedJobsCleanup(clusterLike, { queuePrefixes: [prefix()], scanClients: [redis] }),
      ).not.toThrow();
    });

    it('метасимволы glob в префиксе — буквально: сирота очереди «q[1]» найдена', async () => {
      const odd = `${prefix()}[1]`;
      await sameVersionAs(odd);
      await redis.hset(`${odd}:lost`, { ...JOB_SHAPE, timestamp: String(Date.now() - 600_000) });
      const report = await new OrphanedJobsCleanup(redis, { queuePrefixes: [odd] }).run();
      expect(report.deleted).toBe(1);
      await expect(redis.exists(`${odd}:lost`)).resolves.toBe(0);
      await redis.del(`${odd}:meta`);
    });

    it('maxSnapshotListSize: список длиннее — очередь не убирается и названа; в потолке — убирается', async () => {
      for (let i = 0; i < 5; i += 1) {
        await redis.rpush(`${prefix()}:wait`, `w-${i}`);
      }
      await orphan('lost');
      const capped = await new OrphanedJobsCleanup(redis, {
        queuePrefixes: [prefix()],
        maxSnapshotListSize: 3,
      }).run();
      expect(capped.deleted).toBe(0);
      expect(capped.unsnapshottedIndexes).toContain(`${prefix()}:wait`);
      await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(1);
      const roomy = await new OrphanedJobsCleanup(redis, {
        queuePrefixes: [prefix()],
        maxSnapshotListSize: 10,
      }).run();
      expect(roomy.deleted).toBe(1);
    });

    it('время вышло во время снимка — очередь не убирается, truncatedBy = time', async () => {
      for (let i = 0; i < 6; i += 1) {
        await redis.rpush(`${prefix()}:wait`, `w-${i}`);
      }
      await orphan('lost');
      let clock = 0;
      const report = await new OrphanedJobsCleanup(redis, {
        queuePrefixes: [prefix()],
        snapshotPage: 1,
        maxRunMs: 3_000,
        now: () => {
          clock += 1_000;
          return clock;
        },
      }).run();
      expect(report.truncatedBy).toBe('time');
      expect(report.deleted).toBe(0);
      await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(1);
    });
  });

  /**
   * BullMQ запрещает `:` в имени очереди, но не в префиксе: очередь `x` с
   * префиксом `bull:<имя>` живёт ключами `bull:<имя>:x:*` — внутри пространства
   * убираемой очереди. Её задачи не лежат в индексах убираемой, и уборка видела
   * в них сирот: живая задача удалялась за один прогон, `x:meta` — по второму
   * наблюдению. Грейс выключен: задачи свежие, и держал бы их он, а не проверка.
   */
  describe('вложенный префикс очереди', () => {
    const connection = () => {
      const { hostname, port } = new URL(url as string);
      return { host: hostname, port: Number(port), maxRetriesPerRequest: null };
    };

    it('очередь с префиксом внутри убираемой: её ключи не тронуты, убираемая очередь отказана с причиной', async () => {
      const nested = new Queue('x', { connection: connection(), prefix: prefix() });
      try {
        await nested.waitUntilReady();
        await nested.add('n', {}, { jobId: 'live-1' });
        await nested.add('n', {}, { jobId: 'live-2', delay: 600_000 });
        await orphan('lost');
        const before = (await redis.keys(`${prefix()}:x:*`)).sort();
        // Контроль фикстуры: вложенная очередь действительно лежит внутри.
        expect(before).toEqual(expect.arrayContaining([`${prefix()}:x:live-1`, `${prefix()}:x:meta`]));

        const cleanup = new OrphanedJobsCleanup(redis, { queuePrefixes: [prefix()], graceMs: 0 });
        const first = await cleanup.run();
        const second = await cleanup.run();

        expect((await redis.keys(`${prefix()}:x:*`)).sort()).toEqual(before);
        expect(first.deleted + second.deleted).toBe(0);
        expect(first.unsupportedQueues).toEqual([
          {
            prefix: prefix(),
            version: BULLMQ_META_VERSION,
            reason: expect.stringContaining(`${prefix()}:x`),
          },
        ]);
        // Отказ — на всю очередь: и её собственная сирота ждёт, пока вложенную не уберут из пространства.
        await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(1);
      } finally {
        await nested.obliterate({ force: true });
        await nested.close();
      }
    });

    it('задача вложенной очереди, чей meta обход не видел (обход усечён), не удаляется проверкой в скрипте', async () => {
      const shallow = new Queue('x', { connection: connection(), prefix: prefix() });
      // Глубже на сегмент: суффикс `a:b:live-3` по форме совпадает с jobId вида `<uuid>::<p>`.
      const deep = new Queue('b', { connection: connection(), prefix: `${prefix()}:a` });
      try {
        await shallow.add('n', {}, { jobId: 'live-1' });
        await deep.add('n', {}, { jobId: 'live-3' });
        // У мелкой очереди нет meta — остаётся счётчик `id`, его пишет каждое добавление.
        await redis.del(`${prefix()}:x:meta`);
        await orphan('lost');
        const keys = [`${prefix()}:x:live-1`, `${prefix()}:a:b:live-3`, `${prefix()}:lost`];
        // Обход отдаёт только хэши — до meta вложенных очередей он «не дошёл».
        const partial = { scan: async () => ['0', keys] as [string, string[]] };

        const report = await new OrphanedJobsCleanup(redis, {
          queuePrefixes: [prefix()],
          graceMs: 0,
          scanClients: [partial],
        }).run();

        expect(report.kept).toMatchObject({ nested: 2 });
        // Выборка не выродилась: настоящая сирота убираемой очереди удалена.
        expect(report.deleted).toBe(1);
        await expect(redis.exists(`${prefix()}:x:live-1`, `${prefix()}:a:b:live-3`)).resolves.toBe(2);
        await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(0);
      } finally {
        for (const queue of [shallow, deep]) {
          await queue.obliterate({ force: true });
          await queue.close();
        }
      }
    });
  });

  it('пустой список очередей — отказ, а не уборка всего Redis', () => {
    expect(() => new OrphanedJobsCleanup(redis, { queuePrefixes: [] })).toThrow('пуст');
    expect(() => new OrphanedJobsCleanup(redis, { queuePrefixes: [' '] })).toThrow('пуст');
  });
});

/**
 * Без Redis: уступка циклу событий. Подделка отвечает уже разрешёнными
 * промисами — так ведут себя ответы, лежащие в буфере сокета, — и без
 * `setImmediate` обход не отдал бы цикл ни разу. Мерится числом макрозадач,
 * успевших выполниться (рекурсивный setImmediate-счётчик), а не часами: число
 * тиков не зависит от скорости машины.
 */
describe('OrphanedJobsCleanup: уступка циклу событий', () => {
  it('обход SCAN отдаёт цикл событий между батчами', async () => {
    const pages = 50;
    const fake = {
      scan: async (cursor: string) => {
        const next = Number(cursor) + 1;
        return [next >= pages ? '0' : String(next), [`bull:q:${cursor}:lock`]] as [string, string[]];
      },
      lrange: async () => [],
      zrange: async () => [],
      zcard: async () => 0,
      llen: async () => 0,
      eval: async (script: string) => (script === CLEANUP_SCRIPTS.readQueueVersion ? 'bullmq:5.81.3' : 0),
      pipeline: () => {
        throw new Error('кандидатов нет — пайплайн не нужен');
      },
    };
    let ticks = 0;
    let running = true;
    const tick = () => {
      if (running) {
        ticks += 1;
        setImmediate(tick);
      }
    };
    setImmediate(tick);
    const report = await new OrphanedJobsCleanup(fake, { queuePrefixes: ['bull:q'] }).run();
    running = false;
    // Выборка не выродилась: обход действительно прошёл все страницы.
    expect(report.truncatedBy).toBeNull();
    expect(ticks).toBeGreaterThanOrEqual(pages - 1);
  });
});

describe('OrphanedJobsCleanup.isJobKeySuffix', () => {
  it.each([
    ['123', true],
    ['uuid::project', true],
    ['wait', false],
    ['waiting-children', false],
    ['meta', false],
    ['1:lock', false],
    ['uuid::p:logs', false],
    ['repeat:abc', false],
    ['metrics:completed', false],
    ['dn:dedup-1', false],
    ['1:unsuccessful', false],
    // Пространство с пустым сегментом — relay BullMQ 6 с префиксом очереди.
    [':relay:{ns}:layout', false],
    [':relay:{ns}:node-1:node', false],
    [':a:b', false],
    ['', false],
  ])('%s → %s', (suffix, expected) => {
    expect(OrphanedJobsCleanup.isJobKeySuffix(suffix)).toBe(expected);
  });
});
