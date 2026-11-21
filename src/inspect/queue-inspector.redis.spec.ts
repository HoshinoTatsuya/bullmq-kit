import { randomUUID } from 'node:crypto';

import { Queue } from 'bullmq';
import Redis from 'ioredis';
import { awaitQueueMeta } from '../testing/await-queue-meta';
import { QueueInspector } from './queue-inspector';
import { toPrometheus } from './to-prometheus';

/**
 * Инспектор на НАСТОЯЩЕМ BullMQ. Состояния, которые трудно получить честно
 * (истёкший лок активной, проспавшая отложенная), кладутся в раскладке BullMQ
 * вручную — рядом с настоящими задачами очереди.
 */
const url = process.env.REDIS_TEST_URL;
const describeRedis = url ? describe : describe.skip;

describeRedis('QueueInspector [нужен REDIS_TEST_URL]', () => {
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

  it('счёт индексов, возраст старой ждущей, просрочка отложенной, активные без лока, прирост failed, сироты', async () => {
    const hour = 3_600_000;
    await queue.add('old', {}, { jobId: 'old', timestamp: Date.now() - hour });
    await queue.add('new', {}, { jobId: 'new' });
    await queue.add('later', {}, { jobId: 'later', delay: 600_000 });
    // Отложенная, которая должна была проснуться минуту назад (очко BullMQ 5: wakeAt * 0x1000).
    await redis.hset(`${prefix()}:overdue`, {
      name: 'n',
      data: '{}',
      opts: '{}',
      timestamp: String(Date.now()),
    });
    await redis.zadd(`${prefix()}:delayed`, (Date.now() - 60_000) * 0x1000, 'overdue');
    // Активная без лока (лок истёк) и активная с живым локом.
    for (const id of ['stuck', 'held']) {
      await redis.hset(`${prefix()}:${id}`, {
        name: 'n',
        data: '{}',
        opts: '{}',
        timestamp: String(Date.now()),
      });
      await redis.rpush(`${prefix()}:active`, id);
    }
    await redis.set(`${prefix()}:held:lock`, 'token', 'PX', 60_000);
    await redis.hset(`${prefix()}:lost`, {
      name: 'n',
      data: '{}',
      opts: '{}',
      timestamp: String(Date.now() - hour),
    });
    const inspector = new QueueInspector(redis, { queuePrefixes: [prefix()], orphans: true });

    const [first] = await inspector.inspect();

    expect(first).toMatchObject({
      prefix: prefix(),
      counts: { wait: 2, active: 2, delayed: 2, failed: 0, completed: 0 },
      stuckActive: { count: 1, scanned: 2, sample: ['stuck'] },
      failedIncrease: null,
      orphans: 1,
    });
    expect(first?.oldestWaitAgeMs).toBeGreaterThan(hour - 60_000);
    expect(first?.delayedOverdueMs).toBeGreaterThan(50_000);
    expect(first?.delayedOverdueMs).toBeLessThan(120_000);

    await redis.zadd(`${prefix()}:failed`, 1, 'f1', 2, 'f2');
    const [second] = await inspector.inspect();
    expect(second?.failedIncrease).toBe(2);

    const text = toPrometheus([second as NonNullable<typeof second>]);
    expect(text).toContain(`bullmq_queue_jobs{queue="${prefix()}",state="failed"} 2`);
    expect(text).toContain(`bullmq_queue_stuck_active{queue="${prefix()}"} 1`);
    expect(text).toContain(`bullmq_queue_orphans{queue="${prefix()}"} 1`);
    // Сирота не удалена: инспектор только читает.
    await expect(redis.exists(`${prefix()}:lost`)).resolves.toBe(1);
  });

  /**
   * Пауза очереди: BullMQ 5 переносила `wait` в список `paused`, BullMQ 6
   * оставляет задачи в `wait`. По одному счёту индексов приостановленная очередь
   * на 6 неотличима от очереди без воркеров (растёт `oldestWaitAgeMs`), поэтому
   * признак паузы — отдельным полем, из `meta` (его пишут обе версии).
   */
  it('пауза: признак из meta на любой BullMQ; ждущие — в wait (6) или paused (5)', async () => {
    const major = Number(
      (
        require(`${process.env.BULLMQ_UNDER_TEST ?? 'bullmq'}/package.json`) as { version: string }
      ).version.split('.')[0],
    );
    await queue.add('a', {}, { jobId: 'a' });
    const inspector = new QueueInspector(redis, { queuePrefixes: [prefix()] });
    const [running] = await inspector.inspect();
    await queue.pause();
    await queue.add('b', {}, { jobId: 'b' });
    const [paused] = await inspector.inspect();
    await queue.resume();
    const [resumed] = await inspector.inspect();

    expect([running?.paused, paused?.paused, resumed?.paused]).toEqual([false, true, false]);
    expect(paused?.counts).toMatchObject(major >= 6 ? { wait: 2, paused: 0 } : { wait: 0, paused: 2 });
    expect(resumed?.counts).toMatchObject({ wait: 2, paused: 0 });
    const text = toPrometheus([paused as NonNullable<typeof paused>, resumed as NonNullable<typeof resumed>]);
    expect(text).toContain(`bullmq_queue_paused{queue="${prefix()}"} 1`);
    expect(text).toContain(`bullmq_queue_paused{queue="${prefix()}"} 0`);
  });

  /**
   * BullMQ 5 при паузе переносит ждущих в список `paused` (RENAME wait paused)
   * и туда же кладёт новые: возраст, читаемый только из `wait`, на паузе
   * пропадал (`null`) ровно тогда, когда ждущие копятся.
   */
  it('возраст старой ждущей виден и на паузе (BullMQ 5 держит ждущих в paused)', async () => {
    const hour = 3_600_000;
    await queue.add('old', {}, { jobId: 'old', timestamp: Date.now() - hour });
    await queue.pause();
    await queue.add('new', {}, { jobId: 'new' });
    const [health] = await new QueueInspector(redis, { queuePrefixes: [prefix()] }).inspect();
    expect(health?.paused).toBe(true);
    expect((health?.counts.wait ?? 0) + (health?.counts.paused ?? 0)).toBe(2);
    expect(health?.oldestWaitAgeMs).toBeGreaterThan(hour - 60_000);
    expect(health?.oldestWaitAgeMs).toBeLessThan(hour + 60_000);
  });

  /**
   * Воркер берёт задачу `RPOPLPUSH wait active` — новая встаёт в ГОЛОВУ active,
   * самая старая — в хвосте. Просмотр с головы при active длиннее
   * `maxActiveScan` смотрел только свежие задачи, а зависшие старые — нет.
   */
  it('зависшие active ищутся с хвоста: самые старые попадают в просмотр первыми', async () => {
    for (const id of ['old-stuck', 'new-1', 'new-2']) {
      await redis.hset(`${prefix()}:${id}`, {
        name: 'n',
        data: '{}',
        opts: '{}',
        timestamp: String(Date.now()),
      });
      await redis.lpush(`${prefix()}:active`, id);
    }
    for (const id of ['new-1', 'new-2']) {
      await redis.set(`${prefix()}:${id}:lock`, 'token', 'PX', 60_000);
    }
    const [health] = await new QueueInspector(redis, {
      queuePrefixes: [prefix()],
      maxActiveScan: 2,
    }).inspect();
    expect(health?.stuckActive).toEqual({ count: 1, scanned: 2, sample: ['old-stuck'] });
  });

  /**
   * Индекс чужого типа (чужая запись в пространстве очереди, ручная правка)
   * ронял весь снимок WRONGTYPE — и метрики ВСЕХ очередей инспектора, а не
   * одной. Теперь такой ключ назван в отчёте, а снимок собран по остальным.
   */
  it('индекс не своего типа — в unexpectedTypes, снимок и соседние очереди не падают', async () => {
    const broken = `${prefix()}-broken`;
    try {
      await redis.set(`${broken}:wait`, 'x');
      await redis.hset(`${broken}:delayed`, 'f', 'v');
      await redis.set(`${broken}:active`, 'x');
      await queue.add('a', {}, { jobId: 'a' });
      const report = await new QueueInspector(redis, { queuePrefixes: [broken, prefix()] }).inspect();
      expect(report.map((item) => item.prefix)).toEqual([broken, prefix()]);
      expect(report[0]).toMatchObject({
        counts: { wait: 0, active: 0, delayed: 0 },
        oldestWaitAgeMs: null,
        delayedOverdueMs: null,
        stuckActive: { count: 0, scanned: 0, sample: [] },
      });
      expect(report[0]?.unexpectedTypes).toEqual([`${broken}:wait`, `${broken}:active`, `${broken}:delayed`]);
      expect(report[1]).toMatchObject({ counts: { wait: 1 }, unexpectedTypes: [] });
      expect(toPrometheus(report)).toContain(`bullmq_queue_unexpected_type_keys{queue="${broken}"} 3`);
    } finally {
      await redis.del(`${broken}:wait`, `${broken}:delayed`, `${broken}:active`);
    }
  });

  it('пустая очередь: неизвестное — null и не выводится в метрики, счёты — нули', async () => {
    const [health] = await new QueueInspector(redis, { queuePrefixes: [prefix()] }).inspect();
    expect(health).toMatchObject({ oldestWaitAgeMs: null, delayedOverdueMs: null, orphans: null });
    const text = toPrometheus([health as NonNullable<typeof health>]);
    expect(text).not.toContain('oldest_wait_age_seconds');
    expect(text).toContain(`bullmq_queue_jobs{queue="${prefix()}",state="wait"} 0`);
  });

  it('метка queue экранируется по формату экспозиции', () => {
    const text = toPrometheus([
      {
        prefix: 'a"b\\c',
        counts: {
          wait: 0,
          active: 0,
          paused: 0,
          delayed: 0,
          prioritized: 0,
          'waiting-children': 0,
          completed: 0,
          failed: 0,
        },
        paused: false,
        unexpectedTypes: [],
        stuckActive: { count: 0, scanned: 0, sample: [] },
        oldestWaitAgeMs: null,
        delayedOverdueMs: null,
        failedIncrease: null,
        orphans: null,
      },
    ]);
    expect(text).toContain('queue="a\\"b\\\\c"');
  });

  it('пустой список очередей и неверный потолок — отказ', () => {
    expect(() => new QueueInspector(redis, { queuePrefixes: [] })).toThrow('пуст');
    expect(() => new QueueInspector(redis, { queuePrefixes: ['x'], maxActiveScan: 0 })).toThrow(
      'maxActiveScan',
    );
  });
});
