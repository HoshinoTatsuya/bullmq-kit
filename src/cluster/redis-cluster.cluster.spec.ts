import { randomUUID } from 'node:crypto';

import { Queue } from 'bullmq';
import { Cluster } from 'ioredis';

import { FairShare } from '../fair-share/fair-share';
import { GroupConcurrency } from '../group-concurrency/group-concurrency';
import { GroupRateLimit } from '../group-rate-limit/group-rate-limit';
import { QueueInspector } from '../inspect/queue-inspector';
import { OrphanedJobsCleanup } from '../orphan-cleanup/orphaned-jobs-cleanup';

/**
 * Обещание кластерной работы — прогоном на НАСТОЯЩЕМ Redis Cluster из трёх
 * мастеров (`scripts/with-redis-cluster.sh`): ключи одного скрипта в одном слоте
 * (хэш-тег префикса), ключи, собранные внутри скриптов, — в том же слоте,
 * обход SCAN по каждому мастеру, публикация из скрипта доходит подписчику на
 * другом узле. Без REDIS_CLUSTER_TEST_NODES набор пропускается, и
 * jest-assert-no-skipped валит прогон.
 */
const nodes = process.env.REDIS_CLUSTER_TEST_NODES;
const describeCluster = nodes ? describe : describe.skip;

describeCluster('Redis Cluster [нужен REDIS_CLUSTER_TEST_NODES]', () => {
  let cluster: Cluster;
  const startupNodes = () =>
    (nodes as string).split(',').map((node) => {
      const [host, port] = node.split(':');
      return { host: host as string, port: Number(port) };
    });

  beforeAll(async () => {
    cluster = new Cluster(startupNodes(), { redisOptions: { maxRetriesPerRequest: null } });
    await cluster.ping();
    // Соединение с узлом клиент кластера открывает лениво — при первой команде в его слот.
    // Открыть все здесь: иначе общий клиент файла открывал бы сокеты внутри тестов, и они
    // (закрываемые в afterAll) засчитывались бы тестам как оставленные.
    await Promise.all(cluster.nodes('all').map((node) => node.ping()));
  });
  afterAll(async () => {
    await cluster.quit();
  });

  it('фикстура — действительно кластер из трёх мастеров', async () => {
    expect(cluster.nodes('master')).toHaveLength(3);
    const info = String(await cluster.cluster('INFO'));
    expect(info).toContain('cluster_state:ok');
    // Контроль: ключи разных слотов в одном скрипте кластер действительно отвергает —
    // зелёные тесты ниже не потому, что всё легло на один узел.
    await expect(cluster.eval('return 1', 2, 'slot-a', 'slot-b')).rejects.toThrow('CROSSSLOT');
  });

  it('GroupConcurrency с хэш-тегом: acquireMany по трём группам, withSlot, держатели — без CROSSSLOT', async () => {
    const slots = new GroupConcurrency(cluster, { prefix: `{gc-${randomUUID()}}:slots` });
    const groups = ['project', 'tenant', 'ip'].map((group) => ({ group, limit: 1 }));
    expect((await slots.acquireMany('a', groups, 60_000)).outcome).toBe('acquired');
    expect((await slots.acquireMany('b', groups, 60_000)).outcome).toBe('limit');
    for (const { group } of groups) {
      expect(await slots.release(group, 'a')).toBe(true);
    }
    const result = await slots.withSlot('project', 'c', { limit: 1, ttlMs: 1_000 }, async () =>
      slots.count('project'),
    );
    expect(result).toEqual({ outcome: 'acquired', value: 1 });
  });

  it('без хэш-тега на клиенте кластера — отказ в конструкторе; тег в keyPrefix клиента засчитывается', async () => {
    expect(() => new GroupConcurrency(cluster, { prefix: 'plain:slots' })).toThrow('хэш-тега');
    const prefixed = new Cluster(startupNodes(), {
      keyPrefix: `{kp-${randomUUID()}}:`,
      redisOptions: { maxRetriesPerRequest: null },
    });
    try {
      const slots = new GroupConcurrency(prefixed, { prefix: 'slots' });
      const groups = ['g1', 'g2'].map((group) => ({ group, limit: 1 }));
      expect((await slots.acquireMany('a', groups, 60_000)).outcome).toBe('acquired');
      expect((await slots.acquireMany('b', groups, 60_000)).outcome).toBe('limit');
    } finally {
      await prefixed.quit();
    }
  });

  it('acquireWaiting: публикация из скрипта release доходит до подписчика на кластере', async () => {
    const subscriber = cluster.duplicate();
    try {
      const slots = new GroupConcurrency(cluster, { prefix: `{gw-${randomUUID()}}:slots`, subscriber });
      const slot = { limit: 1, ttlMs: 60_000 };
      await slots.acquire('g', 'X', slot);
      const waiting = slots.acquireWaiting('g', 'W', slot, { timeoutMs: 20_000, pollMs: 60_000 });
      await new Promise((resolve) => setTimeout(resolve, 200));
      await slots.release('g', 'X');
      await expect(waiting).resolves.toMatchObject({ outcome: 'acquired' });
    } finally {
      await subscriber.quit();
    }
  }, 30_000);

  it('FairShare и GroupRateLimit на кластере', async () => {
    const pool = new FairShare(cluster, { prefix: `{fs-${randomUUID()}}` });
    expect((await pool.acquire('p', 'A', 'h', { poolLimit: 1, ttlMs: 60_000 })).outcome).toBe('acquired');
    expect((await pool.acquire('p', 'B', 'h', { poolLimit: 1, ttlMs: 60_000 })).outcome).toBe('limit');
    expect(() => new FairShare(cluster, { prefix: 'fs' })).toThrow('хэш-тега');
    const rate = new GroupRateLimit(cluster, { prefix: `rl-${randomUUID()}` });
    expect((await rate.take('t', { limit: 1, intervalMs: 60_000 })).outcome).toBe('acquired');
    expect((await rate.take('t', { limit: 1, intervalMs: 60_000 })).outcome).toBe('limit');
  });

  /**
   * Очередь BullMQ на кластере (префикс с хэш-тегом — требование самой BullMQ):
   * уборка обходит каждый мастер, а проверка и удаление идут одним скриптом с
   * ключами `de:`/`dn:`/`<P>:meta`, собранными внутри, — в слоте тега.
   */
  it('уборка и инспектор: очередь BullMQ с хэш-тегом, обход по мастерам функцией', async () => {
    const name = `q-${randomUUID()}`;
    const bullPrefix = `{bull-${randomUUID()}}`;
    // Экземпляр, а не параметры: у bullmq своя копия ioredis, и её типы не совпадают с нашими;
    // BullMQ узнаёт клиента по методам (connect/disconnect/duplicate), так что на деле он годится.
    const queue = new Queue(name, { connection: cluster as never, prefix: bullPrefix });
    const prefix = `${bullPrefix}:${name}`;
    try {
      await queue.waitUntilReady();
      await queue.add('live', {}, { jobId: 'live' });
      await cluster.hset(`${prefix}:lost`, {
        name: 'n',
        data: '{}',
        opts: '{}',
        timestamp: String(Date.now() - 600_000),
        deid: 'd1',
      });
      await cluster.set(`${prefix}:de:d1`, 'lost');

      const report = await new OrphanedJobsCleanup(cluster, {
        queuePrefixes: [prefix],
        scanClients: () => cluster.nodes('master'),
      }).run();

      expect(report).toMatchObject({ scanned: 2, deleted: 1, unsupportedQueues: [] });
      expect(await cluster.exists(`${prefix}:lost`)).toBe(0);
      expect(await cluster.exists(`${prefix}:de:d1`)).toBe(0);
      expect(await cluster.type(`${prefix}:live`)).toBe('hash');
      expect(() => new OrphanedJobsCleanup(cluster, { queuePrefixes: [prefix] })).toThrow('scanClients');

      const [health] = await new QueueInspector(cluster, { queuePrefixes: [prefix] }).inspect();
      expect(health?.counts.wait).toBe(1);
    } finally {
      await queue.obliterate({ force: true });
      await queue.close();
    }
  });
});
