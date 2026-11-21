import { randomUUID } from 'node:crypto';

import Redis from 'ioredis';

import { FairShare } from './fair-share';

/** Общий пул со справедливой долей — на НАСТОЯЩЕМ Redis. */
const url = process.env.REDIS_TEST_URL;
const describeRedis = url ? describe : describe.skip;

describeRedis('FairShare [нужен REDIS_TEST_URL]', () => {
  let redis: Redis;
  let pool: FairShare;

  beforeAll(() => {
    redis = new Redis(url as string, { maxRetriesPerRequest: null });
  });
  afterAll(async () => {
    await redis.quit();
  });
  beforeEach(() => {
    pool = new FairShare(redis, { prefix: `fs-${randomUUID()}` });
  });

  const slot = (weight = 1) => ({ poolLimit: 4, ttlMs: 60_000, weight });
  const fill = async (group: string, count: number, weight = 1) => {
    const outcomes = [];
    for (let i = 0; i < count; i += 1) {
      outcomes.push((await pool.acquire('p', group, `${group}-${i}`, slot(weight))).outcome);
    }
    return outcomes;
  };

  it('одна группа без соперников занимает весь пул: свободное не простаивает', async () => {
    expect(await fill('A', 5)).toEqual(['acquired', 'acquired', 'acquired', 'acquired', 'limit']);
  });

  it('пока ждёт другая группа, крупная сверх доли новых слотов не берёт: освободившийся достаётся ждущей', async () => {
    await fill('A', 4);
    // Пул полон — B упирается и становится ждущей.
    expect((await pool.acquire('p', 'B', 'B-0', slot())).outcome).toBe('limit');
    await pool.release('p', 'A', 'A-0');

    const greedy = await pool.acquire('p', 'A', 'A-9', slot());
    const fair = await pool.acquire('p', 'B', 'B-0', slot());

    expect(greedy).toMatchObject({ outcome: 'limit', share: 2, held: 3 });
    expect(fair).toMatchObject({ outcome: 'acquired', share: 2, held: 1 });
  });

  /** Повторный захват своего слота (held) отвечал долей 0 — «группе не положено ничего». */
  it('повторный захват удерживаемого слота — доля группы считается, как при новом', async () => {
    await fill('A', 2);
    expect((await pool.acquire('p', 'B', 'B-0', slot())).outcome).toBe('acquired');
    const again = await pool.acquire('p', 'A', 'A-0', slot());
    expect(again).toMatchObject({ outcome: 'held', share: 2, held: 2 });
    const weighted = await pool.acquire('p', 'B', 'B-0', slot(3));
    expect(weighted).toMatchObject({ outcome: 'held', share: 3, held: 1 });
  });

  it('вес задаёт долю: 3 к 1 при пуле 4 — доли 3 и 1', async () => {
    await fill('B', 4, 1);
    expect((await pool.acquire('p', 'A', 'A-0', slot(3))).outcome).toBe('limit');
    for (let i = 0; i < 3; i += 1) {
      await pool.release('p', 'B', `B-${i}`);
    }
    const outcomes = [];
    for (let i = 0; i < 4; i += 1) {
      outcomes.push(await pool.acquire('p', 'A', `A-${i}`, slot(3)));
    }
    expect(outcomes.map(({ outcome }) => outcome)).toEqual(['acquired', 'acquired', 'acquired', 'limit']);
    expect(outcomes[0]?.share).toBe(3);
    // Пул полон: B (держит свою долю) нового слота не получает.
    expect((await pool.acquire('p', 'B', 'B-9', slot(1))).outcome).toBe('limit');
  });

  it('держатель повторно — held и продление; снятие идемпотентно; держатели — с группой', async () => {
    await pool.acquire('p', 'A:x', 'h1', slot());
    expect((await pool.acquire('p', 'A:x', 'h1', slot())).outcome).toBe('held');
    expect(await pool.holders('p')).toEqual([
      { group: 'A:x', holder: 'h1', expiresInMs: expect.any(Number) },
    ]);
    expect(await pool.release('p', 'A:x', 'h1')).toBe(true);
    expect(await pool.release('p', 'A:x', 'h1')).toBe(false);
    expect(await pool.holders('p')).toEqual([]);
  });

  it('ждущая перестаёт считаться ждущей по сроку: крупная снова берёт сверх доли', async () => {
    await fill('A', 4);
    expect((await pool.acquire('p', 'B', 'B-0', { ...slot(), waitTtlMs: 100 })).outcome).toBe('limit');
    await pool.release('p', 'A', 'A-0');
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect((await pool.acquire('p', 'A', 'A-9', slot())).outcome).toBe('acquired');
  });

  it('неверные аргументы и кластер без хэш-тега — отказ до Redis', async () => {
    await expect(pool.acquire('p', 'A', 'h', { poolLimit: 0, ttlMs: 1_000 })).rejects.toThrow('poolLimit');
    await expect(pool.acquire('p', 'A', 'h', { poolLimit: 1, ttlMs: 1_000, weight: 0 })).rejects.toThrow(
      'weight',
    );
    await expect(pool.acquire('p', ' ', 'h', slot())).rejects.toThrow('группа');
    expect(() => new FairShare(redis, { prefix: 'fs', cluster: true })).toThrow('хэш-тега');
    expect(() => new FairShare(redis, { prefix: '{fs}', cluster: true })).not.toThrow();
  });
});
