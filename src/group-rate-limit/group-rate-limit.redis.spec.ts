import { randomUUID } from 'node:crypto';

import Redis from 'ioredis';

import { GroupRateLimit } from './group-rate-limit';

/**
 * Темп группы на НАСТОЯЩЕМ Redis: ведро токенов, время сервера. Утверждения —
 * про значения, которые отдаёт Redis (подсказка, срок ключа), а не про часы
 * теста.
 */
const url = process.env.REDIS_TEST_URL;
const describeRedis = url ? describe : describe.skip;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describeRedis('GroupRateLimit [нужен REDIS_TEST_URL]', () => {
  let redis: Redis;
  let prefix: string;
  let limiter: GroupRateLimit;

  beforeAll(() => {
    redis = new Redis(url as string, { maxRetriesPerRequest: null });
  });
  afterAll(async () => {
    await redis.quit();
  });
  beforeEach(() => {
    prefix = `rl-${randomUUID()}`;
    limiter = new GroupRateLimit(redis, { prefix });
  });

  it('всплеск до ёмкости, дальше — отказ с ТОЧНОЙ подсказкой (интервал / ёмкость)', async () => {
    const rate = { limit: 5, intervalMs: 1_000 };
    const outcomes = [];
    for (let i = 0; i < 6; i += 1) {
      outcomes.push(await limiter.take('tenant', rate));
    }
    expect(outcomes.slice(0, 5).map(({ outcome }) => outcome)).toEqual(Array(5).fill('acquired'));
    const refused = outcomes[5];
    expect(refused).toMatchObject({ outcome: 'limit', exact: true });
    // Один токен наливается за 200 мс; с начала всплеска прошло немного — подсказка близка к 200.
    expect(refused?.retryAfterMs).toBeGreaterThan(100);
    expect(refused?.retryAfterMs).toBeLessThanOrEqual(200);
  });

  it('по подсказке токен есть: ожидание retryAfterMs — следующий захват проходит', async () => {
    const rate = { limit: 2, intervalMs: 400 };
    await limiter.take('g', rate);
    await limiter.take('g', rate);
    const refused = await limiter.take('g', rate);
    expect(refused.outcome).toBe('limit');
    await sleep((refused.retryAfterMs ?? 0) + 20);
    expect((await limiter.take('g', rate)).outcome).toBe('acquired');
  });

  it('группы независимы; ключ живёт не дольше полного наполнения ведра', async () => {
    const rate = { limit: 3, intervalMs: 3_000 };
    await limiter.take('a', rate);
    expect((await limiter.take('b', rate)).outcome).toBe('acquired');
    const pttl = await redis.pttl(`${prefix}:a`);
    // Взят один токен из трёх: до полного ведра — треть интервала.
    expect(pttl).toBeGreaterThan(500);
    expect(pttl).toBeLessThanOrEqual(1_000);
  });

  it('стоимость больше одного токена и неверные аргументы — отказ до Redis', async () => {
    const rate = { limit: 4, intervalMs: 1_000 };
    expect((await limiter.take('g', rate, 3)).outcome).toBe('acquired');
    expect((await limiter.take('g', rate, 3)).outcome).toBe('limit');
    await expect(limiter.take('g', rate, 5)).rejects.toThrow('cost');
    await expect(limiter.take('g', { limit: 0, intervalMs: 1_000 })).rejects.toThrow('limit');
    await expect(limiter.take('g', { limit: 1, intervalMs: 0 })).rejects.toThrow('intervalMs');
    await expect(limiter.take(' ', rate)).rejects.toThrow('группа');
    expect(() => new GroupRateLimit(redis, { prefix: ' ' })).toThrow('prefix');
  });

  it('параллельные захваты атомарны: 30 одновременных при ёмкости 10 — ровно 10', async () => {
    const results = await Promise.all(
      Array.from({ length: 30 }, () => limiter.take('hot', { limit: 10, intervalMs: 60_000 })),
    );
    expect(results.filter(({ outcome }) => outcome === 'acquired')).toHaveLength(10);
  });
});
