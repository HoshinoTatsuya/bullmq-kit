import { randomUUID } from 'node:crypto';

import { Queue, Worker } from 'bullmq';
import Redis from 'ioredis';

import { GroupConcurrency } from '../group-concurrency/group-concurrency';
import { acquireOrDelay } from './acquire-or-delay';

/**
 * На НАСТОЯЩЕМ BullMQ и Redis: что задача действительно уходит в delayed (а не
 * в failed), просыпается к сроку слота и попытку не тратит, — знает только сам
 * Worker. Адрес — REDIS_TEST_URL; без него набор пропускается, и
 * jest-assert-no-skipped валит прогон.
 */
const url = process.env.REDIS_TEST_URL;
const describeRedis = url ? describe : describe.skip;

describeRedis('acquireOrDelay на BullMQ [нужен REDIS_TEST_URL]', () => {
  let redis: Redis;

  beforeAll(() => {
    redis = new Redis(url as string, { maxRetriesPerRequest: null });
  });
  afterAll(async () => {
    await redis.quit();
  });

  const connection = () => {
    const { hostname, port } = new URL(url as string);
    return { host: hostname, port: Number(port), maxRetriesPerRequest: null };
  };

  /**
   * Подсказка — худший случай (срок слота 60 с), а слот освобождается раньше:
   * держатель снял его через ~0.5 с. Задача просыпается по экспоненте от
   * `minDelayMs`, а не по подсказке, и выполняется вскоре после освобождения.
   * Прежде первая отсрочка равнялась подсказке — задача ждала бы 60 с.
   */
  it('слот освободился раньше подсказки: задача просыпается по экспоненте от minDelayMs и выполняется, не тратя попытку', async () => {
    const name = `q-${randomUUID()}`;
    const queue = new Queue(name, { connection: connection() });
    const slots = new GroupConcurrency(redis, { prefix: `gc-${randomUUID()}` });
    expect(await slots.acquire('g', 'other', { limit: 1, ttlMs: 60_000 })).toBe('acquired');

    const outcomes: string[] = [];
    const delays: number[] = [];
    const hints: Array<number | null> = [];
    let usedNow = 0;
    const worker = new Worker(
      name,
      async (job, token) => {
        // Задача — с записью задержки каждой отсрочки: момент пробуждения минус «сейчас» помощника.
        const recorded = {
          timestamp: job.timestamp,
          attemptsStarted: job.attemptsStarted,
          attemptsMade: job.attemptsMade,
          moveToDelayed: async (at: number, lockToken?: string) => {
            delays.push(at - usedNow);
            await job.moveToDelayed(at, lockToken);
          },
        };
        const outcome = await acquireOrDelay(
          recorded,
          token,
          async () => {
            const result = await slots.acquireDetailed('g', job.id as string, { limit: 1, ttlMs: 60_000 });
            outcomes.push(result.outcome);
            if (result.outcome === 'limit') {
              hints.push(result.retryAfterMs);
            }
            return result;
          },
          {
            jitterMs: 0,
            jitterFraction: 0,
            minDelayMs: 100,
            now: () => {
              usedNow = Date.now();
              return usedNow;
            },
          },
        );
        await slots.release('g', job.id as string);
        return outcome;
      },
      { connection: connection() },
    );
    try {
      const job = await queue.add('work', {});
      const id = job.id as string;
      for (let i = 0; i < 200 && delays.length < 2; i++) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      // Задержки — 100 и 200 мс (экспонента по числу отсрочек), подсказка при этом — весь срок слота.
      expect(delays.slice(0, 2)).toEqual([100, 200]);
      expect(hints[0]).toBeGreaterThan(50_000);

      await slots.release('g', 'other');
      for (let i = 0; i < 400 && (await queue.getJobState(id)) !== 'completed'; i++) {
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
      const done = await queue.getJob(id);
      expect(await done?.getState()).toBe('completed');
      expect(done?.returnvalue).toBe('acquired');
      expect(done?.attemptsMade).toBe(1);
      expect(outcomes.at(-1)).toBe('acquired');
      expect(outcomes.slice(0, -1).every((outcome) => outcome === 'limit')).toBe(true);
    } finally {
      await worker.close();
      await queue.obliterate({ force: true });
      await queue.close();
    }
  }, 20_000);
});
