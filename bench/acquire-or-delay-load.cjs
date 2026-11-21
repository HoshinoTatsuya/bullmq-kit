'use strict';
/**
 * Пропускная способность группы под потолком через `acquireOrDelay` на
 * настоящем BullMQ. Запуск:
 * `pnpm build && scripts/with-redis.sh node bench/acquire-or-delay-load.cjs [dist] [задач] [потолок] [работа мс] [ttl мс]`.
 * `dist` — каталог сборки пакета (по умолчанию `../dist`): так одна и та же
 * нагрузка гоняется против двух версий.
 *
 * Меряется то, что видит владелец очереди: за сколько выполнены все задачи,
 * сколько каждая ждала (finishedOn − timestamp), сколько отсрочек и скриптов
 * Redis на это ушло. Работа короткая, срок слота длинный — ровно то, при чём
 * подсказка `retryAfterMs` (худший случай) почти равна сроку слота.
 */
const { randomUUID } = require('node:crypto');
const { performance } = require('node:perf_hooks');
const path = require('node:path');
const Redis = require('ioredis');
const { Queue, Worker } = require('bullmq');

const [distArg, jobsArg, limitArg, workArg, ttlArg] = process.argv.slice(2);
const dist = path.resolve(__dirname, distArg ?? '../dist');
const { GroupConcurrency } = require(path.join(dist, 'index.js'));
const { acquireOrDelay } = require(path.join(dist, 'bullmq', 'index.js'));
const jobs = Number(jobsArg ?? 200);
const limit = Number(limitArg ?? 10);
const workMs = Number(workArg ?? 20);
const ttlMs = Number(ttlArg ?? 30_000);
const DEADLINE_MS = 240_000;

const pct = (values, p) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
};

(async () => {
  const url = new URL(process.env.REDIS_TEST_URL ?? 'redis://127.0.0.1:6379');
  const connection = { host: url.hostname, port: Number(url.port), maxRetriesPerRequest: null };
  const redis = new Redis(connection);
  await redis.flushall();
  await redis.config('RESETSTAT');
  const name = `bench-${randomUUID()}`;
  const queue = new Queue(name, { connection });
  const slots = new GroupConcurrency(redis, { prefix: 'bench-aod' });
  let active = 0;
  let maxActive = 0;
  let limited = 0;
  const worker = new Worker(
    name,
    async (job, token) => {
      await acquireOrDelay(job, token, async () => {
        const result = await slots.acquireDetailed('hot', `${name}:${job.id}`, { limit, ttlMs });
        if (result.outcome === 'limit') {
          limited += 1;
        }
        return result;
      });
      try {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, workMs));
      } finally {
        active -= 1;
        await slots.release('hot', `${name}:${job.id}`);
      }
    },
    { connection, concurrency: 50 },
  );
  const started = performance.now();
  await queue.addBulk(Array.from({ length: jobs }, (_, i) => ({ name: 'w', data: { i } })));
  let done = 0;
  while (done < jobs && performance.now() - started < DEADLINE_MS) {
    done = await queue.getCompletedCount();
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  const totalMs = Math.round(performance.now() - started);
  const completed = await queue.getJobs(['completed'], 0, -1);
  const waits = completed.map((job) => job.finishedOn - job.timestamp - workMs);
  const delays = completed.reduce((sum, job) => sum + Math.max(0, (job.attemptsStarted ?? 1) - 1), 0);
  const stats = await redis.info('commandstats');
  // EVAL — скрипты потолка групп, EVALSHA — скрипты самой BullMQ (отсрочки, переходы).
  const evals = Number(/cmdstat_eval:calls=(\d+)/.exec(stats)?.[1] ?? 0);
  const evalshas = Number(/cmdstat_evalsha:calls=(\d+)/.exec(stats)?.[1] ?? 0);
  console.log(
    JSON.stringify({
      dist: path.relative(process.cwd(), dist) || dist,
      jobs,
      limit,
      workMs,
      ttlMs,
      completed: completed.length,
      totalMs,
      idealMs: Math.ceil(jobs / limit) * workMs,
      maxActive,
      limitReplies: limited,
      delays,
      waitP50Ms: pct(waits, 50),
      waitP99Ms: pct(waits, 99),
      waitMaxMs: pct(waits, 100),
      groupScripts: evals,
      bullmqScripts: evalshas,
    }),
  );
  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
  redis.disconnect();
})();
