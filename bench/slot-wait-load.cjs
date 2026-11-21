'use strict';
/**
 * Нагрузка на одну горячую группу через `withSlot`: опрос (повтор по отказу
 * `limit` через min(50 мс, подсказка)) против ожидания (`withSlot(..., { timeoutMs })`:
 * очередь ожидающих, пробуждение первого публикацией при снятии). Запуск:
 * `pnpm build && scripts/with-redis.sh node bench/slot-wait-load.cjs [dist] [работ] [потолок] [работа мс] [ttl мс] [poll|wait]`.
 *
 * Меряется: сколько всё заняло против идеала (работ / потолок × работа),
 * сколько каждая ждала слот, что говорила подсказка, сколько скриптов Redis.
 */
const { performance } = require('node:perf_hooks');
const path = require('node:path');
const Redis = require('ioredis');

const [distArg, worksArg, limitArg, workArg, ttlArg, modeArg] = process.argv.slice(2);
const dist = path.resolve(__dirname, distArg ?? '../dist');
const { GroupConcurrency } = require(path.join(dist, 'index.js'));
const works = Number(worksArg ?? 200);
const limit = Number(limitArg ?? 10);
const workMs = Number(workArg ?? 20);
const ttlMs = Number(ttlArg ?? 30_000);
const mode = modeArg ?? 'poll';

const pct = (values, p) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
};

(async () => {
  const url = process.env.REDIS_TEST_URL ?? 'redis://127.0.0.1:6379';
  const redis = new Redis(url);
  const subscriber = new Redis(url);
  await redis.flushall();
  await redis.config('RESETSTAT');
  const slots = new GroupConcurrency(redis, { prefix: 'load', subscriber });
  const slot = { limit, ttlMs };
  const hints = [];
  const waits = [];
  let active = 0;
  let maxActive = 0;
  let limits = 0;
  const work = async () => {
    active += 1;
    maxActive = Math.max(maxActive, active);
    await new Promise((resolve) => setTimeout(resolve, workMs));
    active -= 1;
  };
  const started = performance.now();
  await Promise.all(
    Array.from({ length: works }, async (_, i) => {
      const begin = performance.now();
      if (mode === 'wait') {
        const result = await slots.withSlot('one', `h${i}`, slot, work, { timeoutMs: 120_000 });
        if (result.outcome === 'limit') {
          limits += 1;
        }
      } else {
        for (;;) {
          const result = await slots.withSlot('one', `h${i}`, slot, work);
          if (result.outcome !== 'limit') {
            break;
          }
          limits += 1;
          hints.push(result.retryAfterMs);
          await new Promise((resolve) => setTimeout(resolve, Math.min(50, result.retryAfterMs ?? 50)));
        }
      }
      waits.push(performance.now() - begin - workMs);
    }),
  );
  const stats = await redis.info('commandstats');
  const evals = Number(/cmdstat_eval:calls=(\d+)/.exec(stats)?.[1] ?? 0);
  console.log(
    JSON.stringify({
      dist: path.basename(path.dirname(path.join(dist, 'x'))),
      mode,
      works,
      limit,
      workMs,
      ttlMs,
      totalMs: Math.round(performance.now() - started),
      idealMs: Math.ceil(works / limit) * workMs,
      maxActive,
      limitReplies: limits,
      waitP50Ms: Math.round(pct(waits, 50)),
      waitMaxMs: Math.round(pct(waits, 100)),
      hintP50Ms: hints.length > 0 ? pct(hints, 50) : null,
      evals,
    }),
  );
  subscriber.disconnect();
  redis.disconnect();
})();
