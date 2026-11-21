'use strict';
/**
 * Замер предела потолка групп. Запуск:
 * `pnpm build && scripts/with-redis.sh node bench/capacity.cjs`.
 *
 * Меряется то, что решает при эксплуатации:
 *  1. пропускная способность «занять → снять» от числа параллельных вызывающих
 *     (одна горячая группа и много разных групп);
 *  2. цена захвата от числа держателей группы (ZSET: prune + ZCARD + ZRANGE);
 *  3. память Redis на группу — пространство ключей задаётся внешним вводом.
 */
const { performance } = require('node:perf_hooks');
const Redis = require('ioredis');
const { GroupConcurrency } = require('../dist/index.js');

const pct = (values, p) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
};
const ms = (value) => value.toFixed(2);

async function cycles(url, { callers, groups, perCaller, limit }) {
  const clients = Array.from({ length: Math.min(callers, 8) }, () => new Redis(url));
  const latencies = [];
  let limited = 0;
  const started = performance.now();
  await Promise.all(
    Array.from({ length: callers }, async (_, caller) => {
      const slots = new GroupConcurrency(clients[caller % clients.length], { prefix: 'bench' });
      for (let i = 0; i < perCaller; i++) {
        const group = `g${(caller * perCaller + i) % groups}`;
        const holder = `h${caller}-${i}`;
        const t0 = performance.now();
        const outcome = await slots.acquire(group, holder, { limit, ttlMs: 30_000 });
        if (outcome === 'limit') {
          limited++;
        } else {
          await slots.release(group, holder);
        }
        latencies.push(performance.now() - t0);
      }
    }),
  );
  const seconds = (performance.now() - started) / 1000;
  await Promise.all(clients.map((client) => client.quit()));
  return {
    perSecond: Math.round((callers * perCaller) / seconds),
    p50: pct(latencies, 50),
    p99: pct(latencies, 99),
    limited,
  };
}

async function main() {
  const url = process.env.REDIS_TEST_URL;
  if (!url) {
    throw new Error('REDIS_TEST_URL не задан: запуск через scripts/with-redis.sh');
  }
  const admin = new Redis(url);
  await admin.flushall();

  console.log('## 1. занять → снять, пары в секунду (ttl 30 с)');
  for (const [label, groups, limit] of [
    ['одна группа, потолок 10', 1, 10],
    ['10 тыс. групп, потолок 10', 10_000, 10],
  ]) {
    for (const callers of [1, 8, 32, 128]) {
      const result = await cycles(url, { callers, groups, perCaller: Math.ceil(8_000 / callers), limit });
      console.log(
        `${label}; вызывающих ${callers}: ${result.perSecond}/с, p50 ${ms(result.p50)} мс, p99 ${ms(result.p99)} мс, отказов limit ${result.limited}`,
      );
    }
    await admin.flushall();
  }

  console.log('## 2. захват при N держателях группы (без потолка)');
  const slots = new GroupConcurrency(admin, { prefix: 'bench' });
  for (const holders of [0, 1_000, 100_000]) {
    await admin.flushall();
    if (holders > 0) {
      const deadline = Date.now() + 600_000;
      for (let start = 0; start < holders; start += 10_000) {
        const args = [];
        for (let i = start; i < Math.min(holders, start + 10_000); i++) {
          args.push(deadline, `pre-${i}`);
        }
        await admin.zadd('bench:g-big', ...args);
      }
    }
    const latencies = [];
    for (let i = 0; i < 2_000; i++) {
      const t0 = performance.now();
      await slots.acquire('g-big', `x${i}`, { limit: Number.POSITIVE_INFINITY, ttlMs: 600_000 });
      latencies.push(performance.now() - t0);
    }
    const slow = await admin.slowlog('get', 1);
    console.log(
      `держателей ${holders}: p50 ${ms(pct(latencies, 50))} мс, p99 ${ms(pct(latencies, 99))} мс; slowlog ${slow.length}`,
    );
  }

  console.log('## 3. память на группу (1 держатель)');
  await admin.flushall();
  const before = Number(/used_memory:(\d+)/.exec(await admin.info('memory'))[1]);
  const pipeline = admin.pipeline();
  for (let i = 0; i < 100_000; i++) {
    pipeline.zadd(`bench:project-${i}`, Date.now() + 60_000, `holder-${i}`);
    pipeline.pexpire(`bench:project-${i}`, 60_000);
  }
  await pipeline.exec();
  const after = Number(/used_memory:(\d+)/.exec(await admin.info('memory'))[1]);
  console.log(`100 тыс. групп: ${Math.round((after - before) / 1024 / 1024)} МБ, ${Math.round((after - before) / 100_000)} байт на группу`);
  await admin.quit();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
