'use strict';
/**
 * Замер уборки. Запуск: `pnpm build && scripts/with-redis.sh node bench/orphan-cleanup.cjs`.
 *
 * Очередь синтетическая (хэши задач с `timestamp` в прошлом, индекс `completed`),
 * рядом — чужие ключи, которые SCAN с MATCH обходит впустую. Меряется:
 * скорость прогона (ключей SCAN и хэшей задач в секунду) и то, насколько
 * прогон занимает Redis, — задержка PING с другого соединения во время прогона.
 * Второй сценарий — с длинным списком `wait`: проверка сироты делает LPOS по
 * списку целиком.
 *
 * `node bench/orphan-cleanup.cjs list-scan` — матрица для умолчания
 * `maxListScan`: длина `wait` 10 тыс. / 100 тыс. / 1 млн × потолок LPOS
 * (без потолка и несколько конечных). Сироты отсутствуют в `wait`, то есть
 * LPOS каждой проходит список до потолка — худший случай.
 * `list-scan-beyond` — то же при `wait` на 3 млн и снятом потолке снимка.
 */
const { performance } = require('node:perf_hooks');
const Redis = require('ioredis');
const { OrphanedJobsCleanup } = require('../dist/orphan-cleanup/index.js');

const PREFIX = 'bull:bench';
const pct = (values, p) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
};

async function fill(redis, { jobs, orphans, foreign, waitLength }) {
  await redis.flushall();
  // Версия BullMQ в meta — как у очереди, созданной new Queue: без неё уборка очередь не трогает.
  await redis.hset(`${PREFIX}:meta`, 'version', `bullmq:${require('bullmq/package.json').version}`);
  const old = String(Date.now() - 3_600_000);
  for (let start = 0; start < jobs; start += 5_000) {
    const pipeline = redis.pipeline();
    for (let i = start; i < Math.min(jobs, start + 5_000); i++) {
      // Поля формы задачи (`BULLMQ_KEYS.JOB_SHAPE_FIELDS`): без `opts` хэш — «не задача», и его не удаляют.
      pipeline.hset(`${PREFIX}:${i}`, 'name', 'job', 'opts', '{}', 'data', '{}', 'timestamp', old);
      if (i >= orphans) {
        pipeline.zadd(`${PREFIX}:completed`, i, String(i));
      }
    }
    await pipeline.exec();
  }
  for (let start = 0; start < foreign; start += 10_000) {
    const pipeline = redis.pipeline();
    for (let i = start; i < Math.min(foreign, start + 10_000); i++) {
      pipeline.set(`app:cache:${i}`, '1');
    }
    await pipeline.exec();
  }
  for (let start = 0; start < waitLength; start += 10_000) {
    const ids = [];
    for (let i = start; i < Math.min(waitLength, start + 10_000); i++) {
      ids.push(`w${i}`);
    }
    await redis.rpush(`${PREFIX}:wait`, ...ids);
  }
}

async function scenario(url, label, shape, extra = {}) {
  const redis = new Redis(url);
  const probe = new Redis(url);
  await fill(redis, shape);
  const cleanup = new OrphanedJobsCleanup(redis, {
    queuePrefixes: [PREFIX],
    maxJobsPerRun: 1_000_000,
    maxScannedKeysPerRun: 10_000_000,
    maxRunMs: 600_000,
    maxSnapshotZsetSize: 10_000_000,
    graceMs: 1_000,
    ...extra,
  });
  const pings = [];
  let running = true;
  const pinger = (async () => {
    while (running) {
      const t0 = performance.now();
      await probe.ping();
      pings.push(performance.now() - t0);
      await new Promise((resolve) => setTimeout(resolve, 2));
    }
  })();
  const started = performance.now();
  const report = await cleanup.run();
  const seconds = (performance.now() - started) / 1000;
  running = false;
  await pinger;
  console.log(
    `${label}: ${seconds.toFixed(1)} с; ключей SCAN ${report.scannedKeys} (${Math.round(report.scannedKeys / seconds)}/с), ` +
      `хэшей ${report.scanned} (${Math.round(report.scanned / seconds)}/с), удалено ${report.deleted}, ` +
      `не проверено ${report.kept.unverifiable}, не задача ${report.kept.notJob}; ` +
      `PING рядом (${pings.length}): p50 ${pct(pings, 50).toFixed(2)} мс, p99 ${pct(pings, 99).toFixed(2)} мс, max ${Math.max(...pings).toFixed(1)} мс`,
  );
  await redis.quit();
  await probe.quit();
}

async function main() {
  const url = process.env.REDIS_TEST_URL;
  if (!url) {
    throw new Error('REDIS_TEST_URL не задан: запуск через scripts/with-redis.sh');
  }
  if (process.argv[2] === 'list-scan-beyond') {
    // Список длиннее умолчания снимка (1 млн): снимок разрешён явно, и цену LPOS
    // одного кандидата не делит уже ничто — пайплайн не короче одного скрипта.
    for (const maxListScan of [Number.POSITIVE_INFINITY, 1_000_000]) {
      await scenario(
        url,
        `wait 3000000, maxSnapshotListSize Infinity, maxListScan ${maxListScan}`,
        { jobs: 2_000, orphans: 300, foreign: 0, waitLength: 3_000_000 },
        { maxListScan, maxSnapshotListSize: Number.POSITIVE_INFINITY },
      );
    }
    return;
  }
  if (process.argv[2] === 'list-scan') {
    // Три повтора на точку: в коротком прогоне PING-ов мало, и p99 одного прогона — почти max.
    for (const waitLength of [10_000, 100_000, 1_000_000]) {
      for (const maxListScan of [Number.POSITIVE_INFINITY, 1_000_000, 100_000, 10_000]) {
        for (let rep = 1; rep <= 3; rep++) {
          await scenario(
            url,
            `wait ${waitLength}, maxListScan ${maxListScan}, повтор ${rep}`,
            { jobs: 20_000, orphans: 2_000, foreign: 0, waitLength },
            { maxListScan },
          );
        }
      }
    }
    return;
  }
  await scenario(url, '100 тыс. задач, 10 тыс. сирот, 1 млн чужих ключей', {
    jobs: 100_000,
    orphans: 10_000,
    foreign: 1_000_000,
    waitLength: 0,
  });
  await scenario(url, 'то же + wait на 100 тыс.', {
    jobs: 100_000,
    orphans: 10_000,
    foreign: 1_000_000,
    waitLength: 100_000,
  });
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
