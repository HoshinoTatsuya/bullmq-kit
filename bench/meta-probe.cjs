'use strict';
/**
 * Проба: что BullMQ пишет в `<prefix>:meta` и когда. Запуск:
 * `scripts/with-redis.sh node bench/meta-probe.cjs`. Отвечает на вопрос гейта
 * версии уборки: по какому полю узнать версию BullMQ, создавшую очередь, и
 * есть ли оно у очереди, в которую только добавляли задачи.
 */
const Redis = require('ioredis');
const { Queue, Worker } = require('bullmq');

async function main() {
  setTimeout(() => {
    console.error('проба не уложилась в 60 с');
    process.exit(2);
  }, 60_000).unref();
  const url = process.env.REDIS_TEST_URL;
  const connection = new Redis(url, { maxRetriesPerRequest: null });
  await connection.flushall();
  const queue = new Queue('probe', { connection });
  await queue.waitUntilReady();
  console.log('после new Queue:', await connection.hgetall('bull:probe:meta'));
  await queue.add('job', { a: 1 });
  console.log('после add:', await connection.hgetall('bull:probe:meta'));
  const workerConnection = new Redis(url, { maxRetriesPerRequest: null });
  const worker = new Worker('probe', async () => 'ok', {
    connection: workerConnection,
    autorun: false,
  });
  const done = new Promise((resolve) => worker.on('completed', resolve));
  void worker.run();
  await done;
  console.log('после обработки:', await connection.hgetall('bull:probe:meta'));
  await worker.close();
  await queue.close();
  const pkg = require('bullmq/package.json');
  console.log('версия пакета:', pkg.version);
  await workerConnection.quit();
  await connection.quit();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
