import { randomUUID } from 'node:crypto';

import { Queue, UnrecoverableError, Worker } from 'bullmq';

import { attachDeadLetter } from './attach-dead-letter';
import type { DeadLetterEntry } from './dead-letter-entry.type';
import { retryDeadLetter } from './retry-dead-letter';

/** Очередь мёртвых на НАСТОЯЩЕМ BullMQ: только окончательный провал, повтор из неё. */
const url = process.env.REDIS_TEST_URL;
const describeRedis = url ? describe : describe.skip;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describeRedis('очередь мёртвых [нужен REDIS_TEST_URL]', () => {
  let name: string;
  let queue: Queue;
  let dead: Queue<DeadLetterEntry>;
  const connection = () => {
    const { hostname, port } = new URL(url as string);
    return { host: hostname, port: Number(port), maxRetriesPerRequest: null };
  };
  beforeEach(() => {
    name = `q-${randomUUID()}`;
    queue = new Queue(name, { connection: connection() });
    dead = new Queue<DeadLetterEntry>(`${name}-dead`, { connection: connection() });
  });
  afterEach(async () => {
    for (const target of [queue, dead]) {
      await target.obliterate({ force: true });
      await target.close();
    }
  });

  const until = async (check: () => Promise<boolean>) => {
    for (let i = 0; i < 300 && !(await check()); i += 1) {
      await sleep(10);
    }
    expect(await check()).toBe(true);
  };

  it('копия — только после ОКОНЧАТЕЛЬНОГО провала, с причиной и данными; повтор из неё выполняет задачу', async () => {
    let healthy = false;
    const worker = new Worker(
      name,
      async (job) => {
        if (!healthy) {
          throw job.name === 'fatal'
            ? new UnrecoverableError('данные битые')
            : new Error(`попытка ${job.attemptsMade + 1}`);
        }
        return 'ok';
      },
      { connection: connection() },
    );
    const detach = attachDeadLetter(worker, dead);
    try {
      await queue.add(
        'retried',
        { n: 1 },
        { jobId: 'retried', attempts: 3, backoff: { type: 'fixed', delay: 10 } },
      );
      await queue.add('fatal', { n: 2 }, { jobId: 'fatal', attempts: 5 });
      await until(async () => (await dead.count()) === 2);
      // Провалы попыток до последней копий не дали: ровно по одной на задачу.
      await sleep(100);
      const entries = (await dead.getJobs(['wait'])).map((job) => job.data);
      expect(entries.map(({ jobId }) => jobId).sort()).toEqual(['fatal', 'retried']);
      const retried = entries.find(({ jobId }) => jobId === 'retried') as DeadLetterEntry;
      expect(retried).toMatchObject({
        queue: name,
        name: 'retried',
        data: { n: 1 },
        failedReason: 'попытка 3',
        attemptsMade: 3,
        opts: { attempts: 3, backoff: { type: 'fixed', delay: 10 } },
      });
      expect(retried.stacktrace.length).toBeGreaterThan(0);
      expect(entries.find(({ jobId }) => jobId === 'fatal')).toMatchObject({ failedReason: 'данные битые' });

      healthy = true;
      const [first] = await dead.getJobs(['wait']);
      if (first === undefined) {
        throw new Error('очередь мёртвых пуста');
      }
      const outcome = await retryDeadLetter(first, queue);
      expect(outcome).toBe('retried');
      const id = first.data.jobId;
      await until(async () => (await queue.getJobState(id)) === 'completed');
      expect(await dead.count()).toBe(1);
    } finally {
      detach();
      await worker.close();
    }
  });

  it('исходная удалена политикой хранения — повтор добавляет задачу заново с тем же id', async () => {
    const entry: DeadLetterEntry = {
      queue: name,
      jobId: 'gone',
      name: 'n',
      data: { x: 1 },
      opts: { attempts: 2 },
      failedReason: 'r',
      stacktrace: [],
      attemptsMade: 2,
      failedAt: 1,
    };
    const job = await dead.add('n', entry, { jobId: 'dl-1' });
    expect(await retryDeadLetter(job, queue)).toBe('readded');
    expect(await queue.getJobState('gone')).toBe('waiting');
    expect((await queue.getJob('gone'))?.opts.attempts).toBe(2);
    await expect(
      retryDeadLetter({ data: { ...entry, queue: 'other' }, remove: async () => undefined }, queue),
    ).rejects.toThrow('other');
  });
});
