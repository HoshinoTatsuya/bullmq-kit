import { randomUUID } from 'node:crypto';

import { Queue, Worker } from 'bullmq';

import { ShutdownDeadlineError } from './shutdown-deadline.error';
import { WorkerShutdown } from './worker-shutdown';

/** Выключение Worker на НАСТОЯЩЕМ BullMQ: порядок шагов и дедлайн. */
const url = process.env.REDIS_TEST_URL;
const describeRedis = url ? describe : describe.skip;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describeRedis('WorkerShutdown [нужен REDIS_TEST_URL]', () => {
  let name: string;
  let queue: Queue;
  const connection = () => {
    const { hostname, port } = new URL(url as string);
    return { host: hostname, port: Number(port), maxRetriesPerRequest: null };
  };
  beforeEach(() => {
    name = `q-${randomUUID()}`;
    queue = new Queue(name, { connection: connection() });
  });
  afterEach(async () => {
    await queue.obliterate({ force: true });
    await queue.close();
  });

  /**
   * Ждать, пока задачу начал ПРОЦЕССОР, а не пока BullMQ перевёл её в active:
   * BullMQ ставит active раньше вызова процессора, и выключение, начатое между
   * ними, честно отдаёт такую задачу обратно — фикстура проверяла бы гонку.
   */
  const untilStarted = async (started: readonly string[], id: string) => {
    for (let i = 0; i < 200 && !started.includes(id); i += 1) {
      await sleep(10);
    }
    expect(started).toContain(id);
  };

  it('начатая задача успевает до дедлайна — drained, новые задачи после начала выключения не берутся', async () => {
    const shutdown = new WorkerShutdown();
    const started: string[] = [];
    let release: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    // Два места: без паузы Worker взял бы позднюю задачу вторым, пока первая держит первое.
    const worker = new Worker(
      name,
      shutdown.track(async (job) => {
        started.push(String(job.id));
        await gate;
      }),
      { connection: connection(), concurrency: 2 },
    );
    shutdown.attach(worker);
    await queue.add('first', {}, { jobId: 'first' });
    await untilStarted(started, 'first');

    const closing = shutdown.close({ deadlineMs: 10_000 });
    await sleep(100);
    await queue.add('late', {}, { jobId: 'late' });
    await sleep(200);
    release();
    const report = await closing;

    expect(report).toMatchObject({ drained: true, interrupted: [] });
    expect(await queue.getJobState('first')).toBe('completed');
    // Добавленная после начала выключения не выполнялась. Взял ли её Worker,
    // ждавший задачу блокирующим вызовом (BullMQ его паузой не отменяет), —
    // гонка; взял — отдал обратно не больше одного раза (пауза остановила
    // дальнейшую выборку), без траты попытки.
    expect(started).toEqual(['first']);
    expect(['', 'late']).toContain(report.handedBack.join(','));
    expect(['delayed', 'waiting']).toContain(await queue.getJobState('late'));
    expect((await queue.getJob('late'))?.attemptsMade).toBe(0);
  });

  /**
   * Задача приходит, когда выключение уже началось, но Worker ещё выбирает
   * (ровно гонка блокирующего ожидания BullMQ): подменённая пауза кладёт её
   * в очередь до настоящей паузы. Выполнять её нельзя — она отдаётся обратно.
   */
  it('задача, взятая после начала выключения, не выполняется и отдаётся обратно без траты попытки', async () => {
    const shutdown = new WorkerShutdown();
    const started: string[] = [];
    let release: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const worker = new Worker(
      name,
      shutdown.track(async (job) => {
        started.push(String(job.id));
        await gate;
      }),
      { connection: connection(), concurrency: 2 },
    );
    shutdown.attach({
      pause: async (doNotWaitActive) => {
        await queue.add('racing', {}, { jobId: 'racing' });
        for (let i = 0; i < 100 && (await queue.getJobState('racing')) === 'waiting'; i += 1) {
          await sleep(10);
        }
        await worker.pause(doNotWaitActive);
      },
      close: (force) => worker.close(force),
    });
    await queue.add('first', {}, { jobId: 'first' });
    await untilStarted(started, 'first');

    const closing = shutdown.close({ deadlineMs: 10_000 });
    await sleep(200);
    release();
    const report = await closing;

    expect(started).toEqual(['first']);
    expect(report.handedBack).toEqual(['racing']);
    expect(['delayed', 'waiting']).toContain(await queue.getJobState('racing'));
    expect((await queue.getJob('racing'))?.attemptsMade).toBe(0);
  });

  it('дедлайн вышел — сигнал задачи прерван ShutdownDeadlineError, она названа в отчёте, close не ждёт её', async () => {
    const shutdown = new WorkerShutdown();
    const started: string[] = [];
    let reason: unknown;
    const worker = new Worker(
      name,
      shutdown.track(
        (job, _token, signal) =>
          new Promise<void>(() => {
            started.push(String(job.id));
            signal.addEventListener('abort', () => {
              reason = signal.reason;
            });
          }),
      ),
      { connection: connection() },
    );
    shutdown.attach(worker);
    await queue.add('stuck', {}, { jobId: 'stuck' });
    await untilStarted(started, 'stuck');

    const report = await shutdown.close({ deadlineMs: 300, abortGraceMs: 50 });

    expect(report).toMatchObject({ drained: false, interrupted: ['stuck'] });
    expect(reason).toBeInstanceOf(ShutdownDeadlineError);
    // Повторный вызов — тот же отчёт, а не второе выключение.
    await expect(shutdown.close({ deadlineMs: 0 })).resolves.toBe(report);
  });

  it('без attach — отказ с причиной', async () => {
    await expect(new WorkerShutdown().close({ deadlineMs: 0 })).rejects.toThrow('attach');
  });
});
