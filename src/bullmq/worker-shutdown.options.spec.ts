import type { ShutdownableWorker } from './shutdownable-worker.type';
import { WorkerShutdown } from './worker-shutdown';

/**
 * Настройки выключения проверяются ДО первого шага: больше 2^31 − 1 мс Node
 * молча заменяет на 1 мс (TimeoutOverflowWarning), NaN — тоже на 1 мс, и
 * «дедлайн в месяц» прерывал бы задачи сразу. Отказ не отравляет `close`:
 * повтор с верными настройками выключает.
 */
const TIMER_LIMIT = 2_147_483_647;

const fakeWorker = () => {
  const calls: string[] = [];
  const worker: ShutdownableWorker = {
    pause: async () => {
      calls.push('pause');
    },
    close: async (force) => {
      calls.push(`close:${force === true}`);
    },
  };
  return { worker, calls };
};

describe('WorkerShutdown.close: настройки', () => {
  it.each([
    ['deadlineMs', { deadlineMs: TIMER_LIMIT + 1 }],
    ['deadlineMs', { deadlineMs: Number.NaN }],
    ['deadlineMs', { deadlineMs: -1 }],
    ['abortGraceMs', { deadlineMs: 1_000, abortGraceMs: TIMER_LIMIT + 1 }],
    ['abortGraceMs', { deadlineMs: 1_000, abortGraceMs: Number.NaN }],
    ['abortGraceMs', { deadlineMs: 1_000, abortGraceMs: -1 }],
    ['handBackDelayMs', { deadlineMs: 1_000, handBackDelayMs: TIMER_LIMIT + 1 }],
    ['handBackDelayMs', { deadlineMs: 1_000, handBackDelayMs: Number.NaN }],
    ['handBackDelayMs', { deadlineMs: 1_000, handBackDelayMs: -1 }],
  ])('%s вне [0, 2^31 − 1] (%o) — отказ до паузы Worker', async (field, options) => {
    const { worker, calls } = fakeWorker();
    const shutdown = new WorkerShutdown().attach(worker);
    await expect(shutdown.close(options)).rejects.toThrow(field);
    expect(calls).toEqual([]);
  });

  it('граница 2^31 − 1 принимается', async () => {
    const { worker, calls } = fakeWorker();
    const shutdown = new WorkerShutdown().attach(worker);
    await expect(
      shutdown.close({ deadlineMs: TIMER_LIMIT, abortGraceMs: TIMER_LIMIT, handBackDelayMs: TIMER_LIMIT }),
    ).resolves.toMatchObject({ drained: true, interrupted: [] });
    expect(calls).toEqual(['pause', 'close:false']);
  });

  it('отказ по настройкам не отравляет close: повтор с верными выключает', async () => {
    const { worker, calls } = fakeWorker();
    const shutdown = new WorkerShutdown().attach(worker);
    await expect(shutdown.close({ deadlineMs: TIMER_LIMIT + 1 })).rejects.toThrow('deadlineMs');
    await expect(shutdown.close({ deadlineMs: 1_000 })).resolves.toMatchObject({ drained: true });
    expect(calls).toEqual(['pause', 'close:false']);
  });

  it('дедлайн сверх предела таймера не прерывает идущую задачу через 1 мс', async () => {
    const { worker } = fakeWorker();
    const shutdown = new WorkerShutdown().attach(worker);
    let finish: () => void = () => undefined;
    const job = { id: 'j1' };
    const running = shutdown.track(
      (_job: typeof job, _token, signal) =>
        new Promise<string>((resolve, reject) => {
          finish = () => resolve('done');
          signal.addEventListener('abort', () => reject(signal.reason));
        }),
    )(job);
    const closing = shutdown.close({ deadlineMs: TIMER_LIMIT + 1 }).catch((error: Error) => error.message);
    await new Promise((resolve) => setImmediate(resolve));
    finish();
    await expect(running).resolves.toBe('done');
    await expect(closing).resolves.toContain('deadlineMs');
  });
});
