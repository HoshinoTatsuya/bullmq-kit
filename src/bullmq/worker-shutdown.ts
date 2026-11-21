import { performance } from 'node:perf_hooks';

import { DelayedError } from 'bullmq';

import { assertTimerMs } from './assert-timer-ms';
import { ShutdownDeadlineError } from './shutdown-deadline.error';
import type { ShutdownReport } from './shutdown-report.type';
import type { ShutdownableWorker } from './shutdownable-worker.type';
import type { TrackedJob } from './tracked-job.type';
import type { WorkerShutdownOptions } from './worker-shutdown-options.type';

/**
 * Корректное выключение Worker BullMQ под grace-период пода. Порядок шагов —
 * в нём ошибаются чаще всего:
 *  1. перестать брать новые задачи (`pause(true)`), не дожидаясь идущих —
 *     иначе за время дренажа Worker наберёт новых. Пауза BullMQ не отменяет
 *     уже начатого блокирующего ожидания задачи: пришедшую в него Worker
 *     возьмёт и после паузы (и после `close`). Такая задача не выполняется, а
 *     отдаётся обратно нетронутой (`moveToDelayed(через handBackDelayMs)` + `DelayedError`,
 *     попытка не тратится) — `handedBack` в отчёте;
 *  2. ждать завершения НАЧАТЫХ задач до дедлайна (учёт — обёрткой процессора,
 *     а не событиями: отложенная `DelayedError` задача не шлёт ни `completed`,
 *     ни `failed`);
 *  3. дедлайн вышел — прервать сигнал задач, дать им `abortGraceMs` и закрыть
 *     `close(true)`; дренаж успел — обычный `close()`;
 *  4. отчёт: успел ли, какие задачи прерваны.
 *
 * ```ts
 * const shutdown = new WorkerShutdown();
 * const worker = new Worker(name, shutdown.track(async (job, token, signal) => { ... }), { connection });
 * shutdown.attach(worker);
 * process.once('SIGTERM', async () => log(await shutdown.close({ deadlineMs: 25_000 })));
 * ```
 */
export class WorkerShutdown {
  private static readonly DEFAULT_ABORT_GRACE_MS = 1_000;
  private static readonly DEFAULT_HAND_BACK_DELAY_MS = 1_000;
  private readonly _inFlight = new Map<TrackedJob, AbortController>();
  private _worker: ShutdownableWorker | undefined;
  private _drained: (() => void) | undefined;
  private _closing: Promise<ShutdownReport> | undefined;
  private _stopping = false;
  private _handBackDelayMs = WorkerShutdown.DEFAULT_HAND_BACK_DELAY_MS;
  private readonly _handedBack: string[] = [];

  /**
   * Обёртка процессора: учёт начатых задач и сигнал, прерываемый дедлайном
   * выключения (вместе с сигналом самого BullMQ, если он его передал).
   */
  public track<J extends TrackedJob, R>(
    processor: (job: J, token: string | undefined, signal: AbortSignal) => Promise<R>,
  ): (job: J, token?: string, signal?: AbortSignal) => Promise<R> {
    return async (job, token, signal) => {
      if (this._stopping && typeof job.moveToDelayed === 'function' && token !== undefined) {
        // Не «сейчас»: до настоящей паузы этот же Worker взял бы её снова — по кругу.
        await job.moveToDelayed(Date.now() + this._handBackDelayMs, token);
        this._handedBack.push(String(job.id));
        throw new DelayedError();
      }
      const controller = new AbortController();
      this._inFlight.set(job, controller);
      // Сигнал BullMQ — переносом события, а не AbortSignal.any: в части версий BullMQ
      // (5.72) он не нативный AbortSignal, и AbortSignal.any отказывает — задача падала.
      const forward = () => controller.abort(signal?.reason);
      if (signal?.aborted === true) {
        forward();
      } else {
        signal?.addEventListener?.('abort', forward, { once: true });
      }
      try {
        return await processor(job, token, controller.signal);
      } finally {
        signal?.removeEventListener?.('abort', forward);
        this._inFlight.delete(job);
        if (this._inFlight.size === 0) {
          this._drained?.();
        }
      }
    };
  }

  public attach(worker: ShutdownableWorker): this {
    this._worker = worker;
    return this;
  }

  /** id начатых и не завершённых задач. */
  public inFlight(): string[] {
    return [...this._inFlight.keys()].map((job) => String(job.id));
  }

  /**
   * Выключить; повторный вызов получает тот же отчёт. Неверные настройки —
   * отказ ДО первого шага, и он не запоминается: повтор с верными выключает.
   */
  public close(options: WorkerShutdownOptions): Promise<ShutdownReport> {
    if (this._closing === undefined) {
      try {
        assertTimerMs('deadlineMs', options.deadlineMs);
        assertTimerMs('abortGraceMs', options.abortGraceMs ?? WorkerShutdown.DEFAULT_ABORT_GRACE_MS);
        assertTimerMs(
          'handBackDelayMs',
          options.handBackDelayMs ?? WorkerShutdown.DEFAULT_HAND_BACK_DELAY_MS,
        );
      } catch (error) {
        return Promise.reject(error);
      }
      this._closing = this._close(options);
    }
    return this._closing;
  }

  private async _close({
    deadlineMs,
    abortGraceMs = WorkerShutdown.DEFAULT_ABORT_GRACE_MS,
    handBackDelayMs = WorkerShutdown.DEFAULT_HAND_BACK_DELAY_MS,
  }: WorkerShutdownOptions): Promise<ShutdownReport> {
    const worker = this._worker;
    if (worker === undefined) {
      throw new Error('WorkerShutdown: Worker не подключён (attach)');
    }
    const started = performance.now();
    this._handBackDelayMs = handBackDelayMs;
    this._stopping = true;
    await worker.pause(true);
    const drained = await this._waitDrained(deadlineMs);
    const interrupted = drained ? [] : this.inFlight();
    if (!drained) {
      for (const controller of this._inFlight.values()) {
        controller.abort(new ShutdownDeadlineError(deadlineMs));
      }
      await this._waitDrained(abortGraceMs);
    }
    await worker.close(!drained);
    return {
      drained,
      interrupted,
      handedBack: [...this._handedBack],
      durationMs: Math.round(performance.now() - started),
    };
  }

  private _waitDrained(ms: number): Promise<boolean> {
    if (this._inFlight.size === 0) {
      return Promise.resolve(true);
    }
    return new Promise<boolean>((resolve) => {
      const timer = setTimeout(() => {
        this._drained = undefined;
        resolve(false);
      }, ms);
      this._drained = () => {
        clearTimeout(timer);
        this._drained = undefined;
        resolve(true);
      };
    });
  }
}
