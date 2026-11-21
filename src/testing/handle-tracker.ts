import { type AsyncHook, createHook } from 'node:async_hooks';

/**
 * Учёт ресурсов цикла событий (таймеры, сокеты, серверы), созданных между
 * `start()` и `stop()`, с местом создания. Живость — у самого объекта, а не по
 * событию destroy: таймер жив, пока не сработал, не снят и `hasRef()`; хэндл —
 * пока `hasRef()` (у закрытого он `false`). Ресурс с `unref()` процесс не
 * держит и не в счёт.
 */
export class HandleTracker {
  private readonly _tracked = new Map<
    number,
    { type: string; resource: { hasRef?: () => boolean; _destroyed?: boolean }; stack: string }
  >();
  private readonly _hook: AsyncHook;

  public constructor() {
    this._hook = createHook({
      init: (asyncId, type, _trigger, resource) => {
        if (type === 'PROMISE') {
          return;
        }
        const holder = { stack: '' };
        const limit = Error.stackTraceLimit;
        Error.stackTraceLimit = 40;
        Error.captureStackTrace(holder);
        Error.stackTraceLimit = limit;
        this._tracked.set(asyncId, { type, resource: resource as never, stack: holder.stack });
      },
    });
  }

  public start(): void {
    this._tracked.clear();
    this._hook.enable();
  }

  public stop(): void {
    this._hook.disable();
  }

  /** Живые ресурсы, держащие процесс: тип и место создания. */
  public alive(): { type: string; stack: string }[] {
    return [...this._tracked.values()]
      .filter(({ resource }) => resource._destroyed !== true && resource.hasRef?.() === true)
      .map(({ type, stack }) => ({ type, stack }));
  }
}
