/** Настройки выключения Worker. */
export interface WorkerShutdownOptions {
  /**
   * Сколько ждать завершения начатых задач, мс. Ставьте меньше grace-периода
   * пода (`terminationGracePeriodSeconds`) с запасом на `close`: после него
   * придёт SIGKILL, и ни отчёта, ни закрытия соединений не будет.
   */
  readonly deadlineMs: number;
  /** Сколько дать прерванным задачам отреагировать на сигнал до `close(true)`, мс. По умолчанию 1 000. */
  readonly abortGraceMs?: number;
  /**
   * На сколько отложить задачу, взятую уже после начала выключения, мс (по
   * умолчанию 1 000): отложенная «на сейчас», она вернулась бы в wait сразу, и
   * этот же Worker, ещё не вставший на паузу, брал бы её по кругу.
   */
  readonly handBackDelayMs?: number;
}
