/**
 * Часть Worker BullMQ, которая нужна выключению: структурно, а не `Worker` из
 * bullmq — у проекта может стоять своя копия bullmq.
 */
export interface ShutdownableWorker {
  /** `pause(true)` — перестать брать новые задачи, не дожидаясь идущих. */
  pause(doNotWaitActive?: boolean): Promise<void>;
  /** `close(true)` — закрыть, не дожидаясь идущих: их локи истекут, и задачи заберёт другой воркер. */
  close(force?: boolean): Promise<void>;
}
