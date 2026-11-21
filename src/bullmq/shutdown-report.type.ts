/** Итог выключения Worker. */
export interface ShutdownReport {
  /** Все начатые задачи завершились до дедлайна. */
  readonly drained: boolean;
  /**
   * id задач, прерванных дедлайном: их сигнал прерван, Worker закрыт без
   * ожидания. Лок такой задачи истечёт, и её заберёт другой воркер (stalled) —
   * работа будет сделана повторно, поэтому список и отдаётся.
   */
  readonly interrupted: readonly string[];
  /**
   * id задач, которые Worker взял уже после начала выключения (BullMQ не
   * отменяет начатого блокирующего ожидания): не выполнялись, отданы обратно в
   * очередь нетронутыми, попытка не потрачена.
   */
  readonly handedBack: readonly string[];
  readonly durationMs: number;
}
