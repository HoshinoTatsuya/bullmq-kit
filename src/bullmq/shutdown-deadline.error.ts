/** Причина сигнала задачи, прерванной дедлайном выключения Worker. */
export class ShutdownDeadlineError extends Error {
  public constructor(deadlineMs: number) {
    super(`выключение Worker: задача не завершилась за ${deadlineMs} мс — прервана`);
    this.name = ShutdownDeadlineError.name;
  }
}
