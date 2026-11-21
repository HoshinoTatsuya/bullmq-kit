/** Журнал уборки: совместим с `Logger` Nest и консолью. */
export interface CleanupLogger {
  log(message: string): void;
  warn(message: string): void;
  debug?(message: string): void;
}
