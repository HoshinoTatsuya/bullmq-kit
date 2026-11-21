/**
 * Задача — то, что нужно учёту выключения: id и (если есть) возврат в очередь
 * `moveToDelayed`, которым задача, взятая уже после начала выключения,
 * отдаётся обратно нетронутой.
 */
export type TrackedJob = {
  readonly id?: string | undefined;
  moveToDelayed?(timestamp: number, token?: string): Promise<void>;
};
