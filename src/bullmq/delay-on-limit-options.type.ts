/** Настройки `acquireOrDelay`; по умолчанию — `DELAY_ON_LIMIT_DEFAULTS`. */
export interface DelayOnLimitOptions {
  readonly minDelayMs?: number;
  readonly maxDelayMs?: number;
  readonly closedGroupDelayMs?: number;
  readonly jitterMs?: number;
  /**
   * Доля задержки в разбросе добавки: добавка — `[0, max(jitterMs, задержка · доля))`,
   * число от 0 до 1. По умолчанию `DELAY_ON_LIMIT_DEFAULTS.JITTER_FRACTION`;
   * `jitterMs: 0` и `jitterFraction: 0` — без добавки.
   */
  readonly jitterFraction?: number;
  /** Часы для отметки пробуждения (`moveToDelayed` ждёт абсолютное время). */
  readonly now?: () => number;
  /** Источник случайности для добавки, `[0, 1)`. */
  readonly random?: () => number;
  /**
   * Потолок суммарного ожидания с создания задачи (`job.timestamp`), мс. Дальше
   * — `UnrecoverableError`: задача падает с причиной, а не крутится через
   * delayed бесконечно (закрытая группа `limit: 0` не освободится никогда).
   * Считается от СОЗДАНИЯ, то есть включает намеренную задержку `delay` задачи:
   * задача с `delay: 10 мин` и потолком 5 мин упадёт на первой же отсрочке.
   * По умолчанию не задан.
   */
  readonly maxTotalWaitMs?: number;
}

/**
 * Часть задачи BullMQ, которая нужна помощнику: структурно, а не `Job` из
 * bullmq, — у проекта может стоять своя копия bullmq, и типы классов разных
 * копий несовместимы.
 */
export interface DelayableJob {
  moveToDelayed(timestamp: number, token?: string): Promise<void>;
  /** Момент создания задачи (BullMQ `job.timestamp`), мс — для `maxTotalWaitMs`. */
  readonly timestamp?: number;
  /**
   * Сколько раз задача стартовала и сколько попыток провалила (BullMQ
   * `attemptsStarted`, `attemptsMade`): их разность минус один — число уже
   * сделанных отсрочек, показатель экспоненциальной задержки. Нет полей —
   * задержка первой отсрочки.
   */
  readonly attemptsStarted?: number;
  readonly attemptsMade?: number;
}
