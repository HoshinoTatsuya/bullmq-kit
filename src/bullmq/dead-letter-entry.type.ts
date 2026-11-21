/** Данные задачи очереди мёртвых: исходная задача и почему она умерла. */
export interface DeadLetterEntry {
  /** Имя исходной очереди. */
  readonly queue: string;
  readonly jobId: string;
  readonly name: string;
  readonly data: unknown;
  /** Безопасное подмножество исходных opts для повтора (`DEAD_LETTER.RETRY_OPTS`). */
  readonly opts: Readonly<Record<string, unknown>>;
  readonly failedReason: string;
  readonly stacktrace: readonly string[];
  readonly attemptsMade: number;
  /** Момент окончательного провала (BullMQ `finishedOn`), мс; `null` — не записан. */
  readonly failedAt: number | null;
}
