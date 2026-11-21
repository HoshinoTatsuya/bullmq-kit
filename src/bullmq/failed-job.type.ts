/** Провалившаяся задача BullMQ — ровно то, что нужно очереди мёртвых (структурно). */
export interface FailedJob {
  readonly id?: string | undefined;
  readonly name: string;
  readonly data: unknown;
  readonly opts: object;
  readonly queueName: string;
  readonly failedReason?: string;
  readonly stacktrace?: string[] | null;
  readonly attemptsMade: number;
  readonly finishedOn?: number | undefined;
  getState(): Promise<string>;
}
