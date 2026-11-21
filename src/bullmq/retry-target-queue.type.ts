/** Исходная очередь для повтора из очереди мёртвых. */
export interface RetryTargetQueue {
  readonly name: string;
  getJob(
    id: string,
  ): Promise<{ getState(): Promise<string>; retry(state?: 'failed'): Promise<void> } | undefined>;
  add(name: string, data: unknown, opts: Record<string, unknown>): Promise<unknown>;
}
