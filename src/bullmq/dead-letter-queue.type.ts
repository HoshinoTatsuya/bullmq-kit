import type { DeadLetterEntry } from './dead-letter-entry.type';

/** Очередь мёртвых: только добавление. */
export interface DeadLetterQueue {
  add(name: string, data: DeadLetterEntry, opts: { jobId: string }): Promise<unknown>;
}
