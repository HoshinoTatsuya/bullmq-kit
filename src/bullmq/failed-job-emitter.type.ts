import type { FailedJob } from './failed-job.type';

/** Worker, на чьё событие `failed` вешается очередь мёртвых. */
export interface FailedJobEmitter {
  on(event: 'failed', listener: (job: FailedJob | undefined, error: Error) => void): unknown;
  off(event: 'failed', listener: (job: FailedJob | undefined, error: Error) => void): unknown;
}
