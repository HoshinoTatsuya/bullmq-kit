import { createHash } from 'node:crypto';

import { DEAD_LETTER } from './dead-letter.constant';
import type { DeadLetterEntry } from './dead-letter-entry.type';
import type { DeadLetterQueue } from './dead-letter-queue.type';
import type { FailedJob } from './failed-job.type';

/** Копия задачи в очередь мёртвых — если провал окончательный (состояние `failed`). */
export async function copyToDeadLetter(
  job: FailedJob,
  error: Error,
  deadLetters: DeadLetterQueue,
): Promise<void> {
  if ((await job.getState()) !== 'failed') {
    return;
  }
  const failedAt = job.finishedOn ?? null;
  const entry: DeadLetterEntry = {
    queue: job.queueName,
    jobId: job.id as string,
    name: job.name,
    data: job.data,
    opts: Object.fromEntries(
      Object.entries(job.opts).filter(([key]) => (DEAD_LETTER.RETRY_OPTS as readonly string[]).includes(key)),
    ),
    failedReason: job.failedReason ?? error.message,
    stacktrace: job.stacktrace ?? [],
    attemptsMade: job.attemptsMade,
    failedAt,
  };
  // Детерминированный id без `:` (BullMQ запрещает его в произвольных id): повтор события — та же копия.
  const jobId = createHash('sha1')
    .update(`${entry.queue}\0${entry.jobId}\0${failedAt ?? ''}`)
    .digest('hex');
  await deadLetters.add(job.name, entry, { jobId });
}
