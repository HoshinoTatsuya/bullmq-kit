import { copyToDeadLetter } from './copy-to-dead-letter';
import type { DeadLetterQueue } from './dead-letter-queue.type';
import type { FailedJob } from './failed-job.type';
import type { FailedJobEmitter } from './failed-job-emitter.type';

/**
 * Очередь мёртвых: задача, провалившаяся ОКОНЧАТЕЛЬНО (попытки кончились или
 * `UnrecoverableError`), копируется в отдельную очередь с причиной, стеком и
 * исходными данными. Иначе failed копится и удаляется политикой хранения
 * (`removeOnFail`) вместе с причиной — разбирать нечего.
 *
 * Окончательность — по состоянию задачи (`failed`), а не по событию: событие
 * `failed` Worker шлёт на КАЖДУЮ проваленную попытку, и повторяемая задача
 * оказалась бы в очереди мёртвых живой. id копии детерминирован (очередь, id,
 * момент провала): повторная доставка события не плодит копий.
 *
 * Копия — по событию процесса: процесс, упавший между провалом и копией,
 * копию не сделает. Это помощник разбора, а не гарантия доставки.
 *
 * Возвращает отписку. Отказ записи — в `onError` (иначе — молча: событие
 * Worker'а не место для исключения, оно уронило бы процесс).
 */
export function attachDeadLetter(
  worker: FailedJobEmitter,
  deadLetters: DeadLetterQueue,
  options: { readonly onError?: (error: unknown, job: FailedJob) => void } = {},
): () => void {
  const listener = (job: FailedJob | undefined, error: Error): void => {
    if (job === undefined || job.id === undefined) {
      return;
    }
    void copyToDeadLetter(job, error, deadLetters).catch((failure: unknown) =>
      options.onError?.(failure, job),
    );
  };
  worker.on('failed', listener);
  return () => {
    worker.off('failed', listener);
  };
}
