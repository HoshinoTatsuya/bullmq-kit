import type { DeadLetterEntry } from './dead-letter-entry.type';
import type { RetryTargetQueue } from './retry-target-queue.type';

/**
 * Повторить задачу из очереди мёртвых в её исходной очереди. Исходная задача
 * ещё лежит в failed — её `retry()` (тот же id, история попыток); удалена
 * политикой хранения — добавляется заново с тем же id и безопасными opts.
 * Добавить заново, пока исходная лежит в failed, нельзя: BullMQ вернул бы
 * существующую задачу и ничего не поставил. Копия в очереди мёртвых снимается.
 */
export async function retryDeadLetter(
  entry: { readonly data: DeadLetterEntry; remove(): Promise<void> },
  target: RetryTargetQueue,
): Promise<'retried' | 'readded'> {
  if (entry.data.queue !== target.name) {
    throw new Error(
      `retryDeadLetter: задача из очереди «${entry.data.queue}», а повтор — в «${target.name}»`,
    );
  }
  const original = await target.getJob(entry.data.jobId);
  let outcome: 'retried' | 'readded';
  if (original !== undefined && (await original.getState()) === 'failed') {
    await original.retry('failed');
    outcome = 'retried';
  } else if (original !== undefined) {
    throw new Error(
      `retryDeadLetter: задача ${entry.data.jobId} в «${target.name}» есть и не в failed — повтор задвоил бы её`,
    );
  } else {
    await target.add(entry.data.name, entry.data.data, { ...entry.data.opts, jobId: entry.data.jobId });
    outcome = 'readded';
  }
  await entry.remove();
  return outcome;
}
