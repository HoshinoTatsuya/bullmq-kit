import { deepFreeze } from '../deep-freeze';

/** Настройки очереди мёртвых. */
export const DEAD_LETTER = {
  /**
   * Opts исходной задачи, которые переносятся в повтор. Остальные — нет:
   * `repeat` завёл бы расписание заново, `delay` отложил бы повтор на исходную
   * задержку, `jobId`/`parent`/`deduplication` ставятся повтором явно или
   * теряют смысл вне исходного дерева.
   */
  RETRY_OPTS: ['attempts', 'backoff', 'priority', 'removeOnComplete', 'removeOnFail', 'lifo'],
} as const;

deepFreeze(DEAD_LETTER);
