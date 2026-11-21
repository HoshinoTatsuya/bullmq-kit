import { BULLMQ_KEYS } from './bullmq-keys.constant';

/**
 * Суффикс ключа-признака вложенной очереди внутри пространства убираемой:
 * `<P>:meta` или `<P>:id` (группа 1 — `P`). Подозрение, а не вывод: очередь
 * ли это, решает скрипт `nestedQueue` по типу и полям ключа.
 */
export const NESTED_MARKER = new RegExp(
  `^(.+):(?:${BULLMQ_KEYS.NESTED_QUEUE_META_SUFFIX}|${BULLMQ_KEYS.NESTED_QUEUE_ID_SUFFIX})$`,
);
