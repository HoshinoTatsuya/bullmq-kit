import type { ACQUIRE_OUTCOME } from './acquire-outcome.constant';

/**
 * Исход захвата слота группы. Три ответа, а не `boolean`: «уже держит» и «занял
 * новый» различаются в метриках, а «потолок» — единственный отказ (ошибка
 * Redis — исключение, а не `false`: «не смогли спросить» и «нельзя» —
 * разные ответы, и политику отказа выбирает вызывающий).
 */
export type AcquireOutcome = (typeof ACQUIRE_OUTCOME)[keyof typeof ACQUIRE_OUTCOME];
