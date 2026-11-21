import { deepFreeze } from '../deep-freeze';

/**
 * Индексы, которые считает инспектор, в порядке KEYS скрипта: первые четыре
 * скрипт читает по месту (`wait` — хвост, `active` — локи, `delayed` — первое
 * очко), поэтому порядок — часть контракта скрипта.
 */
export const INSPECT_STATUSES = [
  'wait',
  'active',
  'paused',
  'delayed',
  'prioritized',
  'waiting-children',
  'completed',
  'failed',
] as const;

deepFreeze(INSPECT_STATUSES);
