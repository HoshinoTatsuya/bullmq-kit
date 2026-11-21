import { deepFreeze } from '../deep-freeze';
import type { INSPECT_STATUSES } from './inspect-statuses.constant';

/** Тип Redis каждого индекса `INSPECT_STATUSES` (в том же порядке): ключ другого типа не читается. */
export const INSPECT_STATUS_TYPES: Readonly<Record<(typeof INSPECT_STATUSES)[number], 'list' | 'zset'>> = {
  wait: 'list',
  active: 'list',
  paused: 'list',
  delayed: 'zset',
  prioritized: 'zset',
  'waiting-children': 'zset',
  completed: 'zset',
  failed: 'zset',
};

deepFreeze(INSPECT_STATUS_TYPES);
