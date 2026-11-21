import { deepFreeze } from '../../deep-freeze';
import type { CleanupKept } from '../types/cleanup-report.type';
import { ORPHAN_VERDICT as V } from './orphan-verdict.constant';

/**
 * Причина «не удалили» по ответу скрипта проверки — один словарь и для счёта
 * (`kept`), и для выборки ключей (`keptKeys`). Ответ вне словаря — `errors`:
 * «не смогли определить» не значит ни «удалено», ни «жива».
 */
export const KEPT_BY_VERDICT: Readonly<Record<number, keyof CleanupKept>> = {
  [V.alive]: 'alive',
  [V.locked]: 'locked',
  [V.young]: 'young',
  [V.unknownAge]: 'unknownAge',
  [V.notHash]: 'notHash',
  [V.unverifiable]: 'unverifiable',
  [V.untimestamped]: 'untimestamped',
  [V.notJob]: 'notJob',
  [V.nested]: 'nested',
};

deepFreeze(KEPT_BY_VERDICT);
