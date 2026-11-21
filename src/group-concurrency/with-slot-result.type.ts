import type { ACQUIRE_OUTCOME } from './acquire-outcome.constant';

/** Итог `withSlot`: работа выполнена в слоте или потолок занят (работа не начиналась). */
export type WithSlotResult<T> =
  | {
      readonly outcome: typeof ACQUIRE_OUTCOME.acquired | typeof ACQUIRE_OUTCOME.held;
      readonly value: T;
    }
  | { readonly outcome: typeof ACQUIRE_OUTCOME.limit; readonly retryAfterMs: number | null };
