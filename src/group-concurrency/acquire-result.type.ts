import type { AcquireOutcome } from './acquire-outcome.type';

/**
 * Исход захвата с подсказкой, когда повторять. `retryAfterMs` — только при
 * `limit`: через сколько мс слот освободится ПО СРОКУ держателей, если никто не
 * снимет свой раньше (время сервера Redis). Это верхняя граница ожидания, а не
 * точный момент: держатель, закончивший работу, снимает слот раньше срока.
 * `null` — подсказки нет: исход не `limit`, либо группа закрыта (`limit: 0`) и
 * по сроку не освободится никогда.
 */
export interface AcquireResult {
  readonly outcome: AcquireOutcome;
  readonly retryAfterMs: number | null;
  /**
   * Подсказка ТОЧНАЯ (темп `GroupRateLimit`: токен будет ровно через столько), а
   * не худший случай: `acquireOrDelay` откладывает на неё, а не по экспоненте.
   */
  readonly exact?: boolean;
}

/** Группа и её потолок для захвата нескольких групп сразу. */
export interface GroupLimit {
  readonly group: string;
  /** Целое ≥ 0 или `Infinity`, как у `acquire`. */
  readonly limit: number;
}

/**
 * Исход захвата нескольких групп (всё или ничего). `outcome`: `limit` — хоть одна
 * группа упёрлась, не занято НИЧЕГО; `acquired` — занят хоть один новый слот;
 * `held` — держатель уже держал слоты во всех группах (продлены).
 * `retryAfterMs` при `limit` — максимум по упёршимся группам (освободиться
 * должны все), `null` — среди них есть закрытая. `blockedBy` — упёршиеся группы.
 */
export interface AcquireManyResult extends AcquireResult {
  readonly blockedBy: readonly string[];
}

/** Живой держатель группы и остаток его срока по времени сервера Redis. */
export interface GroupHolder {
  readonly holder: string;
  readonly expiresInMs: number;
}
