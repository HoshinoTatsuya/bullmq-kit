import type { INSPECT_STATUSES } from './inspect-statuses.constant';

/** Здоровье одной очереди на момент снимка (время сервера Redis). */
export interface QueueHealth {
  readonly prefix: string;
  readonly counts: { readonly [S in (typeof INSPECT_STATUSES)[number]]: number };
  /**
   * Очередь на паузе (`meta.paused`, его пишут BullMQ 5 и 6). На BullMQ 6 ждущие
   * приостановленной очереди остаются в `wait` (5 переносила их в `paused`), и
   * без этого признака пауза неотличима от очереди без воркеров: растёт
   * `oldestWaitAgeMs`.
   */
  readonly paused: boolean;
  /**
   * Индексы статусов, у которых в Redis не тот тип (не список/не sorted set):
   * чужая запись в пространстве очереди или ручная правка. Они не читаются, и
   * их счёт в `counts` — 0, который значит «не прочитано», а не «пусто».
   */
  readonly unexpectedTypes: readonly string[];
  /**
   * Активные задачи без `:lock`: лок истёк, воркер умер или завис. Живые
   * воркеры вернут их проверкой stalled; растущее число — воркеров нет или они
   * не успевают продлевать лок. Просмотр — `maxActiveScan` САМЫХ СТАРЫХ active
   * (хвост списка); `sample` — их id без лока, от старых к новым.
   */
  readonly stuckActive: {
    readonly count: number;
    readonly scanned: number;
    readonly sample: readonly string[];
  };
  /**
   * Возраст самой старой ждущей задачи (по `timestamp`), мс, — по `wait` и
   * `paused` (BullMQ 5 на паузе держит ждущих там); `null` — оба пусты.
   */
  readonly oldestWaitAgeMs: number | null;
  /**
   * На сколько мс самая ранняя отложенная задача опоздала проснуться; `0` —
   * не опоздала; `null` — отложенных нет. Растёт — некому переводить delayed
   * в wait (нет воркеров).
   */
  readonly delayedOverdueMs: number | null;
  /** Прирост failed с прошлого снимка этого инспектора; `null` — первый снимок. */
  readonly failedIncrease: number | null;
  /** Сирот (уборка в dryRun) — только с `orphans: true`, иначе `null`. */
  readonly orphans: number | null;
}
