import type { CLEANUP_STOP_REASON } from '../constants/orphan-verdict.constant';

/** Очередь, не убранная из-за версии BullMQ, вложенной очереди или нечитаемого meta. */
export interface UnsupportedQueue {
  readonly prefix: string;
  /** Значение `meta.version`, `null` — поля нет или meta не читается. */
  readonly version: string | null;
  readonly reason: string;
}

/** Почему не удалили хэш, найденный кандидатом в сироты. */
export interface CleanupKept {
  /** Задача оказалась в индексе к моменту удаления (переехала во время прогона). */
  readonly alive: number;
  /** Вне индексов, но с живым `:lock`: задачу держит воркер. */
  readonly locked: number;
  /** Моложе грейса. */
  readonly young: number;
  /** `timestamp` не число. */
  readonly unknownAge: number;
  /** Ключ исчез или не хэш. */
  readonly notHash: number;
  /** Индекс неожиданного типа или список длиннее `maxListScan`. */
  readonly unverifiable: number;
  /**
   * Хэш без `timestamp`: по умолчанию не удаляется никогда (так выглядит и
   * служебный хэш незнакомой версии BullMQ); с `deleteUntimestamped` —
   * удаляется на повторном наблюдении не раньше грейса.
   */
  readonly untimestamped: number;
  /** Хэш с `timestamp`, но без полей задачи (`BULLMQ_KEYS.JOB_SHAPE_FIELDS`): не хэш задачи. */
  readonly notJob: number;
  /** Ключ другой очереди BullMQ, чей префикс вложен в префикс убираемой. */
  readonly nested: number;
  /** Ошибка команды проверки или незнакомый ответ: «не смогли проверить» — не удаляли. */
  readonly errors: number;
}

export interface CleanupReport {
  /** Проверено хэшей задач. */
  readonly scanned: number;
  /** Ключей, которые вернул SCAN (хэши задач и их хвосты) — к потолку `maxScannedKeysPerRun`. */
  readonly scannedKeys: number;
  /** Удалено хэшей (вместе с хвостами). В dryRun — 0. */
  readonly deleted: number;
  readonly durationMs: number;
  /** Доля сирот (`orphans / scanned`) выше порога — повод искать причину рассинхрона. */
  readonly highOrphanShare: boolean;
  /** Найдено сирот: удалено, а в dryRun — удалили бы. */
  readonly orphans: number;
  /** Ключи сирот — выборка до `reportKeysLimit`, а не весь список. */
  readonly orphanKeys: readonly string[];
  /**
   * Ключи по причинам «не удалили» — выборка до `reportKeysLimit` на причину.
   * Вместе с `dryRun` — режим отчёта для первого включения на незнакомой
   * инсталляции: что удалилось бы (`orphanKeys`) и что и почему осталось.
   */
  readonly keptKeys: { readonly [K in keyof CleanupKept]: readonly string[] };
  readonly dryRun: boolean;
  readonly kept: CleanupKept;
  /** Обход остановлен потолком, и не всё просмотрено; `null` — дошёл до конца. */
  readonly truncatedBy: (typeof CLEANUP_STOP_REASON)[keyof typeof CLEANUP_STOP_REASON] | null;
  /**
   * Индексы `<prefix>:<status>` больше потолка снимка: sorted set — его задачи
   * проверены поштучно; список — очередь в этом прогоне не убиралась.
   */
  readonly unsnapshottedIndexes: readonly string[];
  /**
   * Очереди, не убранные целиком: `meta.version` вне проверенного диапазона,
   * нет или не разбирается, meta не читается, внутри пространства — другая
   * очередь BullMQ. Пустой список — все очереди убирались.
   */
  readonly unsupportedQueues: readonly UnsupportedQueue[];
  /** Лок прогона занят другим прогоном — уборка не делалась. */
  readonly lockedOut: boolean;
}
