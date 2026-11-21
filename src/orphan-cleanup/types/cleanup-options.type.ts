import type { CleanupLogger } from './cleanup-logger.type';
import type { CleanupScanner } from './cleanup-redis.type';

export interface OrphanedJobsCleanupOptions {
  /** Префиксы очередей вида `bull:<имя очереди>` — без завершающего двоеточия. */
  readonly queuePrefixes: readonly string[];
  /** Потолок проверенных хэшей задач за прогон (общий на все очереди). */
  readonly maxJobsPerRun?: number;
  /** Потолок ключей, которые вернул SCAN за прогон, включая хвосты задач. */
  readonly maxScannedKeysPerRun?: number;
  /** Потолок длительности прогона, мс (проверяется между батчами SCAN и удаления). */
  readonly maxRunMs?: number;
  readonly scanCount?: number;
  readonly batchSize?: number;
  readonly snapshotPage?: number;
  /** Sorted set больше этого в снимок не читается — его задачи проверяются поштучно. */
  readonly maxSnapshotZsetSize?: number;
  /**
   * Потолок элементов списка индекса, которые LPOS просматривает на кандидата
   * (целое > 0 или Infinity; по умолчанию `CLEANUP_DEFAULTS.MAX_LIST_SCAN` —
   * миллион, с 0.9.0; «без потолка» — только явным Infinity). Список
   * длиннее потолка, в начале которого id нет, — «не смогли проверить»: хэш
   * остаётся (`kept.unverifiable`). Ограничивает блокировку Redis проходом
   * огромного `wait` ценой того, что сироты такой очереди ждут, пока список
   * не сократится.
   */
  readonly maxListScan?: number;
  /**
   * Потолок длины списка статуса (wait/active/paused), который читается в
   * снимок (целое > 0 или Infinity). Список
   * длиннее — очередь в этом прогоне НЕ убирается (`unsnapshottedIndexes`):
   * без снимка её задачи стали бы кандидатами, а удалять, не сверив, нельзя.
   * Ночной батч на миллионы задач в `wait` иначе занимал бы сотни МБ памяти.
   * По умолчанию — `CLEANUP_DEFAULTS.MAX_SNAPSHOT_LIST_SIZE` (миллион); `Infinity` —
   * без потолка.
   */
  readonly maxSnapshotListSize?: number;
  /**
   * Потолок проходов LPOS на пайплайн удаления: батч кандидатов уменьшается
   * до `потолок / длина списков статусов` (не меньше одного). По умолчанию
   * `CLEANUP_DEFAULTS.MAX_LIST_WORK_PER_BATCH`.
   */
  readonly maxListWorkPerBatch?: number;
  readonly highOrphanShare?: number;
  /** Хэш моложе (по полю `timestamp`, против времени сервера Redis) не удаляется. */
  readonly graceMs?: number;
  /**
   * Удалять хэши без `timestamp` (по второму наблюдению не раньше грейса). По
   * умолчанию — нет: так выглядит и поздняя запись в удалённую задачу, и
   * служебный хэш версии BullMQ, которой уборка не знает; первое BullMQ
   * сама почти не допускает (поздние записи проверяют `EXISTS`), второе ломает
   * очередь. Счёт — `kept.untimestamped` в любом случае.
   */
  readonly deleteUntimestamped?: boolean;
  /** Ничего не удалять: отчёт о том, что было бы удалено (`orphans`, `orphanKeys`). */
  readonly dryRun?: boolean;
  /** Сколько ключей сирот вернуть в отчёте (`orphanKeys`). */
  readonly reportKeysLimit?: number;
  /** Журнал: строка на каждую сироту (debug), итог (log), усечения и отказы проверки (warn). */
  readonly logger?: CleanupLogger;
  /**
   * Клиенты для SCAN. По умолчанию — основной клиент. В Redis Cluster SCAN
   * обходит ОДИН узел: клиент кластера отправил бы его на случайный и не нашёл
   * бы ключей очереди на остальных. Передайте мастера: функцией
   * (`() => cluster.nodes('master')`) — она зовётся в начале каждого прогона, и
   * после failover обход идёт по новым мастерам; списком — узлы на всё время
   * жизни уборки. Проверка и удаление — через основной клиент. Пустой список —
   * отказ (в конструкторе или в прогоне).
   */
  readonly scanClients?:
    | readonly CleanupScanner[]
    | (() => readonly CleanupScanner[] | Promise<readonly CleanupScanner[]>);
  /**
   * Ключ лока прогона (`SET NX PX`). Несколько реплик с одним расписанием иначе
   * запускают уборку одновременно — та же работа N раз и N-кратная нагрузка на
   * Redis. Занят — прогон не делается (`lockedOut: true`). Срок — `maxRunMs` плюс
   * запас; снимается только свой.
   */
  readonly runLockKey?: string;
  readonly now?: () => number;
}
