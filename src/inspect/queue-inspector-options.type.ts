import type { OrphanedJobsCleanupOptions } from '../orphan-cleanup/types/cleanup-options.type';

/** Настройки инспектора очередей. */
export interface QueueInspectorOptions {
  /** Префиксы очередей `bull:<имя>` — как у уборки. */
  readonly queuePrefixes: readonly string[];
  /** Сколько первых id `active` проверять на лок, по умолчанию 1 000. */
  readonly maxActiveScan?: number;
  /**
   * Считать сирот: уборка в dryRun по каждой очереди (SCAN всего пространства —
   * дорого; включать реже, чем остальной снимок). Настройки уборки — кроме
   * `queuePrefixes` и `dryRun`.
   */
  readonly orphans?: boolean | Omit<OrphanedJobsCleanupOptions, 'queuePrefixes' | 'dryRun'>;
}
