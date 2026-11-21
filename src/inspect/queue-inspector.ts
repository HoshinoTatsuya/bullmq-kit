import { clientKeyPrefix } from '../group-concurrency/client-key-prefix';
import { OrphanedJobsCleanup } from '../orphan-cleanup/orphaned-jobs-cleanup';
import type { OrphanedJobsCleanupOptions } from '../orphan-cleanup/types/cleanup-options.type';
import type { CleanupRedis } from '../orphan-cleanup/types/cleanup-redis.type';
import { INSPECT_SCRIPTS } from './inspect-scripts.constant';
import { INSPECT_STATUS_TYPES } from './inspect-status-types.constant';
import { INSPECT_STATUSES } from './inspect-statuses.constant';
import type { QueueHealth } from './queue-health.type';
import type { QueueInspectorOptions } from './queue-inspector-options.type';

/**
 * Инспектор очередей BullMQ: зависшие active (лок истёк), возраст ждущих,
 * просрочка отложенных, рост failed, сироты — отчётом и метриками вместо
 * разглядывания Redis глазами. Только чтение (сироты — уборкой в dryRun).
 *
 * Снимок очереди — один скрипт: значения согласованы между собой и с временем
 * сервера Redis. Прирост failed — между снимками ЭТОГО экземпляра (память
 * процесса): в метриках его лучше брать из счётчика `counts.failed` средствами
 * хранилища метрик, а `failedIncrease` — для журнала и алерта «на этом тике».
 */
export class QueueInspector {
  private readonly _previousFailed = new Map<string, number>();
  private readonly _maxActive: number;
  private readonly _cleanups = new Map<string, OrphanedJobsCleanup>();

  public constructor(
    private readonly _redis: CleanupRedis,
    private readonly _options: QueueInspectorOptions,
  ) {
    if (
      _options.queuePrefixes.length === 0 ||
      _options.queuePrefixes.some((prefix) => prefix.trim() === '')
    ) {
      throw new Error('QueueInspector: список префиксов очередей пуст или содержит пустой префикс');
    }
    if (clientKeyPrefix(_redis) !== '') {
      // Хэши задач и локи скрипт собирает сам — keyPrefix их не получит.
      throw new Error('QueueInspector: у клиента keyPrefix — ключи, собранные в скрипте, его не получат');
    }
    this._maxActive = _options.maxActiveScan ?? 1_000;
    if (!Number.isInteger(this._maxActive) || this._maxActive <= 0) {
      throw new Error(`QueueInspector: maxActiveScan — целое больше нуля, получено ${this._maxActive}`);
    }
    if (_options.orphans !== undefined && _options.orphans !== false) {
      const extra: Omit<OrphanedJobsCleanupOptions, 'queuePrefixes' | 'dryRun'> =
        _options.orphans === true ? {} : _options.orphans;
      for (const prefix of _options.queuePrefixes) {
        this._cleanups.set(
          prefix,
          new OrphanedJobsCleanup(_redis, { ...extra, queuePrefixes: [prefix], dryRun: true }),
        );
      }
    }
  }

  public async inspect(): Promise<QueueHealth[]> {
    const report: QueueHealth[] = [];
    for (const prefix of this._options.queuePrefixes) {
      report.push(await this._inspectQueue(prefix));
    }
    return report;
  }

  private async _inspectQueue(prefix: string): Promise<QueueHealth> {
    const reply = await this._redis.eval(
      INSPECT_SCRIPTS.queueHealth,
      INSPECT_STATUSES.length,
      ...INSPECT_STATUSES.map((status) => `${prefix}:${status}`),
      `${prefix}:`,
      this._maxActive,
      ...INSPECT_STATUSES.map((status) => INSPECT_STATUS_TYPES[status]),
    );
    const values = Array.isArray(reply) ? reply : [];
    const fixed = 1 + INSPECT_STATUSES.length + 5;
    const unexpectedCount = Number(values[fixed - 1]);
    if (
      values.length < fixed ||
      !Number.isInteger(unexpectedCount) ||
      values.length < fixed + unexpectedCount
    ) {
      throw new Error(`QueueInspector: неожиданный ответ скрипта снимка: ${String(reply)}`);
    }
    const now = Number(values[0]);
    const counts = Object.fromEntries(
      INSPECT_STATUSES.map((status, index) => [status, Number(values[1 + index])]),
    ) as QueueHealth['counts'];
    const oldest = Number(values[1 + INSPECT_STATUSES.length]);
    const wakeAt = Number(values[2 + INSPECT_STATUSES.length]);
    const paused = Number(values[3 + INSPECT_STATUSES.length]) === 1;
    const scanned = Number(values[4 + INSPECT_STATUSES.length]);
    const unexpectedTypes = values.slice(fixed, fixed + unexpectedCount).map(String);
    const stuck = values.slice(fixed + unexpectedCount).map(String);
    const previous = this._previousFailed.get(prefix);
    this._previousFailed.set(prefix, counts.failed);
    const cleanup = this._cleanups.get(prefix);
    return {
      prefix,
      counts,
      paused,
      unexpectedTypes,
      stuckActive: { count: stuck.length, scanned, sample: stuck },
      oldestWaitAgeMs: oldest < 0 ? null : Math.max(0, now - oldest),
      delayedOverdueMs: wakeAt < 0 ? null : Math.max(0, now - wakeAt),
      failedIncrease: previous === undefined ? null : counts.failed - previous,
      orphans: cleanup === undefined ? null : (await cleanup.run()).orphans,
    };
  }
}
