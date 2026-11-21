import { INSPECT_STATUSES } from './inspect-statuses.constant';
import type { QueueHealth } from './queue-health.type';

/**
 * Снимок инспектора — текстом экспозиции Prometheus (без зависимости от
 * клиента метрик): отдать из `/metrics` как есть или переложить в свой реестр.
 * Неизвестное (`null`) не выводится: «ноль» и «не мерили» — разные ответы.
 */
export function toPrometheus(health: readonly QueueHealth[], metricPrefix = 'bullmq_queue'): string {
  // Значение метки в формате экспозиции: `\`, `"` и перевод строки экранируются.
  const label = (value: string): string =>
    value.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');
  const lines: string[] = [];
  const metric = (name: string, help: string, type: 'gauge' | 'counter', rows: Array<[string, number]>) => {
    if (rows.length === 0) {
      return;
    }
    lines.push(`# HELP ${metricPrefix}_${name} ${help}`, `# TYPE ${metricPrefix}_${name} ${type}`);
    for (const [labels, value] of rows) {
      lines.push(`${metricPrefix}_${name}{${labels}} ${value}`);
    }
  };
  const queue = (item: QueueHealth) => `queue="${label(item.prefix)}"`;
  metric(
    'jobs',
    'Задач в индексе статуса.',
    'gauge',
    health.flatMap((item) =>
      INSPECT_STATUSES.map((status): [string, number] => [
        `${queue(item)},state="${status}"`,
        item.counts[status],
      ]),
    ),
  );
  metric(
    'paused',
    'Очередь на паузе (1) или нет (0).',
    'gauge',
    health.map((item) => [queue(item), item.paused ? 1 : 0]),
  );
  metric(
    'unexpected_type_keys',
    'Индексов статусов не своего типа (не прочитаны; их счёт — не «пусто»).',
    'gauge',
    health.map((item) => [queue(item), item.unexpectedTypes.length]),
  );
  metric(
    'stuck_active',
    'Активных задач без лока (лок истёк).',
    'gauge',
    health.map((item) => [queue(item), item.stuckActive.count]),
  );
  metric(
    'oldest_wait_age_seconds',
    'Возраст самой старой ждущей задачи.',
    'gauge',
    health.flatMap((item) =>
      item.oldestWaitAgeMs === null ? [] : [[queue(item), item.oldestWaitAgeMs / 1000]],
    ),
  );
  metric(
    'delayed_overdue_seconds',
    'Насколько самая ранняя отложенная задача опоздала проснуться.',
    'gauge',
    health.flatMap((item) =>
      item.delayedOverdueMs === null ? [] : [[queue(item), item.delayedOverdueMs / 1000]],
    ),
  );
  metric(
    'orphans',
    'Сирот (уборка в dryRun).',
    'gauge',
    health.flatMap((item) => (item.orphans === null ? [] : [[queue(item), item.orphans]])),
  );
  return lines.length === 0 ? '' : `${lines.join('\n')}\n`;
}
