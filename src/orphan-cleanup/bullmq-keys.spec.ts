import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

import { QueueKeys } from 'bullmq';

import { BULLMQ_KEYS } from './constants/bullmq-keys.constant';

/**
 * Словарь ключей уборки против `QueueKeys` УСТАНОВЛЕННОЙ BullMQ: в прогоне
 * `test:oldest` `bullmq` подменён нижней границей, в `test:latest` — последняя.
 * На `QueueKeys` построен и `Queue.removeOrphanedJobs` самой BullMQ (5.72+):
 * ключ, которого нет в словаре, уборка сочла бы хэшем задачи.
 */
describe('BULLMQ_KEYS против QueueKeys установленной BullMQ', () => {
  const keys = Object.keys(new QueueKeys('bull').getKeys('q')).filter((key) => key !== '');

  it('выборка не выродилась', () => {
    expect(keys.length).toBeGreaterThanOrEqual(15);
  });

  it('каждый ключ очереди BullMQ — служебный в словаре', () => {
    expect(keys.filter((key) => !BULLMQ_KEYS.INTERNAL.includes(key as never))).toEqual([]);
  });

  it('служебное словаря сверх QueueKeys — только пространство metrics:* (в QueueKeys его нет)', () => {
    expect(BULLMQ_KEYS.INTERNAL.filter((key) => !keys.includes(key))).toEqual(['metrics']);
  });

  it('каждый индекс статуса словаря — ключ BullMQ', () => {
    const statuses = [
      ...BULLMQ_KEYS.ZSET_STATUSES,
      ...BULLMQ_KEYS.LIST_STATUSES,
      ...BULLMQ_KEYS.SET_STATUSES,
    ];
    expect(statuses.filter((status) => !keys.includes(status))).toEqual([]);
  });
});

/**
 * Пер-задачные ключи, которые уборка удаляет вместе с хэшем сироты, — ровно
 * набор `removeJobKeys.lua` УСТАНОВЛЕННОЙ BullMQ (её собственное удаление
 * задачи). Ключ сверх него — удалили бы то, чего BullMQ не удаляет; ключ,
 * которого в словаре нет, — остался бы хвостом удалённой сироты.
 */
describe('AUX_SUFFIXES_DELETED_WITH_JOB против removeJobKeys.lua установленной BullMQ', () => {
  const pkg = process.env.BULLMQ_UNDER_TEST ?? 'bullmq';
  const lua = readFileSync(
    join(dirname(require.resolve(`${pkg}/package.json`)), 'dist/cjs/commands/includes/removeJobKeys.lua'),
    'utf8',
  );
  const suffixes = [...lua.matchAll(/jobKey \.\. ':([a-z-]+)'/g)].map((match) => match[1]);

  it('выборка не выродилась', () => {
    expect(suffixes.length).toBeGreaterThanOrEqual(5);
  });

  it('множество суффиксов совпадает', () => {
    expect([...BULLMQ_KEYS.AUX_SUFFIXES_DELETED_WITH_JOB].sort()).toEqual([...suffixes].sort());
  });
});
