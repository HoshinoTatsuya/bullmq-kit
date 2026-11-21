import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { BULLMQ_SUPPORTED_VERSIONS } from './constants/bullmq-supported-versions.constant';
import { unsupportedBullmqVersion } from './unsupported-bullmq-version';

describe('unsupportedBullmqVersion', () => {
  it.each([
    ['bullmq:5.72.0', null],
    ['bullmq:5.72.1', null],
    ['bullmq:5.80.0-beta.1', null],
    ['bullmq:5.81.0', null],
    ['bullmq:5.81.99', null],
    ['bullmq:5.81.3+build.7', null],
    ['bullmq:5.72.0-beta.1', 'младше проверенной'],
    ['bullmq:5.71.9', 'младше проверенной'],
    ['bullmq:5.9.99', 'младше проверенной'],
    ['bullmq:5.82.0', 'новее последнего проверенного минора'],
    ['bullmq:5.82.0-beta.1', 'новее последнего проверенного минора'],
    ['bullmq:5.100.0', 'новее последнего проверенного минора'],
    ['bullmq:6.0.0', null],
    ['bullmq:6.3.4', null],
    ['bullmq:6.7.7', null],
    ['bullmq:6.7.99', null],
    ['bullmq:6.0.0-beta.1', 'младше проверенной'],
    ['bullmq:6.8.0', 'новее последнего проверенного минора'],
    ['bullmq:6.8.0-beta.1', 'новее последнего проверенного минора'],
    ['bullmq:4.99.0', 'мажор'],
    ['bullmq:7.0.0', 'мажор'],
    ['bullmq:7.0.0-beta.1', 'мажор'],
    ['bullmq:5.x', 'не разбирается'],
    ['bullmq:5.81.3-', 'не разбирается'],
    ['5.81.3', 'не BullMQ'],
    ['', 'нет'],
    [null, 'нет'],
  ] as const)('%p → %p', (raw, reason) => {
    const verdict = unsupportedBullmqVersion(raw);
    if (reason === null) {
      expect(verdict).toBeNull();
    } else {
      expect(verdict).toContain(reason);
    }
  });

  const installed = (alias: string): number[] => {
    const { version } = JSON.parse(
      readFileSync(join(__dirname, '..', '..', 'node_modules', alias, 'package.json'), 'utf8'),
    ) as { version: string };
    return version.split('.').map(Number);
  };
  const rangeOf = (major: number) => {
    const range = BULLMQ_SUPPORTED_VERSIONS.RANGES.find((candidate) => candidate.MIN[0] === major);
    if (range === undefined) {
      throw new Error(`нет диапазона мажора ${major}`);
    }
    return range;
  };

  /**
   * Каждая граница каждого мажора — версия, против которой набор действительно
   * идёт (псевдонимы devDependencies и прогоны `test:*`): нижняя — целиком,
   * верхняя — минором (патчи внутри него принимаются). Подняли devDependency
   * (новый минор мог принести служебный ключ) — красный, пока граница не
   * поднята вслед за зелёным прогоном набора.
   */
  it.each([
    ['bullmq-oldest', 5, 'MIN'],
    ['bullmq-5', 5, 'MAX_MINOR'],
    ['bullmq-6-oldest', 6, 'MIN'],
    ['bullmq', 6, 'MAX_MINOR'],
  ] as const)('%s — граница %s.%s', (alias, major, bound) => {
    const version = installed(alias);
    expect(version[0]).toBe(major);
    const range = rangeOf(major);
    if (bound === 'MIN') {
      expect([...range.MIN]).toEqual(version);
    } else {
      expect([...range.MAX_MINOR]).toEqual(version.slice(0, 2));
    }
  });

  it('диапазонов ровно столько, сколько мажоров проверяет набор (новый мажор — только с прогонами на нём)', () => {
    expect(BULLMQ_SUPPORTED_VERSIONS.RANGES.map((range) => range.MIN[0])).toEqual([5, 6]);
    for (const range of BULLMQ_SUPPORTED_VERSIONS.RANGES) {
      expect(range.MAX_MINOR[0]).toBe(range.MIN[0]);
    }
  });
});
