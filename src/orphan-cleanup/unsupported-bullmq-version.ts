import { compareVersionParts as compare } from './compare-version-parts';
import { BULLMQ_SUPPORTED_VERSIONS as SUPPORTED } from './constants/bullmq-supported-versions.constant';
import { SEMVER_PATTERN as VERSION } from './constants/semver-pattern.constant';

/**
 * Почему очередь с этим значением `meta.version` убирать нельзя; `null` —
 * версия поддержана. Нет поля или оно не разбирается — тоже отказ: «не смогли
 * определить» не означает «знакомая раскладка».
 *
 * Предрелиз младше своего релиза (semver): `5.72.0-beta.1` — до 5.72.0, то есть
 * вне проверенного диапазона. Внутри диапазона предрелиз принимается.
 */
export function unsupportedBullmqVersion(raw: unknown): string | null {
  if (typeof raw !== 'string' || raw === '') {
    return 'версии BullMQ в meta очереди нет (очередь не создавалась new Queue или meta удалён)';
  }
  if (!raw.startsWith(SUPPORTED.META_PREFIX)) {
    return `meta.version «${raw}» — не BullMQ`;
  }
  const match = VERSION.exec(raw.slice(SUPPORTED.META_PREFIX.length));
  if (match === null) {
    return `meta.version «${raw}» не разбирается`;
  }
  const version = [Number(match[1]), Number(match[2]), Number(match[3])];
  const shown = raw.slice(SUPPORTED.META_PREFIX.length);
  const range = SUPPORTED.RANGES.find((candidate) => candidate.MIN[0] === version[0]);
  if (range === undefined) {
    const majors = SUPPORTED.RANGES.map((candidate) => candidate.MIN[0]).join(', ');
    return `BullMQ ${shown}: мажор ${version[0]} не проверен (проверены ${majors})`;
  }
  const toMin = compare(version, range.MIN);
  if (toMin < 0 || (toMin === 0 && match[4] !== undefined)) {
    return `BullMQ ${shown} младше проверенной ${range.MIN.join('.')}`;
  }
  if (compare(version.slice(0, 2), range.MAX_MINOR) > 0) {
    return `BullMQ ${shown} новее последнего проверенного минора ${range.MAX_MINOR.join('.')}: раскладку ключей уборка не знает`;
  }
  return null;
}
