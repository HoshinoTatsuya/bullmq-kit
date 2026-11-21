import { DelayedError, UnrecoverableError } from 'bullmq';

import { ACQUIRE_OUTCOME } from '../group-concurrency/acquire-outcome.constant';
import type { AcquireOutcome } from '../group-concurrency/acquire-outcome.type';
import type { AcquireResult } from '../group-concurrency/acquire-result.type';
import { DELAY_ON_LIMIT_DEFAULTS } from './delay-on-limit.constant';
import type { DelayableJob, DelayOnLimitOptions } from './delay-on-limit-options.type';

/**
 * Захват слота в процессоре Worker BullMQ: при `limit` задача уходит в delayed
 * — не в failed и без траты попытки (`moveToDelayed` + `DelayedError`: Worker
 * признаёт эту ошибку штатной и попыток не считает).
 *
 * Задержка — экспоненциальная от `minDelayMs` по числу уже сделанных отсрочек
 * этой задачи (`attemptsStarted − attemptsMade − 1`), с потолком `maxDelayMs` и
 * подсказкой `retryAfterMs`. Подсказка — ХУДШИЙ случай («освободится, если
 * все держатели перестанут продлевать»): с продлением (`withSlot`) она почти
 * всегда равна полному сроку слота, а слоты освобождаются раньше. Прежде
 * задержка равнялась подсказке, и каждая упёршаяся задача ждала ~срок слота
 * (замер: подсказка p50 30 с при фактическом ожидании p50 0.56 с).
 *
 * Каждая отсрочка — новый старт задачи: `attemptsStarted` растёт, и
 * `maxStartedAttempts` Worker'а считает отсрочки наравне со стартами. Ставьте его
 * с запасом на них или не ставьте.
 *
 * ```ts
 * new Worker(name, async (job, token) => {
 *   await acquireOrDelay(job, token, () => slots.acquireDetailed(group, job.id as string, slot));
 *   try { ... } finally { await slots.release(group, job.id as string); }
 * });
 * ```
 *
 * `token` — второй аргумент процессора: по нему BullMQ проверяет, что задачу
 * всё ещё держит этот воркер. Отказ `moveToDelayed` (лок потерян) пробрасывается
 * как есть — это не отказ потолка.
 *
 * Принимает любой захват с подсказкой: `acquireDetailed`, `reserveDetailed`,
 * `acquireMany`. Возвращает исход успеха (`acquired` или `held`).
 */
export async function acquireOrDelay(
  job: DelayableJob,
  token: string | undefined,
  acquire: () => Promise<AcquireResult>,
  options: DelayOnLimitOptions = {},
): Promise<AcquireOutcome> {
  if (typeof token !== 'string' || token === '') {
    // Без токена BullMQ подставит '0' и moveToDelayed пройдёт без проверки лока:
    // задачу, которую уже взял другой воркер, этот отложил бы повторно.
    throw new Error('acquireOrDelay: token пуст — передайте второй аргумент процессора Worker');
  }
  const delay = delayResolver(options);
  // Та же зажимка и добавка, но с показателем, при котором экспонента не меньше подсказки.
  const exactDelay = (hint: number) => delay(hint, DELAY_ON_LIMIT_DEFAULTS.MAX_BACKOFF_EXPONENT);
  const maxWait = options.maxTotalWaitMs;
  if (maxWait !== undefined && (!Number.isInteger(maxWait) || maxWait <= 0)) {
    throw new Error(`maxTotalWaitMs — целое больше нуля, получено ${maxWait}`);
  }
  const result = await acquire();
  if (result.outcome !== ACQUIRE_OUTCOME.limit) {
    return result.outcome;
  }
  const now = (options.now ?? Date.now)();
  if (maxWait !== undefined) {
    if (job.timestamp === undefined) {
      // «Не знаем, сколько ждёт» — не повод ждать вечно и не повод падать молча.
      throw new Error('maxTotalWaitMs задан, а у задачи нет timestamp — суммарное ожидание не посчитать');
    }
    if (now - job.timestamp >= maxWait) {
      throw new UnrecoverableError(
        `слот не освободился за ${maxWait} мс с создания задачи — потолок ожидания (maxTotalWaitMs)`,
      );
    }
  }
  const delaysSoFar = Math.max(0, (job.attemptsStarted ?? 1) - (job.attemptsMade ?? 0) - 1);
  // Точная подсказка (темп) — ждать ровно её: экспонента разбудила бы раньше, чем токен появится.
  const wait =
    result.exact === true && result.retryAfterMs !== null
      ? exactDelay(result.retryAfterMs)
      : delay(result.retryAfterMs, delaysSoFar);
  await job.moveToDelayed(now + wait, token);
  throw new DelayedError();
}

/**
 * Задержка `n`-й отсрочки: `minDelayMs · 2ⁿ`, не больше потолка — меньшего из
 * `maxDelayMs` и подсказки (или задержки закрытой группы), не меньше
 * `minDelayMs`; плюс добавка `[0, max(jitterMs, задержка · jitterFraction))` — разброс растёт
 * с задержкой, иначе задачи одной волны просыпались бы толпой. Настройки
 * проверяются ДО захвата: неверная настройка, обнаруженная после, оставила бы
 * задачу в active с исключением вместо delayed.
 */
export function delayResolver(
  options: DelayOnLimitOptions = {},
): (retryAfterMs: number | null, delaysSoFar?: number) => number {
  const min = nonNegativeInteger('minDelayMs', options.minDelayMs ?? DELAY_ON_LIMIT_DEFAULTS.MIN_DELAY_MS);
  const max = nonNegativeInteger('maxDelayMs', options.maxDelayMs ?? DELAY_ON_LIMIT_DEFAULTS.MAX_DELAY_MS);
  const closed = nonNegativeInteger(
    'closedGroupDelayMs',
    options.closedGroupDelayMs ?? DELAY_ON_LIMIT_DEFAULTS.CLOSED_GROUP_DELAY_MS,
  );
  const jitter = nonNegativeInteger('jitterMs', options.jitterMs ?? DELAY_ON_LIMIT_DEFAULTS.JITTER_MS);
  const fraction = options.jitterFraction ?? DELAY_ON_LIMIT_DEFAULTS.JITTER_FRACTION;
  if (!(fraction >= 0 && fraction <= 1)) {
    throw new Error(`jitterFraction — число от 0 до 1, получено ${fraction}`);
  }
  if (min > max) {
    throw new Error(`minDelayMs (${min}) больше maxDelayMs (${max})`);
  }
  const random = options.random ?? Math.random;
  return (retryAfterMs, delaysSoFar = 0) => {
    const ceiling = Math.min(max, retryAfterMs ?? closed);
    const backoff =
      min * 2 ** Math.min(Math.max(0, delaysSoFar), DELAY_ON_LIMIT_DEFAULTS.MAX_BACKOFF_EXPONENT);
    const base = Math.max(min, Math.min(ceiling, backoff));
    const spread = Math.max(jitter, Math.floor(base * fraction));
    return base + Math.floor(random() * spread);
  };
}

function nonNegativeInteger(name: string, value: number): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} — целое ≥ 0, получено ${value}`);
  }
  return value;
}
