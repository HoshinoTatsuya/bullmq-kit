import { ACQUIRE_OUTCOME } from '../group-concurrency/acquire-outcome.constant';
import type { AcquireResult } from '../group-concurrency/acquire-result.type';
import type { GroupConcurrencyRedis } from '../group-concurrency/group-concurrency-redis.type';
import type { GroupRateLimitOptions } from './group-rate-limit-options.type';
import type { Rate } from './rate.type';
import { RATE_LIMIT_SCRIPTS } from './rate-limit-scripts.constant';

/**
 * Темп на группу: «не больше N за интервал на арендатора/хост», общий для всех
 * реплик (ведро токенов в Redis, время сервера). Внешние API ограничивают
 * ТЕМП, а не параллельность: потолок одновременных задач (`GroupConcurrency`)
 * пропустил бы сотню коротких запросов в секунду при потолке 3.
 *
 * Ответ — тот же `AcquireResult`, что у захвата слота, и поэтому годится
 * `acquireOrDelay` как есть. Подсказка здесь ТОЧНАЯ (`exact: true`): через
 * столько мс токен будет — если его не возьмёт другой; `acquireOrDelay`
 * откладывает задачу ровно на неё, а не по экспоненте.
 * Взятый токен не возвращается: темп — про начатые операции, а не про идущие.
 */
export class GroupRateLimit {
  public constructor(
    private readonly _redis: Pick<GroupConcurrencyRedis, 'eval'>,
    private readonly _options: GroupRateLimitOptions,
  ) {
    if (_options.prefix.trim() === '') {
      throw new Error('prefix пуст: ключи ведер совпали бы с чужими');
    }
  }

  /** Взять `cost` токенов группы (по умолчанию один). */
  public async take(group: string, rate: Rate, cost = 1): Promise<AcquireResult & { readonly exact: true }> {
    if (group.trim() === '') {
      throw new Error('группа пуста');
    }
    if (!Number.isInteger(rate.limit) || rate.limit <= 0) {
      throw new Error(`limit — целое больше нуля, получено ${rate.limit}`);
    }
    if (!Number.isInteger(rate.intervalMs) || rate.intervalMs <= 0) {
      throw new Error(`intervalMs — целое больше нуля, получено ${rate.intervalMs}`);
    }
    if (!Number.isInteger(cost) || cost <= 0 || cost > rate.limit) {
      // Больше ёмкости ведро не наберёт никогда: ждать было бы вечно.
      throw new Error(`cost — целое от 1 до limit (${rate.limit}), получено ${cost}`);
    }
    const reply = await this._redis.eval(
      RATE_LIMIT_SCRIPTS.take,
      1,
      `${this._options.prefix}:${group}`,
      rate.limit,
      rate.intervalMs,
      cost,
    );
    if (!Array.isArray(reply) || reply.length !== 3) {
      throw new Error(`неожиданный ответ скрипта темпа: ${String(reply)}`);
    }
    const taken = Number(reply[0]) === 1;
    return {
      outcome: taken ? ACQUIRE_OUTCOME.acquired : ACQUIRE_OUTCOME.limit,
      retryAfterMs: taken ? null : Number(reply[1]),
      exact: true,
    };
  }
}
