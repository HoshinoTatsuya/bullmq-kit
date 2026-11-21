import { ACQUIRE_OUTCOME } from '../group-concurrency/acquire-outcome.constant';
import type { AcquireOutcome } from '../group-concurrency/acquire-outcome.type';
import { clientKeyPrefix } from '../group-concurrency/client-key-prefix';
import type { GroupConcurrencyRedis } from '../group-concurrency/group-concurrency-redis.type';
import { redisHashTag } from '../group-concurrency/redis-hash-tag';
import { WITH_SLOT } from '../group-concurrency/with-slot.constant';
import type { FairAcquireResult } from './fair-acquire-result.type';
import type { FairShareOptions } from './fair-share-options.type';
import { FAIR_SHARE_SCRIPTS } from './fair-share-scripts.constant';
import type { FairSlot } from './fair-slot.type';

/**
 * Общий пул слотов со справедливой долей групп: крупный арендатор не выедает
 * весь пул воркеров, пока ждут мелкие. Доля группы — по весу среди АКТИВНЫХ
 * (держат слоты или недавно упёрлись); свободное не простаивает — группа
 * занимает сверх доли, если никто из ждущих не ниже своей. Вытеснения нет:
 * лишнее отдаётся по мере завершения работ.
 *
 * Пул — множество держателей со сроком (как у `GroupConcurrency`): снятие
 * идемпотентно, упавший держатель освобождает слот по сроку, время — сервера
 * Redis. Подсказка `retryAfterMs` — худший случай; для отложенного повтора —
 * `acquireOrDelay` (экспонента от малой задержки).
 */
export class FairShare {
  public constructor(
    private readonly _redis: Pick<GroupConcurrencyRedis, 'eval'>,
    private readonly _options: FairShareOptions,
  ) {
    if (_options.prefix.trim() === '') {
      throw new Error('prefix пуст: ключи пула совпали бы с чужими');
    }
    const cluster = _options.cluster ?? (_redis as { isCluster?: unknown }).isCluster === true;
    if (cluster && redisHashTag(`${clientKeyPrefix(_redis)}${_options.prefix}`) === null) {
      throw new Error(
        `prefix «${_options.prefix}» без хэш-тега {…}: три ключа пула лягут в разные слоты (CROSSSLOT)`,
      );
    }
  }

  public async acquire(
    pool: string,
    group: string,
    holder: string,
    slot: FairSlot,
  ): Promise<FairAcquireResult> {
    const weight = slot.weight ?? 1;
    if (!(Number.isFinite(weight) && weight > 0)) {
      throw new Error(`weight — число больше нуля, получено ${weight}`);
    }
    if (!Number.isInteger(slot.poolLimit) || slot.poolLimit <= 0) {
      throw new Error(`poolLimit — целое больше нуля, получено ${slot.poolLimit}`);
    }
    if (!Number.isInteger(slot.ttlMs) || slot.ttlMs <= 0 || slot.ttlMs > WITH_SLOT.MAX_TTL_MS) {
      throw new Error(`ttlMs — целое от 1 до ${WITH_SLOT.MAX_TTL_MS}, получено ${slot.ttlMs}`);
    }
    const waitTtl = slot.waitTtlMs ?? 60_000;
    if (!Number.isInteger(waitTtl) || waitTtl <= 0) {
      throw new Error(`waitTtlMs — целое больше нуля, получено ${waitTtl}`);
    }
    const reply = await this._redis.eval(
      FAIR_SHARE_SCRIPTS.acquire,
      3,
      ...this._keys(pool),
      FairShare._name('группа', group),
      FairShare._name('держатель', holder),
      slot.ttlMs,
      slot.poolLimit,
      weight,
      waitTtl,
    );
    if (!Array.isArray(reply) || reply.length !== 4) {
      throw new Error(`неожиданный ответ скрипта пула: ${String(reply)}`);
    }
    const code = Number(reply[0]);
    const outcome: AcquireOutcome | undefined = (
      { 0: ACQUIRE_OUTCOME.limit, 1: ACQUIRE_OUTCOME.acquired, 2: ACQUIRE_OUTCOME.held } as Record<
        number,
        AcquireOutcome
      >
    )[code];
    if (outcome === undefined) {
      throw new Error(`неожиданный ответ скрипта пула: код ${String(reply[0])}`);
    }
    const wait = Number(reply[1]);
    return {
      outcome,
      retryAfterMs: outcome === ACQUIRE_OUTCOME.limit && wait >= 0 ? wait : null,
      share: Number(reply[2]),
      held: Number(reply[3]),
    };
  }

  /** Снять слот держателя группы. Идемпотентно. `true` — слот был. */
  public async release(pool: string, group: string, holder: string): Promise<boolean> {
    const reply = await this._redis.eval(
      FAIR_SHARE_SCRIPTS.release,
      3,
      ...this._keys(pool),
      FairShare._name('группа', group),
      FairShare._name('держатель', holder),
    );
    return Number(reply) === 1;
  }

  /** Живые держатели пула с группой и остатком срока, по возрастанию дедлайна. */
  public async holders(pool: string): Promise<Array<{ group: string; holder: string; expiresInMs: number }>> {
    const reply = await this._redis.eval(FAIR_SHARE_SCRIPTS.holders, 3, ...this._keys(pool));
    if (!Array.isArray(reply) || reply.length % 3 !== 0) {
      throw new Error(`неожиданный ответ скрипта держателей пула: ${String(reply)}`);
    }
    const holders: Array<{ group: string; holder: string; expiresInMs: number }> = [];
    for (let index = 0; index < reply.length; index += 3) {
      holders.push({
        group: String(reply[index]),
        holder: String(reply[index + 1]),
        expiresInMs: Number(reply[index + 2]),
      });
    }
    return holders;
  }

  private _keys(pool: string): [string, string, string] {
    const name = FairShare._name('пул', pool);
    const { prefix } = this._options;
    return [`${prefix}:${name}`, `${prefix}~w:${name}`, `${prefix}~x:${name}`];
  }

  private static _name(what: string, value: string): string {
    if (value.trim() === '') {
      throw new Error(`${what} пуст`);
    }
    return value;
  }
}
