import { ACQUIRE_OUTCOME } from './acquire-outcome.constant';
import type { AcquireOutcome } from './acquire-outcome.type';
import type { AcquireManyResult, AcquireResult, GroupHolder, GroupLimit } from './acquire-result.type';
import { clientKeyPrefix } from './client-key-prefix';
import type { GroupConcurrencyOptions } from './group-concurrency-options.type';
import type { GroupConcurrencyRedis } from './group-concurrency-redis.type';
import { redisHashTag } from './redis-hash-tag';
import { GROUP_CONCURRENCY_SCRIPTS, SCRIPT_REPLY } from './scripts.constant';
import { settleWithin } from './settle-within';
import { SlotLease } from './slot-lease';
import { SlotLostError } from './slot-lost.error';
import { SlotLostDuringWorkError } from './slot-lost-during-work.error';
import { SlotWaiters } from './slot-waiters';
import type { WaitForSlotOptions } from './wait-for-slot-options.type';
import { WITH_SLOT } from './with-slot.constant';
import type { WithSlotResult } from './with-slot-result.type';

const OUTCOME_BY_REPLY: Readonly<Record<number, AcquireOutcome>> = {
  [SCRIPT_REPLY.limit]: ACQUIRE_OUTCOME.limit,
  [SCRIPT_REPLY.acquired]: ACQUIRE_OUTCOME.acquired,
  [SCRIPT_REPLY.held]: ACQUIRE_OUTCOME.held,
};

type Slot = { readonly limit: number; readonly ttlMs: number };

/**
 * Потолок одновременных задач на группу (арендатор, проект, IP), общий для всех
 * реплик: состояние — в Redis, решения — атомарными скриптами.
 *
 * Группа — МНОЖЕСТВО ДЕРЖАТЕЛЕЙ со сроком (ZSET, очко — дедлайн слота), а не
 * счётчик. Счётчик `INCR`/`DECR` нельзя сделать идемпотентным: снятие
 * приходит из нескольких мест (провал каждой попытки, страховка восстановления,
 * повтор), и лишний `DECR` отдавал чужой слот — замер: при потолке 3 работало 5
 * задач. Здесь снятие — `ZREM` держателя (повтор ничего не меняет), повторный
 * захват тем же держателем слот не тратит, упавший держатель освобождает слот
 * по сроку, а не никогда.
 *
 * Ключ — НОВЫЙ относительно прежних строковых счётчиков: команды множества по
 * строковому ключу отвечают `WRONGTYPE` на каждый вызов сразу после выкатки.
 */
export class GroupConcurrency {
  private readonly _waiters: SlotWaiters | undefined;

  public constructor(
    private readonly _redis: GroupConcurrencyRedis,
    private readonly _options: GroupConcurrencyOptions,
  ) {
    this._waiters = _options.subscriber === undefined ? undefined : new SlotWaiters(_options.subscriber);
    if (_options.prefix.trim() === '') {
      throw new Error('prefix пуст: ключи групп совпали бы с чужими');
    }
    // Клиент Redis Cluster (ioredis Cluster несёт isCluster) или явная
    // настройка: без хэш-тега ключи групп одного `acquireMany` лягут в разные
    // слоты, и `CROSSSLOT` всплыл бы только на первом вызове в проде.
    const cluster = _options.cluster ?? (_redis as { isCluster?: unknown }).isCluster === true;
    // Тег считается по ключу целиком: `keyPrefix` клиента ioredis стоит перед префиксом.
    if (cluster && redisHashTag(`${clientKeyPrefix(_redis)}${_options.prefix}`) === null) {
      throw new Error(
        `prefix «${_options.prefix}» без хэш-тега {…}: в Redis Cluster ключи групп acquireMany лягут в разные ` +
          'слоты (CROSSSLOT). Цена тега — все группы на одном шарде.',
      );
    }
  }

  /**
   * Работа в слоте: занять (`acquireDetailed`), выполнять, продлевая слот каждые
   * `ttlMs / 3`, снять в `finally`. Срок слота больше не надо подгонять под
   * длительность работы: «час с запасом» превращался в час возврата слота после
   * смерти воркера, а «впритык» — в лишнего держателя, если работа затянулась.
   * Здесь `ttlMs` — время возврата слота после смерти процесса, его и задавайте.
   *
   * Слот потерян во время работы — сигнал работы прерывается `SlotLostError`;
   * работа обязана его слушать. Вызов при потере ОТКАЗЫВАЕТ, даже если работа
   * вернула значение: `SlotLostDuringWorkError` (подкласс `SlotLostError`,
   * значение — в `value`). Исход `acquired` означает «вся работа шла под слотом». Потерей считается: продление ответило «слота
   * нет», ИЛИ продление не подтверждено `ttlMs − ttlMs/10` с отправки последнего
   * подтверждённого (в том числе когда Redis не отвечает вовсе: замёрзшая сеть,
   * клиент с `maxRetriesPerRequest: null`). Потолок занят — работа не
   * начинается, ответ `limit` с подсказкой.
   *
   * Исход `held` — слот уже держал этот держатель (прежний захват, ещё живой
   * повтор той же задачи): `withSlot` его продлевает, но НЕ снимает — снимет
   * тот, кто занял, или он истечёт по сроку. Снятие здесь отдало бы чужой слот.
   * Снятие в `finally` ограничено сроком слота: в недоступный Redis оно не
   * держит вызывающего дольше, чем слот прожил бы и без него.
   *
   * `wait` — ждать слот (`acquireWaiting`) до `timeoutMs`, а не отвечать `limit`
   * сразу; требует `subscriber` в настройках.
   */
  public async withSlot<T>(
    group: string,
    holder: string,
    slot: Slot,
    work: (signal: AbortSignal) => Promise<T>,
    wait?: WaitForSlotOptions,
  ): Promise<WithSlotResult<T>> {
    const attempt =
      wait === undefined
        ? { sentAt: SlotLease.now(), result: await this.acquireDetailed(group, holder, slot) }
        : await this._acquireWaiting(group, holder, slot, wait);
    const { sentAt, result: taken } = attempt;
    if (taken.outcome === ACQUIRE_OUTCOME.limit) {
      return { outcome: ACQUIRE_OUTCOME.limit, retryAfterMs: taken.retryAfterMs };
    }
    const controller = new AbortController();
    const lease = new SlotLease(
      slot.ttlMs,
      sentAt,
      () => this.heartbeat(group, holder, slot.ttlMs),
      (reason) => controller.abort(new SlotLostError(group, holder, reason)),
    );
    lease.start();
    try {
      const value = await work(controller.signal);
      // Работа вернулась, но слот потерян во время неё (работа сигнал не слушала
      // или не могла прервать начатый вызов): в группу уже могли пройти другие.
      // Отвечать `acquired` значило бы тихий успех — неотличимый от работы под слотом.
      if (controller.signal.aborted) {
        throw new SlotLostDuringWorkError(controller.signal.reason as SlotLostError, value);
      }
      return { outcome: taken.outcome, value };
    } finally {
      lease.stop();
      if (taken.outcome === ACQUIRE_OUTCOME.acquired) {
        await settleWithin(this.release(group, holder), slot.ttlMs);
      }
    }
  }

  /**
   * Занять слот группы держателем на `ttlMs`. `limit` — целое ≥ 0 или
   * `Infinity` (без потолка; держатели всё равно записываются — их читает
   * `count`). 0 — группа закрыта: отказ любому новому держателю.
   */
  public async acquire(group: string, holder: string, slot: Slot): Promise<AcquireOutcome> {
    return (await this.acquireDetailed(group, holder, slot)).outcome;
  }

  /** Как `acquire`, плюс подсказка `retryAfterMs` при `limit` — когда слот освободится по сроку. */
  public async acquireDetailed(group: string, holder: string, slot: Slot): Promise<AcquireResult> {
    return await this._take(GROUP_CONCURRENCY_SCRIPTS.acquire, group, holder, slot);
  }

  /**
   * Зарезервировать слот на подготовку работы: как `acquire`, но слот, который
   * держатель уже держит, НЕ продлевается (`held`, срок прежний). Резерв
   * короче рабочего срока: продление им укоротило бы слот идущей работы.
   * После подготовки слот переводят на рабочий срок обычным `acquire`.
   */
  public async reserve(group: string, holder: string, slot: Slot): Promise<AcquireOutcome> {
    return (await this.reserveDetailed(group, holder, slot)).outcome;
  }

  /** Как `reserve`, плюс подсказка `retryAfterMs` при `limit`. */
  public async reserveDetailed(group: string, holder: string, slot: Slot): Promise<AcquireResult> {
    return await this._take(GROUP_CONCURRENCY_SCRIPTS.reserve, group, holder, slot);
  }

  /**
   * Занять слоты держателя в НЕСКОЛЬКИХ группах одним скриптом: всё или ничего
   * (проект И арендатор И IP). Отказ хоть одной группы не меняет ничего — ни
   * новых слотов, ни сроков уже занятых. Слоты, которые держатель уже держит,
   * продлеваются, как у `acquire`. Снимать — `release` по каждой группе.
   * В Redis Cluster префикс обязан нести хэш-тег (см. `prefix`).
   */
  public async acquireMany(
    holder: string,
    groups: readonly GroupLimit[],
    ttlMs: number,
  ): Promise<AcquireManyResult> {
    if (groups.length === 0) {
      throw new Error('acquireMany: список групп пуст');
    }
    const keys = groups.map(({ group }) => this._key(group));
    if (new Set(keys).size !== keys.length) {
      throw new Error('acquireMany: группа повторяется — слот в ней считался бы дважды');
    }
    const limits = groups.map(({ limit }) => GroupConcurrency._limitArg(limit));
    const reply = await this._redis.eval(
      GROUP_CONCURRENCY_SCRIPTS.acquireMany,
      keys.length,
      ...keys,
      GroupConcurrency._holder(holder),
      GroupConcurrency._ttl(ttlMs),
      ...limits,
    );
    if (!Array.isArray(reply) || reply.length !== 3 || !Array.isArray(reply[2])) {
      throw new Error(`неожиданный ответ скрипта захвата групп: ${String(reply)}`);
    }
    const codes = (reply[2] as unknown[]).map((code) => GroupConcurrency._outcome(code));
    if (codes.length !== groups.length) {
      throw new Error(
        `неожиданный ответ скрипта захвата групп: кодов ${codes.length} на ${groups.length} групп`,
      );
    }
    if (Number(reply[0]) === SCRIPT_REPLY.manyBlocked) {
      const blockedBy = groups
        .filter((_, index) => codes[index] === ACQUIRE_OUTCOME.limit)
        .map(({ group }) => group);
      for (const group of blockedBy) {
        this._notify(group, ACQUIRE_OUTCOME.limit);
      }
      return {
        outcome: ACQUIRE_OUTCOME.limit,
        retryAfterMs: GroupConcurrency._retryAfter(reply[1]),
        blockedBy,
      };
    }
    for (const [index, { group }] of groups.entries()) {
      this._notify(group, codes[index] as AcquireOutcome);
    }
    return {
      outcome: codes.includes(ACQUIRE_OUTCOME.acquired) ? ACQUIRE_OUTCOME.acquired : ACQUIRE_OUTCOME.held,
      retryAfterMs: null,
      blockedBy: [],
    };
  }

  /** Продлить ЖИВОЙ слот держателя. `false` — слота нет (снят или истёк): занимать заново — через `acquire`. */
  public async heartbeat(group: string, holder: string, ttlMs: number): Promise<boolean> {
    const reply = await this._redis.eval(
      GROUP_CONCURRENCY_SCRIPTS.heartbeat,
      1,
      this._key(group),
      GroupConcurrency._holder(holder),
      GroupConcurrency._ttl(ttlMs),
    );
    return Number(reply) === 1;
  }

  /** Снять слот держателя. Идемпотентно: повтор и снятие чужим путём ничего не отдают. `true` — слот был. */
  public async release(group: string, holder: string): Promise<boolean> {
    const reply = await this._redis.eval(
      GROUP_CONCURRENCY_SCRIPTS.releaseNotify,
      3,
      this._key(group),
      this._queueKey(group),
      this._queueDeadlineKey(group),
      GroupConcurrency._holder(holder),
      this._channel(group),
    );
    return Number(reply) === 1;
  }

  /**
   * Ждать слот, а не опрашивать: ожидающие встают в очередь группы (порядок
   * прихода), освобождение слота (`release`) будит ПЕРВОГО ожидающего
   * публикацией в канал группы. Свободный слот достаётся ожидающим по порядку:
   * держатель занимает его, только если его место в очереди меньше числа
   * свободных. Слот, освобождённый по сроку (держатель умер) или `clear`,
   * публикации не даёт — его подхватывает страховочная перепроверка (`pollMs`).
   *
   * Не дождались за `timeoutMs` или отменили (`signal`) — исход `limit` с
   * подсказкой, ожидающий уходит из очереди. Требует `subscriber` в настройках
   * (отдельное соединение: в режиме подписки оно других команд не принимает).
   * Обычный `acquire` очередь не соблюдает: смешивать их в одной группе можно,
   * но порядок тогда держится только среди ожидающих.
   */
  public async acquireWaiting(
    group: string,
    holder: string,
    slot: Slot,
    options: WaitForSlotOptions,
  ): Promise<AcquireResult> {
    return (await this._acquireWaiting(group, holder, slot, options)).result;
  }

  /** Число живых держателей группы (просроченные снимаются тем же вызовом). */
  public async count(group: string): Promise<number> {
    return Number(await this._redis.eval(GROUP_CONCURRENCY_SCRIPTS.count, 1, this._key(group)));
  }

  /**
   * Живые держатели группы с остатком срока, по возрастанию дедлайна — для
   * разбора «кто держит слоты». Просроченные снимаются тем же вызовом.
   */
  public async holders(group: string): Promise<GroupHolder[]> {
    const reply = await this._redis.eval(GROUP_CONCURRENCY_SCRIPTS.holders, 1, this._key(group));
    if (!Array.isArray(reply) || reply.length % 2 !== 0) {
      throw new Error(`неожиданный ответ скрипта держателей: ${String(reply)}`);
    }
    const holders: GroupHolder[] = [];
    for (let index = 0; index < reply.length; index += 2) {
      holders.push({ holder: String(reply[index]), expiresInMs: Number(reply[index + 1]) });
    }
    return holders;
  }

  /**
   * Снять ВСЕХ держателей группы (админ: зависшие слоты после инцидента).
   * Работающие держатели при этом теряют слоты: их `heartbeat` вернёт `false`,
   * а в группу пройдут новые — потолок на время их работы превышается.
   * Ответ — сколько живых держателей было снято.
   */
  public async clear(group: string): Promise<number> {
    return Number(await this._redis.eval(GROUP_CONCURRENCY_SCRIPTS.clear, 1, this._key(group)));
  }

  private async _take(script: string, group: string, holder: string, slot: Slot): Promise<AcquireResult> {
    const limitArg = GroupConcurrency._limitArg(slot.limit);
    const reply = await this._redis.eval(
      script,
      1,
      this._key(group),
      GroupConcurrency._holder(holder),
      GroupConcurrency._ttl(slot.ttlMs),
      limitArg,
    );
    if (!Array.isArray(reply) || reply.length !== 2) {
      throw new Error(`неожиданный ответ скрипта захвата: ${String(reply)}`);
    }
    const outcome = GroupConcurrency._outcome(reply[0]);
    this._notify(group, outcome);
    return {
      outcome,
      retryAfterMs: outcome === ACQUIRE_OUTCOME.limit ? GroupConcurrency._retryAfter(reply[1]) : null,
    };
  }

  /** Хук метрик: его отказ не должен превращать занятый слот в исключение у вызывающего. */
  private _notify(group: string, outcome: AcquireOutcome): void {
    const hook = this._options.onOutcome;
    if (hook === undefined) {
      return;
    }
    try {
      const result = hook(group, outcome);
      // Асинхронный хук: отклонённый промис без обработчика уронил бы процесс
      // (unhandledRejection), поэтому отказ гасится и здесь.
      if (
        typeof result === 'object' &&
        result !== null &&
        'then' in result &&
        typeof result.then === 'function'
      ) {
        (result as PromiseLike<unknown>).then(undefined, () => undefined);
      }
    } catch {
      // Глотается осознанно: см. GroupConcurrencyOptions.onOutcome.
    }
  }

  /** Ключ группы. Пустая группа — отказ: пустой id обычно значит «не нашли», а не «группа ''». */
  private _key(group: string): string {
    if (group.trim() === '') {
      throw new Error('группа пуста');
    }
    return `${this._options.prefix}:${group}`;
  }

  /**
   * Очередь ожидающих, сроки их регистрации, канал группы. `~` вместо `:` после
   * префикса — чтобы ни одна группа не совпала с ними ключом; хэш-тег префикса
   * кладёт их в слот группы.
   */
  private _queueKey(group: string): string {
    return `${this._options.prefix}~q:${this._key(group).slice(this._options.prefix.length + 1)}`;
  }

  private _queueDeadlineKey(group: string): string {
    return `${this._options.prefix}~qd:${this._key(group).slice(this._options.prefix.length + 1)}`;
  }

  private _channel(group: string): string {
    return `${this._options.prefix}~c:${this._key(group).slice(this._options.prefix.length + 1)}`;
  }

  private async _acquireWaiting(
    group: string,
    holder: string,
    slot: Slot,
    { timeoutMs, signal, pollMs = WITH_SLOT.WAIT_POLL_MS }: WaitForSlotOptions,
  ): Promise<{ sentAt: number; result: AcquireResult }> {
    const waiters = this._waiters;
    if (waiters === undefined) {
      throw new Error(
        'acquireWaiting: нужен subscriber в настройках (отдельное соединение, redis.duplicate())',
      );
    }
    if (!Number.isInteger(timeoutMs) || timeoutMs < 0) {
      throw new Error(`timeoutMs — целое ≥ 0, получено ${timeoutMs}`);
    }
    // Больше предела таймера Node — 1 мс (TimeoutOverflowWarning): «редкий опрос» стал бы непрерывным.
    if (!Number.isInteger(pollMs) || pollMs <= 0 || pollMs > WITH_SLOT.MAX_TTL_MS) {
      throw new Error(
        `pollMs — целое от 1 до ${WITH_SLOT.MAX_TTL_MS} (предел таймера Node), получено ${pollMs}`,
      );
    }
    if (signal?.aborted === true) {
      // Отменено до начала: ни очереди, ни слота — занятый слот вызывающему уже не нужен.
      this._notify(group, ACQUIRE_OUTCOME.limit);
      return { sentAt: SlotLease.now(), result: { outcome: ACQUIRE_OUTCOME.limit, retryAfterMs: null } };
    }
    const keys = [this._key(group), this._queueKey(group), this._queueDeadlineKey(group)] as const;
    const name = GroupConcurrency._holder(holder);
    const args = [
      name,
      GroupConcurrency._ttl(slot.ttlMs),
      GroupConcurrency._limitArg(slot.limit),
      pollMs * WITH_SLOT.WAITER_TTL_POLLS,
      this._channel(group),
    ] as const;
    const started = SlotLease.now();
    let wake: () => void = () => undefined;
    const unlisten = await waiters.listen(this._channel(group), (message) => {
      if (message === name) {
        wake();
      }
    });
    try {
      for (;;) {
        // Будильник — ДО попытки: публикация между отказом и ожиданием не теряется.
        const woken = new Promise<void>((resolve) => {
          wake = resolve;
        });
        const sentAt = SlotLease.now();
        const reply = await this._redis.eval(GROUP_CONCURRENCY_SCRIPTS.acquireQueued, 3, ...keys, ...args);
        if (!Array.isArray(reply) || reply.length !== 3) {
          throw new Error(`неожиданный ответ скрипта захвата с очередью: ${String(reply)}`);
        }
        const outcome = GroupConcurrency._outcome(reply[0]);
        const retryAfterMs =
          outcome === ACQUIRE_OUTCOME.limit ? GroupConcurrency._retryAfter(reply[1]) : null;
        const left = timeoutMs - (SlotLease.now() - started);
        // Чтение заново: отмена может прийти во время попытки.
        if (
          outcome !== ACQUIRE_OUTCOME.limit ||
          left <= 0 ||
          (signal?.aborted as boolean | undefined) === true
        ) {
          if (outcome === ACQUIRE_OUTCOME.limit) {
            await this._redis.eval(GROUP_CONCURRENCY_SCRIPTS.leaveQueue, 2, keys[1], keys[2], name);
          }
          this._notify(group, outcome);
          return { sentAt, result: { outcome, retryAfterMs } };
        }
        await GroupConcurrency._nap(Math.min(left, pollMs, retryAfterMs ?? pollMs), woken, signal);
        // Чтение заново: отмена приходит во время ожидания, а не до него.
        if ((signal?.aborted as boolean | undefined) === true) {
          // Отменили во время ожидания — без ещё одной попытки: занятый после отмены слот вызывающему не нужен.
          await this._redis.eval(GROUP_CONCURRENCY_SCRIPTS.leaveQueue, 2, keys[1], keys[2], name);
          this._notify(group, ACQUIRE_OUTCOME.limit);
          return { sentAt, result: { outcome: ACQUIRE_OUTCOME.limit, retryAfterMs } };
        }
      }
    } finally {
      await unlisten();
    }
  }

  /** Ждать будильника, таймера или отмены — что раньше. */
  private static async _nap(
    ms: number,
    woken: Promise<void>,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    let timer: NodeJS.Timeout | undefined;
    let onAbort: (() => void) | undefined;
    try {
      await Promise.race([
        woken,
        new Promise<void>((resolve) => {
          timer = setTimeout(resolve, Math.max(1, ms));
          timer.unref();
        }),
        new Promise<void>((resolve) => {
          onAbort = () => resolve();
          signal?.addEventListener('abort', onAbort, { once: true });
        }),
      ]);
    } finally {
      clearTimeout(timer);
      if (onAbort !== undefined) {
        signal?.removeEventListener('abort', onAbort);
      }
    }
  }

  private static _outcome(code: unknown): AcquireOutcome {
    const outcome = OUTCOME_BY_REPLY[Number(code)];
    if (outcome === undefined) {
      throw new Error(`неожиданный ответ скрипта захвата: ${String(code)}`);
    }
    return outcome;
  }

  private static _retryAfter(value: unknown): number | null {
    const ms = Number(value);
    return ms === SCRIPT_REPLY.never || !Number.isFinite(ms) ? null : ms;
  }

  private static _holder(holder: string): string {
    if (holder.trim() === '') {
      throw new Error('держатель пуст: все такие захваты считались бы одним слотом');
    }
    return holder;
  }

  private static _ttl(ttlMs: number): number {
    if (!Number.isInteger(ttlMs) || ttlMs <= 0 || ttlMs > WITH_SLOT.MAX_TTL_MS) {
      throw new Error(
        `ttlMs — целое от 1 до ${WITH_SLOT.MAX_TTL_MS} (предел таймера Node), получено ${ttlMs}`,
      );
    }
    return ttlMs;
  }

  private static _limitArg(limit: number): number {
    if (limit === Number.POSITIVE_INFINITY) {
      return -1;
    }
    if (!Number.isInteger(limit) || limit < 0) {
      throw new Error(`limit — целое ≥ 0 или Infinity, получено ${limit}`);
    }
    return limit;
  }
}
