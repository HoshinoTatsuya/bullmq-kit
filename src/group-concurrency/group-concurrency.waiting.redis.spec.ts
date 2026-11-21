import { randomUUID } from 'node:crypto';

import Redis from 'ioredis';

import { GroupConcurrency } from './group-concurrency';
import { GROUP_CONCURRENCY_SCRIPTS } from './scripts.constant';

/**
 * Ожидание слота вместо опроса — на НАСТОЯЩЕМ Redis: очередь ожидающих,
 * пробуждение ПЕРВОГО публикацией при `release`, страховочная перепроверка.
 * Перепроверка в тестах на пробуждение — минута: если ожидающий получил слот
 * за секунды, разбудила его публикация, а не таймер (без замера часов).
 */
const url = process.env.REDIS_TEST_URL;
const describeRedis = url ? describe : describe.skip;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const MINUTE = 60_000;

describeRedis('GroupConcurrency.acquireWaiting [нужен REDIS_TEST_URL]', () => {
  let redis: Redis;
  let subscriber: Redis;
  let prefix: string;
  let attempts: number;
  let slots: GroupConcurrency;
  const slot = { limit: 1, ttlMs: MINUTE };

  beforeAll(() => {
    redis = new Redis(url as string, { maxRetriesPerRequest: null });
    subscriber = new Redis(url as string, { maxRetriesPerRequest: null });
  });
  afterAll(async () => {
    await subscriber.quit();
    await redis.quit();
  });
  beforeEach(() => {
    prefix = `gc-${randomUUID()}`;
    attempts = 0;
    // Счёт попыток захвата с очередью: каждая — EVAL скрипта acquireQueued.
    const counted = {
      eval: (script: string, numKeys: number, ...args: (string | number)[]) => {
        if (script === GROUP_CONCURRENCY_SCRIPTS.acquireQueued) {
          attempts += 1;
        }
        return redis.eval(script, numKeys, ...args);
      },
    };
    slots = new GroupConcurrency(counted, { prefix, subscriber });
  });

  const queued = () => redis.zcard(`${prefix}~q:g`);
  const untilQueued = async (count: number) => {
    for (let i = 0; i < 200 && (await queued()) < count; i += 1) {
      await sleep(5);
    }
    expect(await queued()).toBe(count);
  };

  it('ожидающие получают слот по порядку прихода, и каждый просыпается один раз — от release, а не от таймера', async () => {
    expect(await slots.acquire('g', 'X', slot)).toBe('acquired');
    const order: string[] = [];
    const waiting: Array<Promise<void>> = [];
    for (const holder of ['W1', 'W2', 'W3']) {
      waiting.push(
        slots
          .acquireWaiting('g', holder, slot, { timeoutMs: MINUTE, pollMs: MINUTE })
          .then(async (result) => {
            order.push(`${holder}:${result.outcome}`);
            await sleep(20);
            await slots.release('g', holder);
          }),
      );
      await untilQueued(waiting.length);
    }

    await slots.release('g', 'X');
    await Promise.all(waiting);

    expect(order).toEqual(['W1:acquired', 'W2:acquired', 'W3:acquired']);
    // Регистрация и одна попытка после пробуждения — на каждого, ни одной лишней.
    expect(attempts).toBe(6);
    expect(await redis.exists(`${prefix}~q:g`, `${prefix}~qd:g`)).toBe(0);
  }, 20_000);

  /**
   * Освобождаются несколько слотов разом: каждое снятие будит ПЕРВОГО, а он
   * ещё не успел занять слот — без цепочки (занявший будит следующих по числу
   * оставшихся свободных) остальные ждали бы страховочной перепроверки.
   */
  it('несколько слотов освободились разом — просыпается столько ожидающих, сколько свободно, без перепроверки', async () => {
    const wide = { limit: 3, ttlMs: MINUTE };
    for (const holder of ['X1', 'X2', 'X3']) {
      expect(await slots.acquire('g', holder, wide)).toBe('acquired');
    }
    const waiting: Array<Promise<string>> = [];
    for (const holder of ['W1', 'W2', 'W3']) {
      waiting.push(
        slots
          .acquireWaiting('g', holder, wide, { timeoutMs: MINUTE, pollMs: MINUTE })
          .then((result) => `${holder}:${result.outcome}`),
      );
      await untilQueued(waiting.length);
    }

    await Promise.all(['X1', 'X2', 'X3'].map((holder) => slots.release('g', holder)));

    expect((await Promise.all(waiting)).sort()).toEqual(['W1:acquired', 'W2:acquired', 'W3:acquired']);
    expect(attempts).toBe(6);
    // Срок теста — 10 с при перепроверке раз в минуту: успели — значит, всех разбудили публикации.
  }, 10_000);

  it('слот свободен, но впереди ожидающий — пришедший позже слот не перехватывает', async () => {
    // Слот X снимается в обход release — публикации нет, и W1 (перепроверка раз в минуту) о нём не узнает.
    expect(await slots.acquire('g', 'X', slot)).toBe('acquired');
    const controller = new AbortController();
    const first = slots.acquireWaiting('g', 'W1', slot, {
      timeoutMs: MINUTE,
      pollMs: MINUTE,
      signal: controller.signal,
    });
    await untilQueued(1);
    await redis.zrem(`${prefix}:g`, 'X');
    // Контроль фикстуры: слот действительно свободен.
    expect(await slots.count('g')).toBe(0);

    const late = await slots.acquireWaiting('g', 'N', slot, { timeoutMs: 200, pollMs: 50 });

    expect(late.outcome).toBe('limit');
    controller.abort();
    await expect(first).resolves.toMatchObject({ outcome: 'limit' });
    expect(await queued()).toBe(0);
  }, 20_000);

  it('не дождался за timeoutMs — limit с подсказкой, из очереди ушёл', async () => {
    await slots.acquire('g', 'X', slot);
    const result = await slots.acquireWaiting('g', 'W', slot, { timeoutMs: 200, pollMs: 50 });
    expect(result.outcome).toBe('limit');
    expect(result.retryAfterMs).toBeGreaterThan(MINUTE - 5_000);
    expect(await queued()).toBe(0);
  });

  it('отмена сигналом — limit сразу, из очереди ушёл', async () => {
    await slots.acquire('g', 'X', slot);
    const controller = new AbortController();
    const waiting = slots.acquireWaiting('g', 'W', slot, {
      timeoutMs: MINUTE,
      pollMs: MINUTE,
      signal: controller.signal,
    });
    await untilQueued(1);
    controller.abort();
    await expect(waiting).resolves.toMatchObject({ outcome: 'limit' });
    expect(await queued()).toBe(0);
  });

  it('умерший ожидающий (регистрация истекла) не держит очередь', async () => {
    await redis.zadd(`${prefix}~q:g`, 1, 'ghost');
    await redis.zadd(`${prefix}~qd:g`, 1, 'ghost');
    const result = await slots.acquireWaiting('g', 'W', slot, { timeoutMs: 0 });
    expect(result.outcome).toBe('acquired');
    expect(attempts).toBe(1);
  });

  it('слот освободился по сроку (публикации нет) — ожидающий берёт его перепроверкой', async () => {
    await slots.acquire('g', 'X', { limit: 1, ttlMs: 300 });
    const result = await slots.acquireWaiting('g', 'W', slot, { timeoutMs: 10_000, pollMs: 100 });
    expect(result.outcome).toBe('acquired');
  });

  it('withSlot с ожиданием: работа начинается, когда держатель отпустил слот', async () => {
    await slots.acquire('g', 'X', slot);
    void sleep(100).then(() => slots.release('g', 'X'));
    const result = await slots.withSlot('g', 'W', slot, async () => 'сделано', {
      timeoutMs: MINUTE,
      pollMs: MINUTE,
    });
    expect(result).toEqual({ outcome: 'acquired', value: 'сделано' });
    expect(await slots.count('g')).toBe(0);
  }, 20_000);

  /**
   * Голова очереди умерла, не сняв себя (регистрация истекла): release будил
   * её, а живой следующий ждал страховочной перепроверки. Перепроверка здесь —
   * минута, ожидание — 3 с: успел — значит, разбудила публикация.
   */
  it('release будит первого ЖИВОГО ожидающего: умершая голова очереди снимается', async () => {
    expect(await slots.acquire('g', 'X', slot)).toBe('acquired');
    const controller = new AbortController();
    const waiting = slots.acquireWaiting('g', 'W1', slot, {
      timeoutMs: MINUTE,
      pollMs: MINUTE,
      signal: controller.signal,
    });
    await untilQueued(1);
    // Умерший пришёл раньше W1 (очко прихода меньше), его регистрация истекла.
    await redis.zadd(`${prefix}~q:g`, 1, 'ghost');
    await redis.zadd(`${prefix}~qd:g`, 1, 'ghost');
    expect(await redis.zrange(`${prefix}~q:g`, '0', '0')).toEqual(['ghost']);

    await slots.release('g', 'X');

    // Перепроверка — минута: за 3 с слот мог прийти только публикацией.
    let timer: NodeJS.Timeout | undefined;
    const outcome = await Promise.race([
      waiting.then((result) => result.outcome),
      new Promise<string>((resolve) => {
        timer = setTimeout(() => resolve('ещё ждёт'), 3_000);
      }),
    ]);
    clearTimeout(timer);
    controller.abort();
    await waiting;
    expect(outcome).toBe('acquired');
    expect(await redis.zscore(`${prefix}~q:g`, 'ghost')).toBeNull();
    await slots.release('g', 'W1');
  }, 10_000);

  /**
   * Отказ подписки (обрыв соединения подписчика) оставался в кэше подписок
   * навсегда: каждое следующее ожидание в этой группе получало тот же отказ
   * без новой попытки подписаться.
   */
  it('отказ подписки не кэшируется: следующее ожидание подписывается заново', async () => {
    let failures = 1;
    const flaky = {
      subscribe: (...channels: string[]) =>
        failures-- > 0 ? Promise.reject(new Error('подписчик отключён')) : subscriber.subscribe(...channels),
      unsubscribe: (...channels: string[]) => subscriber.unsubscribe(...channels),
      on: (event: 'message', listener: (channel: string, message: string) => void) =>
        subscriber.on(event, listener),
    };
    const waiter = new GroupConcurrency(redis, { prefix, subscriber: flaky });
    await expect(waiter.acquireWaiting('g', 'W1', slot, { timeoutMs: 0 })).rejects.toThrow(
      'подписчик отключён',
    );
    await expect(waiter.acquireWaiting('g', 'W2', slot, { timeoutMs: 0 })).resolves.toMatchObject({
      outcome: 'acquired',
    });
    await waiter.release('g', 'W2');
  });

  it('уже отменённый сигнал — limit без захвата: слот не занимается, Redis не трогается', async () => {
    const controller = new AbortController();
    controller.abort();
    const result = await slots.acquireWaiting('g', 'W', slot, {
      timeoutMs: MINUTE,
      signal: controller.signal,
    });
    expect(result).toEqual({ outcome: 'limit', retryAfterMs: null });
    expect(attempts).toBe(0);
    expect(await slots.count('g')).toBe(0);
    let ran = false;
    const viaSlot = await slots.withSlot(
      'g',
      'W',
      slot,
      async () => {
        ran = true;
      },
      { timeoutMs: MINUTE, signal: controller.signal },
    );
    expect(viaSlot.outcome).toBe('limit');
    expect(ran).toBe(false);
  });

  /** Больше 2^31 − 1 мс Node молча заменяет на 1 мс: «опрос раз в месяц» стал бы опросом раз в миллисекунду. */
  it('pollMs сверх предела таймера Node — отказ, а не опрос каждую миллисекунду', async () => {
    await slots.acquire('g', 'X', slot);
    await expect(
      slots.acquireWaiting('g', 'W', slot, { timeoutMs: 200, pollMs: 2_147_483_648 }),
    ).rejects.toThrow('pollMs');
    expect(attempts).toBe(0);
  });

  it('без subscriber — отказ с причиной, а не тихий опрос', async () => {
    const plain = new GroupConcurrency(redis, { prefix });
    await expect(plain.acquireWaiting('g', 'W', slot, { timeoutMs: 100 })).rejects.toThrow('subscriber');
  });
});
