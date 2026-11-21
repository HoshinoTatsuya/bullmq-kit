import { randomUUID } from 'node:crypto';

import Redis from 'ioredis';

import { GroupConcurrency } from './group-concurrency';
import { SlotLostError } from './slot-lost.error';
import { SlotLostDuringWorkError } from './slot-lost-during-work.error';

/**
 * На НАСТОЯЩЕМ Redis: атомарность, срок слота и время сервера проверяются
 * только против него. Адрес — REDIS_TEST_URL (`pnpm test` поднимает redis в
 * docker); без адреса набор пропускается, и jest-assert-no-skipped валит прогон.
 */
const url = process.env.REDIS_TEST_URL;
const describeRedis = url ? describe : describe.skip;

describeRedis('GroupConcurrency на Redis [нужен REDIS_TEST_URL]', () => {
  let redis: Redis;
  let prefix: string;
  let slots: GroupConcurrency;
  const slot = { limit: 3, ttlMs: 60_000 };

  beforeAll(() => {
    redis = new Redis(url as string, { maxRetriesPerRequest: null });
  });
  afterAll(async () => {
    await redis.quit();
  });
  beforeEach(() => {
    prefix = `gc-${randomUUID()}`;
    slots = new GroupConcurrency(redis, { prefix });
  });

  it('потолок: три держателя заняли слоты, четвёртый — отказ', async () => {
    expect([
      await slots.acquire('p1', 'a', slot),
      await slots.acquire('p1', 'b', slot),
      await slots.acquire('p1', 'c', slot),
      await slots.acquire('p1', 'd', slot),
    ]).toEqual(['acquired', 'acquired', 'acquired', 'limit']);
    expect(await slots.count('p1')).toBe(3);
  });

  it('повторный захват тем же держателем слот не тратит', async () => {
    await slots.acquire('p1', 'a', slot);
    await slots.acquire('p1', 'b', slot);
    expect(await slots.acquire('p1', 'a', slot)).toBe('held');
    expect(await slots.count('p1')).toBe(2);
    expect(await slots.acquire('p1', 'c', slot)).toBe('acquired');
    expect(await slots.acquire('p1', 'd', slot)).toBe('limit');
  });

  it('снятие идемпотентно: тройное снятие одной задачи не отдаёт чужие слоты', async () => {
    // Замеренный дефект счётчика: три DECR одной задачи при потолке 3 оставляли
    // работать две чужие задачи без слотов, и пролезали ещё три — 5 вместо 3.
    for (const holder of ['a', 'b', 'c']) {
      await slots.acquire('p1', holder, slot);
    }
    expect(await slots.release('p1', 'a')).toBe(true);
    expect(await slots.release('p1', 'a')).toBe(false);
    expect(await slots.release('p1', 'a')).toBe(false);
    expect(await slots.count('p1')).toBe(2);
    expect([
      await slots.acquire('p1', 'd', slot),
      await slots.acquire('p1', 'e', slot),
      await slots.acquire('p1', 'f', slot),
    ]).toEqual(['acquired', 'limit', 'limit']);
  });

  /**
   * Резерв держит слот на подготовку и короче рабочего срока: продли он уже
   * занятый слот, идущая работа осталась бы со сроком резерва и потеряла бы
   * слот посреди прогона (например, планировщик на каждом обходе резервирует
   * и активный проект).
   */
  it('резерв занятый слот не трогает, свободный — занимает по потолку', async () => {
    await slots.acquire('p1', 'a', { limit: 2, ttlMs: 60_000 });

    expect(await slots.reserve('p1', 'a', { limit: 2, ttlMs: 1_000 })).toBe('held');
    expect(await redis.pttl(`${prefix}:p1`)).toBeGreaterThan(30_000);
    expect(Number(await redis.zscore(`${prefix}:p1`, 'a'))).toBeGreaterThan(Date.now() + 30_000);

    expect(await slots.reserve('p1', 'b', { limit: 2, ttlMs: 1_000 })).toBe('acquired');
    expect(await slots.reserve('p1', 'c', { limit: 2, ttlMs: 1_000 })).toBe('limit');
    expect(await slots.count('p1')).toBe(2);
  });

  it('продление после снятия слот НЕ занимает: иначе мимо потолка', async () => {
    await slots.acquire('p1', 'a', slot);
    await slots.release('p1', 'a');
    expect(await slots.heartbeat('p1', 'a', slot.ttlMs)).toBe(false);
    expect(await slots.count('p1')).toBe(0);
  });

  it('продление живого слота сдвигает его срок', async () => {
    await slots.acquire('p1', 'a', { limit: 3, ttlMs: 1_000 });
    const before = Number(await redis.zscore(`${prefix}:p1`, 'a'));
    expect(await slots.heartbeat('p1', 'a', 600_000)).toBe(true);
    const after = Number(await redis.zscore(`${prefix}:p1`, 'a'));
    expect(after - before).toBeGreaterThanOrEqual(599_000);
  });

  it('упавший держатель освобождает слот по сроку, живой — остаётся', async () => {
    // Долгий держатель держит ключ живым: иначе ключ истёк бы целиком по
    // своему сроку и скрыл бы, что просроченный держатель не снимается.
    await slots.acquire('p1', 'long', { limit: 2, ttlMs: 60_000 });
    await slots.acquire('p1', 'crashed', { limit: 2, ttlMs: 1 });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(await slots.count('p1')).toBe(1);
    expect(await slots.acquire('p1', 'b', { limit: 2, ttlMs: 60_000 })).toBe('acquired');
    expect(await slots.acquire('p1', 'c', { limit: 2, ttlMs: 60_000 })).toBe('limit');
  });

  it('захват атомарен: 25 одновременных при потолке 3 — ровно 3 слота', async () => {
    const outcomes = await Promise.all(
      Array.from({ length: 25 }, (_, index) => slots.acquire('p1', `h${index}`, slot)),
    );
    expect(outcomes.filter((outcome) => outcome === 'acquired')).toHaveLength(3);
    expect(await slots.count('p1')).toBe(3);
  });

  it('группы независимы; ключ группы со сроком и исчезает, когда держателей нет', async () => {
    await slots.acquire('p1', 'a', { limit: 1, ttlMs: 60_000 });
    expect(await slots.acquire('p2', 'a', { limit: 1, ttlMs: 60_000 })).toBe('acquired');
    const pttl = await redis.pttl(`${prefix}:p1`);
    expect(pttl).toBeGreaterThan(0);
    expect(pttl).toBeLessThanOrEqual(60_000);
    await slots.release('p1', 'a');
    expect(await redis.exists(`${prefix}:p1`)).toBe(0);
  });

  it('потолок 0 закрывает группу, Infinity — без потолка', async () => {
    expect(await slots.acquire('closed', 'a', { limit: 0, ttlMs: 60_000 })).toBe('limit');
    const open = await Promise.all(
      Array.from({ length: 10 }, (_, index) =>
        slots.acquire('open', `h${index}`, { limit: Number.POSITIVE_INFINITY, ttlMs: 60_000 }),
      ),
    );
    expect(open.every((outcome) => outcome === 'acquired')).toBe(true);
    expect(await slots.count('open')).toBe(10);
  });

  /**
   * Подсказка — срок того держателя, чьё истечение действительно открывает
   * слот: при потолке ниже числа держателей ждать надо не первого, а N-го.
   * Три держателя на 10/20/30 с: потолок 3 ждёт первого, 2 — второго, 1 —
   * третьего. Фикстура различает все три ответа (разные сроки).
   */
  it('limit отдаёт срок освобождения: N-й держатель при потолке ниже числа держателей', async () => {
    await slots.acquire('p1', 'a', { limit: 3, ttlMs: 10_000 });
    await slots.acquire('p1', 'b', { limit: 3, ttlMs: 20_000 });
    await slots.acquire('p1', 'c', { limit: 3, ttlMs: 30_000 });

    const byLimit = async (limit: number) =>
      (await slots.acquireDetailed('p1', 'd', { limit, ttlMs: 60_000 })).retryAfterMs;
    const [three, two, one] = [await byLimit(3), await byLimit(2), await byLimit(1)];

    expect(three).toBeGreaterThan(9_000);
    expect(three).toBeLessThanOrEqual(10_000);
    expect(two).toBeGreaterThan(19_000);
    expect(two).toBeLessThanOrEqual(20_000);
    expect(one).toBeGreaterThan(29_000);
    expect(one).toBeLessThanOrEqual(30_000);
    expect((await slots.reserveDetailed('p1', 'd', { limit: 2, ttlMs: 1_000 })).retryAfterMs).toBeGreaterThan(
      19_000,
    );
    // Отказ слот не занял.
    expect(await slots.count('p1')).toBe(3);
  });

  /**
   * Индекс держателя в подсказке на ВСЕХ сочетаниях: 0..4 держателей со
   * сроками 10/20/30/40 с и потолки 1..5. Отказ — ровно когда держателей не
   * меньше потолка, и подсказка — срок держателя `ZCARD − limit` (считая с
   * нуля). Ошибка индекса на единицу дала бы либо чужой срок, либо выход за
   * множество (отказ скрипта) — оба видны здесь, в отличие от точечных случаев.
   */
  it('подсказка на всех сочетаниях числа держателей и потолка — срок держателя ZCARD − limit', async () => {
    for (let held = 0; held <= 4; held++) {
      const group = `sweep-${held}`;
      for (let index = 0; index < held; index++) {
        await slots.acquire(group, `h${index}`, { limit: 10, ttlMs: (index + 1) * 10_000 });
      }
      for (let limit = 1; limit <= 5; limit++) {
        const result = await slots.acquireDetailed(group, 'probe', { limit, ttlMs: 60_000 });
        if (held < limit) {
          expect({ held, limit, result }).toEqual({
            held,
            limit,
            result: { outcome: 'acquired', retryAfterMs: null },
          });
          await slots.release(group, 'probe');
          continue;
        }
        const expected = (held - limit + 1) * 10_000;
        expect({ held, limit, outcome: result.outcome }).toEqual({ held, limit, outcome: 'limit' });
        expect(result.retryAfterMs).toBeGreaterThan(expected - 2_000);
        expect(result.retryAfterMs).toBeLessThanOrEqual(expected);
      }
    }
  });

  it('подсказки нет при acquired/held и у закрытой группы', async () => {
    expect(await slots.acquireDetailed('p1', 'a', slot)).toEqual({ outcome: 'acquired', retryAfterMs: null });
    expect(await slots.acquireDetailed('p1', 'a', slot)).toEqual({ outcome: 'held', retryAfterMs: null });
    expect(await slots.reserveDetailed('p1', 'a', slot)).toEqual({ outcome: 'held', retryAfterMs: null });
    // Закрытая группа с держателем: по сроку слот не откроется — подсказка null, а не срок держателя.
    expect(await slots.acquireDetailed('p1', 'b', { limit: 0, ttlMs: 1_000 })).toEqual({
      outcome: 'limit',
      retryAfterMs: null,
    });
  });

  it('holders: живые держатели по возрастанию срока, просроченные сняты', async () => {
    await slots.acquire('p1', 'late', { limit: 5, ttlMs: 60_000 });
    await slots.acquire('p1', 'early', { limit: 5, ttlMs: 30_000 });
    await slots.acquire('p1', 'gone', { limit: 5, ttlMs: 1 });
    await new Promise((resolve) => setTimeout(resolve, 20));

    const holders = await slots.holders('p1');

    expect(holders.map(({ holder }) => holder)).toEqual(['early', 'late']);
    expect(holders[0]?.expiresInMs).toBeGreaterThan(29_000);
    expect(holders[0]?.expiresInMs).toBeLessThanOrEqual(30_000);
    expect(holders[1]?.expiresInMs).toBeGreaterThan(59_000);
    expect(await slots.holders('empty')).toEqual([]);
  });

  it('clear снимает всех держателей группы и не трогает соседнюю', async () => {
    await slots.acquire('p1', 'a', { limit: 2, ttlMs: 60_000 });
    await slots.acquire('p1', 'b', { limit: 2, ttlMs: 60_000 });
    await slots.acquire('p2', 'a', { limit: 2, ttlMs: 60_000 });

    expect(await slots.clear('p1')).toBe(2);

    expect(await redis.exists(`${prefix}:p1`)).toBe(0);
    expect(await slots.heartbeat('p1', 'a', 60_000)).toBe(false);
    expect(await slots.count('p2')).toBe(1);
    expect(await slots.clear('p1')).toBe(0);
  });

  it('acquireMany: занимает во всех группах; отказ одной не меняет НИЧЕГО', async () => {
    await slots.acquire('tenant', 'x', { limit: 1, ttlMs: 60_000 });
    await slots.acquire('ip', 'a', { limit: 2, ttlMs: 5_000 });
    const ipDeadline = Number(await redis.zscore(`${prefix}:ip`, 'a'));

    const blocked = await slots.acquireMany(
      'a',
      [
        { group: 'project', limit: 3 },
        { group: 'ip', limit: 2 },
        { group: 'tenant', limit: 1 },
      ],
      600_000,
    );

    expect(blocked.outcome).toBe('limit');
    expect(blocked.blockedBy).toEqual(['tenant']);
    expect(blocked.retryAfterMs).toBeGreaterThan(59_000);
    // Свободная группа не получила держателя, уже занятый слот не продлён.
    expect(await redis.exists(`${prefix}:project`)).toBe(0);
    expect(Number(await redis.zscore(`${prefix}:ip`, 'a'))).toBe(ipDeadline);

    await slots.release('tenant', 'x');
    const taken = await slots.acquireMany(
      'a',
      [
        { group: 'project', limit: 3 },
        { group: 'ip', limit: 2 },
        { group: 'tenant', limit: 1 },
      ],
      600_000,
    );
    expect(taken).toEqual({ outcome: 'acquired', retryAfterMs: null, blockedBy: [] });
    expect(Number(await redis.zscore(`${prefix}:ip`, 'a'))).toBeGreaterThan(ipDeadline + 500_000);
    for (const group of ['project', 'ip', 'tenant']) {
      expect((await slots.holders(group)).map(({ holder }) => holder)).toContain('a');
    }
    expect(
      (
        await slots.acquireMany(
          'a',
          [
            { group: 'project', limit: 3 },
            { group: 'tenant', limit: 1 },
          ],
          600_000,
        )
      ).outcome,
    ).toBe('held');
  });

  it('acquireMany: подсказка — максимум по упёршимся группам, закрытая — null', async () => {
    await slots.acquire('g1', 'x', { limit: 1, ttlMs: 10_000 });
    await slots.acquire('g2', 'y', { limit: 1, ttlMs: 40_000 });

    const both = await slots.acquireMany(
      'a',
      [
        { group: 'g1', limit: 1 },
        { group: 'g2', limit: 1 },
      ],
      60_000,
    );
    expect(both.blockedBy).toEqual(['g1', 'g2']);
    expect(both.retryAfterMs).toBeGreaterThan(39_000);
    expect(both.retryAfterMs).toBeLessThanOrEqual(40_000);

    const closed = await slots.acquireMany(
      'a',
      [
        { group: 'g1', limit: 1 },
        { group: 'g3', limit: 0 },
      ],
      60_000,
    );
    expect(closed).toEqual({ outcome: 'limit', retryAfterMs: null, blockedBy: ['g1', 'g3'] });
  });

  it('acquireMany атомарен: 20 одновременных при потолках 3 и 5 — по 3 держателя в обеих', async () => {
    const groups = [
      { group: 'narrow', limit: 3 },
      { group: 'wide', limit: 5 },
    ];
    const results = await Promise.all(
      Array.from({ length: 20 }, (_, index) => slots.acquireMany(`h${index}`, groups, 60_000)),
    );
    expect(results.filter(({ outcome }) => outcome === 'acquired')).toHaveLength(3);
    expect(await slots.count('narrow')).toBe(3);
    // Отказавшие не оставили слотов в широкой группе: частичный захват дал бы 5.
    expect(await slots.count('wide')).toBe(3);
  });

  it('acquireMany: пустой список и повтор группы — отказ до Redis', async () => {
    await expect(slots.acquireMany('a', [], 1_000)).rejects.toThrow('пуст');
    await expect(
      slots.acquireMany(
        'a',
        [
          { group: 'g', limit: 1 },
          { group: 'g', limit: 2 },
        ],
        1_000,
      ),
    ).rejects.toThrow('повторяется');
    await expect(slots.acquireMany('a', [{ group: 'g', limit: -1 }], 1_000)).rejects.toThrow('limit');
    expect(await redis.keys(`${prefix}:*`)).toEqual([]);
  });

  it('onOutcome получает группу и исход каждого захвата', async () => {
    const seen: string[] = [];
    const observed = new GroupConcurrency(redis, {
      prefix,
      onOutcome: (group, outcome) => {
        seen.push(`${group}=${outcome}`);
      },
    });
    await observed.acquire('p1', 'a', { limit: 1, ttlMs: 60_000 });
    await observed.reserve('p1', 'a', { limit: 1, ttlMs: 60_000 });
    await observed.acquireDetailed('p1', 'b', { limit: 1, ttlMs: 60_000 });
    await observed.acquireMany(
      'c',
      [
        { group: 'p2', limit: 1 },
        { group: 'p1', limit: 1 },
      ],
      60_000,
    );
    await observed.acquireMany('c', [{ group: 'p2', limit: 1 }], 60_000);
    expect(seen).toEqual(['p1=acquired', 'p1=held', 'p1=limit', 'p1=limit', 'p2=acquired']);
  });

  it('падающий хук не превращает занятый слот в исключение', async () => {
    const throwing = new GroupConcurrency(redis, {
      prefix,
      onOutcome: () => {
        throw new Error('метрики недоступны');
      },
    });
    const rejecting = new GroupConcurrency(redis, {
      prefix,
      onOutcome: async () => {
        throw new Error('метрики недоступны');
      },
    });
    expect(await throwing.acquire('p1', 'a', slot)).toBe('acquired');
    expect(await rejecting.acquire('p1', 'b', slot)).toBe('acquired');
    expect((await slots.holders('p1')).map(({ holder }) => holder).sort()).toEqual(['a', 'b']);
  });

  describe('0.4.0: withSlot, хэш-тег, срок ключей acquireMany', () => {
    const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

    it('работа дольше срока слота: слот продлевается и держится всю работу, снимается после', async () => {
      const short = { limit: 1, ttlMs: 300 };
      const during: number[] = [];
      const result = await slots.withSlot('p1', 'a', short, async () => {
        for (let i = 0; i < 3; i += 1) {
          await sleep(300);
          during.push(await slots.count('p1'));
        }
        return 'готово';
      });
      expect(result).toEqual({ outcome: 'acquired', value: 'готово' });
      // Без продления слот истёк бы на 300 мс, и второй держатель прошёл бы.
      expect(during).toEqual([1, 1, 1]);
      expect(await slots.acquire('p1', 'b', short)).toBe('acquired');
      expect(await slots.holders('p1')).toEqual([expect.objectContaining({ holder: 'b' })]);
    });

    it('слот снят во время работы — сигнал работы прерван SlotLostError', async () => {
      const short = { limit: 1, ttlMs: 300 };
      let reason: unknown;
      // 0.9.0: работа заметила потерю и вернулась — вызов всё равно отказывает.
      await expect(
        slots.withSlot('p1', 'a', short, async (signal) => {
          await slots.clear('p1');
          for (let i = 0; i < 20 && !signal.aborted; i += 1) {
            await sleep(50);
          }
          reason = signal.reason;
        }),
      ).rejects.toBeInstanceOf(SlotLostError);
      expect(reason).toBeInstanceOf(SlotLostError);
    });

    it('0.9.0: работа, глухая к сигналу, доработала после потери слота — отказ SlotLostDuringWorkError со значением, а не acquired', async () => {
      const short = { limit: 1, ttlMs: 300 };
      let abortedInside: unknown;
      const settled = await slots
        .withSlot('p1', 'a', short, async (signal) => {
          await slots.clear('p1');
          // Работа не может прервать начатый вызов: сигнал не слушает.
          await sleep(400);
          abortedInside = signal.reason;
          return 'готово';
        })
        .then(
          (result) => ({ resolved: result }),
          (rejected: unknown) => ({ rejected }),
        );
      // Контроль фикстуры: потеря действительно случилась во время работы.
      expect(abortedInside).toBeInstanceOf(SlotLostError);
      expect(settled).not.toHaveProperty('resolved');
      const { rejected } = settled as { rejected: unknown };
      expect(rejected).toBeInstanceOf(SlotLostDuringWorkError);
      expect(rejected).toBeInstanceOf(SlotLostError);
      expect(rejected).toMatchObject({ group: 'p1', holder: 'a', value: 'готово' });
      expect((rejected as Error).cause).toBe(abortedInside);
    });

    it('0.9.0: слот жив всю работу — исход acquired, а не отказ (контроль к тесту потери)', async () => {
      const short = { limit: 1, ttlMs: 300 };
      await expect(
        slots.withSlot('p1', 'a', short, async () => {
          await sleep(400);
          return 'готово';
        }),
      ).resolves.toEqual({ outcome: 'acquired', value: 'готово' });
    });

    it('потолок занят — работа не начинается, ответ limit с подсказкой', async () => {
      await slots.acquire('p1', 'x', { limit: 1, ttlMs: 60_000 });
      const work = jest.fn(async () => 'нет');
      const result = await slots.withSlot('p1', 'a', { limit: 1, ttlMs: 60_000 }, work);
      expect(result).toMatchObject({ outcome: 'limit' });
      expect(work).not.toHaveBeenCalled();
    });

    it('работа бросила — слот всё равно снят, исключение наружу', async () => {
      await expect(
        slots.withSlot('p1', 'a', { limit: 1, ttlMs: 60_000 }, async () => {
          throw new Error('упало');
        }),
      ).rejects.toThrow('упало');
      expect(await slots.count('p1')).toBe(0);
    });

    it('исход held: withSlot продлевает чужой захват того же держателя, но не снимает его', async () => {
      await slots.acquire('p1', 'a', { limit: 1, ttlMs: 60_000 });
      const result = await slots.withSlot('p1', 'a', { limit: 1, ttlMs: 60_000 }, async () => 'внутри');
      expect(result).toEqual({ outcome: 'held', value: 'внутри' });
      // Слот остался за прежним захватом: снять его — дело того, кто занял.
      expect((await slots.holders('p1')).map(({ holder }) => holder)).toEqual(['a']);
      expect(await slots.acquire('p1', 'b', { limit: 1, ttlMs: 60_000 })).toBe('limit');
    });

    it('ttlMs больше предела таймера Node — отказ до Redis (иначе продление шло бы раз в 1 мс)', async () => {
      await expect(slots.acquire('p1', 'a', { limit: 1, ttlMs: 2 ** 31 })).rejects.toThrow('ttlMs');
      await expect(slots.acquire('p1', 'a', { limit: 1, ttlMs: 2 ** 31 - 1 })).resolves.toBe('acquired');
    });

    it('Redis Cluster: префикс без хэш-тега — отказ в конструкторе (явно и по клиенту)', () => {
      expect(() => new GroupConcurrency(redis, { prefix: 'app:slots', cluster: true })).toThrow('хэш-тега');
      expect(() => new GroupConcurrency(redis, { prefix: '{app}:slots', cluster: true })).not.toThrow();
      const clusterLike = Object.assign(Object.create(redis) as Redis, { isCluster: true });
      expect(() => new GroupConcurrency(clusterLike, { prefix: 'app:slots' })).toThrow('хэш-тега');
      // Контроль: обычный клиент без настройки — без требования тега.
      expect(() => new GroupConcurrency(redis, { prefix: 'app:slots' })).not.toThrow();
    });

    it('acquireMany: ключи групп со сроком — пространство ключей из внешнего ввода не копится', async () => {
      await slots.acquireMany(
        'a',
        [
          { group: 'g1', limit: 2 },
          { group: 'g2', limit: 2 },
        ],
        60_000,
      );
      for (const group of ['g1', 'g2']) {
        const pttl = await redis.pttl(`${prefix}:${group}`);
        expect(pttl).toBeGreaterThan(0);
        expect(pttl).toBeLessThanOrEqual(60_000);
      }
    });
  });

  it('пустые и неверные аргументы — отказ до обращения к Redis', async () => {
    await expect(slots.acquire('', 'a', slot)).rejects.toThrow('группа пуста');
    await expect(slots.acquire('  ', 'a', slot)).rejects.toThrow('группа пуста');
    await expect(slots.acquire('p1', ' ', slot)).rejects.toThrow('держатель пуст');
    await expect(slots.acquire('p1', 'a', { limit: 3, ttlMs: 0 })).rejects.toThrow('ttlMs');
    await expect(slots.acquire('p1', 'a', { limit: -1, ttlMs: 1_000 })).rejects.toThrow('limit');
    await expect(slots.acquire('p1', 'a', { limit: 1.5, ttlMs: 1_000 })).rejects.toThrow('limit');
    await expect(slots.acquire('p1', 'a', { limit: Number.NaN, ttlMs: 1_000 })).rejects.toThrow('limit');
    expect(() => new GroupConcurrency(redis, { prefix: ' ' })).toThrow('prefix');
    expect(await redis.keys(`${prefix}:*`)).toEqual([]);
  });
});
