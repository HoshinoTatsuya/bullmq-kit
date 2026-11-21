import { GroupConcurrency } from './group-concurrency';
import type { GroupConcurrencyRedis } from './group-concurrency-redis.type';
import { redisHashTag } from './redis-hash-tag';

/**
 * Без Redis: проверка ФОРМЫ ответов скриптов и правило хэш-тега. Подменённый
 * ответ — то, что вернул бы скрипт после неудачной правки или чужой прокси:
 * обязан быть отказом с причиной, а не тихо прочитанным исходом.
 */
describe('GroupConcurrency: форма ответов скриптов', () => {
  const answering = (reply: unknown): GroupConcurrencyRedis => ({
    eval: async () => reply,
  });
  const slot = { limit: 1, ttlMs: 1_000 };

  it.each([
    ['не массив', 'x'],
    ['массив не из двух', [1]],
    ['незнакомый код исхода', [9, -1]],
  ])('захват: %s — отказ «неожиданный ответ»', async (_case, reply) => {
    await expect(
      new GroupConcurrency(answering(reply), { prefix: 'p' }).acquireDetailed('g', 'h', slot),
    ).rejects.toThrow('неожиданный ответ скрипта захвата');
  });

  it('подсказка — только при limit: при acquired она null, даже если скрипт её вернул', async () => {
    expect(
      await new GroupConcurrency(answering([1, 5_000]), { prefix: 'p' }).acquireDetailed('g', 'h', slot),
    ).toEqual({
      outcome: 'acquired',
      retryAfterMs: null,
    });
    expect(
      await new GroupConcurrency(answering([0, 5_000]), { prefix: 'p' }).acquireDetailed('g', 'h', slot),
    ).toEqual({
      outcome: 'limit',
      retryAfterMs: 5_000,
    });
    expect(
      await new GroupConcurrency(answering([0, -1]), { prefix: 'p' }).acquireDetailed('g', 'h', slot),
    ).toEqual({
      outcome: 'limit',
      retryAfterMs: null,
    });
  });

  it.each([
    ['не массив', 'x', 'неожиданный ответ скрипта захвата групп'],
    ['не из трёх', [1, -1], 'неожиданный ответ скрипта захвата групп'],
    ['коды не массив', [1, -1, 'x'], 'неожиданный ответ скрипта захвата групп'],
    ['кодов меньше групп', [1, -1, [1]], 'кодов 1 на 2 групп'],
  ])('acquireMany: %s — отказ', async (_case, reply, message) => {
    const groups = [
      { group: 'a', limit: 1 },
      { group: 'b', limit: 1 },
    ];
    await expect(
      new GroupConcurrency(answering(reply), { prefix: 'p' }).acquireMany('h', groups, 1_000),
    ).rejects.toThrow(message);
  });

  it.each([
    ['не массив', 'x'],
    ['нечётная длина', ['a', 1, 'b']],
  ])('holders: %s — отказ', async (_case, reply) => {
    await expect(new GroupConcurrency(answering(reply), { prefix: 'p' }).holders('g')).rejects.toThrow(
      'неожиданный ответ скрипта держателей',
    );
  });
});

describe('хэш-тег префикса по правилу Redis Cluster', () => {
  const stub: GroupConcurrencyRedis = { eval: async () => [1, -1] };

  it.each([
    ['{app}:slots', 'app'],
    ['{a}:slots', 'a'],
    ['x{app}y', 'app'],
    ['{app}{other}', 'app'],
    ['app:slots', null],
    ['{}:slots', null],
    ['app{}{t}', null],
    ['{}x{t}', null],
    ['app{x', null],
    ['app}x{', null],
  ] as const)('%s → тег %p', (prefix, tag) => {
    expect(redisHashTag(prefix)).toBe(tag);
    const make = () => new GroupConcurrency(stub, { prefix, cluster: true });
    if (tag === null) {
      expect(make).toThrow('хэш-тега');
    } else {
      expect(make).not.toThrow();
    }
  });
});

describe('хэш-тег с keyPrefix клиента ioredis', () => {
  it('тег в keyPrefix клиента засчитывается: ioredis ставит его перед префиксом', () => {
    const tagged = { eval: async () => [1, -1], options: { keyPrefix: '{kp}:' } };
    const untagged = { eval: async () => [1, -1], options: { keyPrefix: 'kp:' } };
    expect(() => new GroupConcurrency(tagged, { prefix: 'slots', cluster: true })).not.toThrow();
    expect(() => new GroupConcurrency(untagged, { prefix: 'slots', cluster: true })).toThrow('хэш-тега');
  });
});
