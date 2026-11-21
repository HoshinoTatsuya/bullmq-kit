import { DelayedError, UnrecoverableError } from 'bullmq';

import type { AcquireResult } from '../group-concurrency/acquire-result.type';
import { acquireOrDelay } from './acquire-or-delay';
import { DELAY_ON_LIMIT_DEFAULTS } from './delay-on-limit.constant';

/** Без Redis: арифметика задержки и порядок «moveToDelayed, затем DelayedError». */
describe('acquireOrDelay', () => {
  const NOW = 1_000_000;
  const job = () => {
    const calls: Array<{ timestamp: number; token: string | undefined }> = [];
    return {
      calls,
      moveToDelayed: async (timestamp: number, token?: string) => {
        calls.push({ timestamp, token });
      },
    };
  };
  const limit = (retryAfterMs: number | null) => async (): Promise<AcquireResult> => ({
    outcome: 'limit',
    retryAfterMs,
  });
  const fixed = { now: () => NOW, random: () => 0 };

  it.each(['acquired', 'held'] as const)('%s — задача не трогается, исход возвращается', async (outcome) => {
    const target = job();
    await expect(
      acquireOrDelay(target, 'tok', async () => ({ outcome, retryAfterMs: null }), fixed),
    ).resolves.toBe(outcome);
    expect(target.calls).toEqual([]);
  });

  it('limit — первая отсрочка на minDelayMs, а не на подсказку (подсказка — худший случай), затем DelayedError', async () => {
    const target = job();
    const error = await acquireOrDelay(target, 'tok', limit(30_000), fixed).catch(
      (caught: unknown) => caught,
    );
    expect(error).toBeInstanceOf(DelayedError);
    expect(target.calls).toEqual([{ timestamp: NOW + DELAY_ON_LIMIT_DEFAULTS.MIN_DELAY_MS, token: 'tok' }]);
  });

  /**
   * Показатель — число уже сделанных отсрочек: `attemptsStarted − attemptsMade − 1`
   * (каждая отсрочка — новый старт, попытку она не тратит). Потолок — меньшее из
   * `maxDelayMs` и подсказки; закрытая группа — `closedGroupDelayMs`.
   */
  it.each([
    // [attemptsStarted, attemptsMade, подсказка, задержка]
    [1, 0, 30_000, 100],
    [2, 0, 30_000, 200],
    [4, 0, 30_000, 800],
    [5, 1, 30_000, 800],
    [12, 0, 30_000, 30_000],
    [40, 0, 10 * 60_000, DELAY_ON_LIMIT_DEFAULTS.MAX_DELAY_MS],
    [3, 0, 1, DELAY_ON_LIMIT_DEFAULTS.MIN_DELAY_MS],
    [40, 0, null, DELAY_ON_LIMIT_DEFAULTS.CLOSED_GROUP_DELAY_MS],
  ] as const)('стартов %p, провалов %p, подсказка %p → %p мс', async (started, made, hint, expected) => {
    const target = { ...job(), attemptsStarted: started, attemptsMade: made };
    await acquireOrDelay(target, 'tok', limit(hint), fixed).catch(() => undefined);
    expect((target.calls[0]?.timestamp ?? 0) - NOW).toBe(expected);
  });

  it('без attemptsStarted — задержка первой отсрочки; закрытая группа — своя задержка как потолок', async () => {
    const custom = { ...job(), attemptsStarted: 30 };
    await acquireOrDelay(custom, 'tok', limit(null), { ...fixed, closedGroupDelayMs: 7_000 }).catch(
      () => undefined,
    );
    expect(custom.calls[0]?.timestamp).toBe(NOW + 7_000);
    const bare = job();
    await acquireOrDelay(bare, 'tok', limit(null), fixed).catch(() => undefined);
    expect(bare.calls[0]?.timestamp).toBe(NOW + DELAY_ON_LIMIT_DEFAULTS.MIN_DELAY_MS);
  });

  it('добавка: random × max(jitterMs, задержка/4) — разброс растёт с задержкой', async () => {
    const small = job();
    await acquireOrDelay(small, 'tok', limit(2_000), {
      now: () => NOW,
      random: () => 0.5,
      jitterMs: 400,
    }).catch(() => undefined);
    // 100 мс: разброс — jitterMs (400), половина — 200.
    expect(small.calls[0]?.timestamp).toBe(NOW + 100 + 200);
    const large = { ...job(), attemptsStarted: 8 };
    await acquireOrDelay(large, 'tok', limit(60_000), {
      now: () => NOW,
      random: () => 0.5,
      jitterMs: 400,
    }).catch(() => undefined);
    // 100 · 2⁷ = 12 800 мс: разброс — четверть (3 200), половина — 1 600.
    expect(large.calls[0]?.timestamp).toBe(NOW + 12_800 + 1_600);
  });

  it('token пуст или не задан — отказ ДО захвата: BullMQ подставил бы «0» и отложил без проверки лока', async () => {
    const acquire = jest.fn(limit(1_000));
    await expect(acquireOrDelay(job(), undefined, acquire, fixed)).rejects.toThrow('token');
    await expect(acquireOrDelay(job(), '', acquire, fixed)).rejects.toThrow('token');
    expect(acquire).not.toHaveBeenCalled();
  });

  it('неверные настройки — отказ ДО захвата: иначе занятый слот остался бы без задачи', async () => {
    let acquired = 0;
    const acquire = async (): Promise<AcquireResult> => {
      acquired += 1;
      return { outcome: 'acquired', retryAfterMs: null };
    };
    await expect(acquireOrDelay(job(), 'tok', acquire, { minDelayMs: 10, maxDelayMs: 5 })).rejects.toThrow(
      'minDelayMs',
    );
    await expect(acquireOrDelay(job(), 'tok', acquire, { jitterMs: -1 })).rejects.toThrow('jitterMs');
    await expect(acquireOrDelay(job(), 'tok', acquire, { jitterFraction: 1.5 })).rejects.toThrow(
      'jitterFraction',
    );
    await expect(acquireOrDelay(job(), 'tok', acquire, { jitterFraction: Number.NaN })).rejects.toThrow(
      'jitterFraction',
    );
    await expect(acquireOrDelay(job(), 'tok', acquire, { maxDelayMs: 1.5 })).rejects.toThrow('maxDelayMs');
    expect(acquired).toBe(0);
  });

  it('точная подсказка (темп GroupRateLimit) — отсрочка ровно на неё, а не по экспоненте', async () => {
    const target = job();
    const exact = async (): Promise<AcquireResult> => ({
      outcome: 'limit',
      retryAfterMs: 5_000,
      exact: true,
    });
    await acquireOrDelay(target, 'tok', exact, fixed).catch(() => undefined);
    expect(target.calls[0]?.timestamp).toBe(NOW + 5_000);
    const clamped = job();
    const huge = async (): Promise<AcquireResult> => ({
      outcome: 'limit',
      retryAfterMs: 10 * 60_000,
      exact: true,
    });
    await acquireOrDelay(clamped, 'tok', huge, fixed).catch(() => undefined);
    expect(clamped.calls[0]?.timestamp).toBe(NOW + DELAY_ON_LIMIT_DEFAULTS.MAX_DELAY_MS);
  });

  it('minDelayMs = maxDelayMs — допустимо (граница проверки «min больше max»)', async () => {
    const target = job();
    await acquireOrDelay(target, 'tok', limit(30_000), { ...fixed, minDelayMs: 500, maxDelayMs: 500 }).catch(
      () => undefined,
    );
    expect(target.calls[0]?.timestamp).toBe(NOW + 500);
  });

  it('отказ moveToDelayed (лок потерян) пробрасывается как есть, а не как DelayedError', async () => {
    const lost = new Error('Missing lock for job');
    const target = {
      moveToDelayed: async () => {
        throw lost;
      },
    };
    await expect(acquireOrDelay(target, 'tok', limit(1_000), fixed)).rejects.toBe(lost);
  });

  describe('maxTotalWaitMs', () => {
    const aged = (timestamp: number | undefined) => {
      const calls: number[] = [];
      return {
        calls,
        ...(timestamp === undefined ? {} : { timestamp }),
        moveToDelayed: async (at: number) => {
          calls.push(at);
        },
      };
    };

    it('ждёт дольше потолка с создания — UnrecoverableError, в delayed не уходит', async () => {
      const target = aged(NOW - 10_000);
      const error = await acquireOrDelay(target, 'tok', limit(null), {
        ...fixed,
        maxTotalWaitMs: 10_000,
      }).catch((caught: unknown) => caught);
      expect(error).toBeInstanceOf(UnrecoverableError);
      expect(target.calls).toEqual([]);
    });

    it('в пределах потолка — обычная отсрочка', async () => {
      const target = aged(NOW - 9_999);
      await expect(
        acquireOrDelay(target, 'tok', limit(null), { ...fixed, maxTotalWaitMs: 10_000 }),
      ).rejects.toBeInstanceOf(DelayedError);
      expect(target.calls).toHaveLength(1);
    });

    it('потолок задан, а timestamp у задачи нет — отказ с причиной', async () => {
      await expect(
        acquireOrDelay(aged(undefined), 'tok', limit(null), { ...fixed, maxTotalWaitMs: 10_000 }),
      ).rejects.toThrow('timestamp');
    });

    it('неверный потолок — отказ до захвата', async () => {
      const acquire = jest.fn(limit(null));
      await expect(acquireOrDelay(aged(NOW), 'tok', acquire, { maxTotalWaitMs: 0 })).rejects.toThrow(
        'maxTotalWaitMs',
      );
      expect(acquire).not.toHaveBeenCalled();
    });
  });
});
