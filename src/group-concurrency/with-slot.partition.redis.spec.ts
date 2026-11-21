import { randomUUID } from 'node:crypto';

import Redis from 'ioredis';

import { FreezingTcpProxy } from '../testing/freezing-tcp-proxy';
import { GroupConcurrency } from './group-concurrency';
import { GROUP_CONCURRENCY_SCRIPTS } from './scripts.constant';
import { SlotLostError } from './slot-lost.error';

/**
 * `withSlot` при недоступном Redis — на НАСТОЯЩЕМ Redis через замерзающий
 * TCP-прокси: сокет открыт, байты не идут. Запрос в замёрзшую сеть не
 * отказывает ни с `maxRetriesPerRequest: null` (его требует BullMQ), ни с
 * умолчаниями ioredis — промис продления просто не завершается. Прежде
 * потеря слота проверялась только в ветке ОТКАЗА продления, и держатель A
 * работал дальше, когда слот уже занял B: два держателя при потолке 1.
 *
 * Утверждения — про ПОРЯДОК событий («A заметил потерю» раньше «B занял
 * слот»), а не про длительности: B занимает слот ровно тогда, когда срок A
 * истёк в Redis, так что порядок и есть «потеря замечена до истечения срока».
 */
const url = process.env.REDIS_TEST_URL;
const describeRedis = url ? describe : describe.skip;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Исход промиса, но не позже `ms`: зависший withSlot — красный тест, а не зависший прогон.
 * Таймер срока снимается, как только исход есть: иначе он держал процесс до `ms` после
 * теста, и воркер jest в конце прогона снимался принудительно.
 */
const within = async <T>(promise: Promise<T>, ms: number): Promise<T | string> => {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<string>((resolve) => {
        timer = setTimeout(() => resolve(`не завершился за ${ms} мс`), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
};

describeRedis('withSlot при замёрзшей сети [нужен REDIS_TEST_URL]', () => {
  let direct: Redis;
  let proxy: FreezingTcpProxy;
  let prefix: string;
  const slot = { limit: 1, ttlMs: 3_000 };

  beforeAll(() => {
    direct = new Redis(url as string, { maxRetriesPerRequest: null });
  });
  afterAll(async () => {
    await direct.quit();
  });
  beforeEach(async () => {
    const { hostname, port } = new URL(url as string);
    proxy = await FreezingTcpProxy.start(hostname, Number(port));
    prefix = `gc-${randomUUID()}`;
  });
  afterEach(async () => {
    proxy.thaw();
    await proxy.close();
  });

  /** Клиент через прокси со счётом продлений, отправленных после `mark()`. */
  // Свой тип настроек, а не RedisOptions: под exactOptionalPropertyTypes
  // RedisOptions ioredis 6 не принимает его же конструктор (`replyMapping`).
  const throughProxy = (options: { readonly maxRetriesPerRequest?: null }) => {
    const client = new Redis(proxy.port, '127.0.0.1', options);
    const counter = { marked: false, heartbeats: 0 };
    const counted = new Proxy(client, {
      get(target, property, receiver) {
        if (property === 'eval') {
          return (script: string, ...args: (string | number)[]) => {
            if (counter.marked && script === GROUP_CONCURRENCY_SCRIPTS.heartbeat) {
              counter.heartbeats += 1;
            }
            return target.eval(script, args.length > 0 ? (args[0] as number) : 0, ...args.slice(1));
          };
        }
        const value = Reflect.get(target, property, receiver);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    return { client, counted, counter };
  };

  /** Работа, которую заканчивает только сигнал: событие потери — в общий журнал. */
  const untilAborted = (events: string[], who: string) => (signal: AbortSignal) =>
    new Promise<string>((resolve) => {
      signal.addEventListener('abort', () => {
        events.push(signal.reason instanceof SlotLostError ? `${who} потерял слот` : `${who} прерван не тем`);
        resolve('прервана');
      });
    });

  const takeWhenFree = async (slots: GroupConcurrency, holder: string, events: string[]) => {
    for (let i = 0; i < 200; i += 1) {
      if ((await slots.acquire('g', holder, slot)) !== 'limit') {
        events.push(`${holder} занял слот`);
        return;
      }
      await sleep(25);
    }
  };

  it.each([
    ['maxRetriesPerRequest: null (как требует BullMQ)', { maxRetriesPerRequest: null }],
    ['умолчания ioredis', {}],
  ] as const)(
    '%s: потеря замечена раньше, чем слот занял другой; продления не копятся',
    async (_name, options) => {
      const { client, counted, counter } = throughProxy(options);
      try {
        const a = new GroupConcurrency(counted, { prefix });
        const b = new GroupConcurrency(direct, { prefix });
        const events: string[] = [];
        const finished = a.withSlot('g', 'A', slot, untilAborted(events, 'A'));
        // Отказ (0.9.0) приходит раньше, чем на него подпишется проверка ниже: помечен обработанным сразу.
        finished.catch(() => undefined);
        for (let i = 0; i < 100 && (await b.count('g')) === 0; i += 1) {
          await sleep(10);
        }
        // Контроль фикстуры: A действительно держит слот до замерзания.
        expect(await b.count('g')).toBe(1);
        proxy.freeze();
        counter.marked = true;

        await takeWhenFree(b, 'B', events);

        expect(events).toEqual(['A потерял слот', 'B занял слот']);
        // Продление не отправляется, пока висит прежнее: очередь команд не растёт.
        expect(counter.heartbeats).toBeLessThanOrEqual(1);
        // Снятие слота в замёрзшую сеть не держит withSlot вечно: оно ограничено сроком слота.
        // 0.9.0: работа вернулась после потери — вызов отказывает, значение в `value`.
        await expect(within(finished, 2 * slot.ttlMs)).rejects.toMatchObject({
          name: 'SlotLostDuringWorkError',
          value: 'прервана',
        });
      } finally {
        client.disconnect();
      }
    },
    30_000,
  );

  it('продление отказывает (ошибка Redis) — слот потерян раньше, чем его занял другой', async () => {
    let failing = false;
    const flaky = new Proxy(direct, {
      get(target, property, receiver) {
        if (property === 'eval') {
          return (script: string, ...args: (string | number)[]) =>
            failing && script === GROUP_CONCURRENCY_SCRIPTS.heartbeat
              ? Promise.reject(new Error('READONLY You can not write against a read only replica'))
              : target.eval(script, args[0] as number, ...args.slice(1));
        }
        const value = Reflect.get(target, property, receiver);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    const a = new GroupConcurrency(flaky, { prefix });
    const b = new GroupConcurrency(direct, { prefix });
    const events: string[] = [];
    const finished = a.withSlot('g', 'A', slot, untilAborted(events, 'A'));
    // Отказ (0.9.0) приходит раньше, чем на него подпишется проверка ниже: помечен обработанным сразу.
    finished.catch(() => undefined);
    for (let i = 0; i < 100 && (await b.count('g')) === 0; i += 1) {
      await sleep(10);
    }
    failing = true;

    await takeWhenFree(b, 'B', events);

    expect(events).toEqual(['A потерял слот', 'B занял слот']);
    await expect(within(finished, 2 * slot.ttlMs)).rejects.toMatchObject({
      name: 'SlotLostDuringWorkError',
      value: 'прервана',
    });
  }, 30_000);

  it('единичный отказ продления слот не теряет: следующее подтверждение продлевает отсчёт', async () => {
    let failures = 1;
    const flaky = new Proxy(direct, {
      get(target, property, receiver) {
        if (property === 'eval') {
          return (script: string, ...args: (string | number)[]) => {
            if (failures > 0 && script === GROUP_CONCURRENCY_SCRIPTS.heartbeat) {
              failures -= 1;
              return Promise.reject(new Error('соединение сброшено'));
            }
            return target.eval(script, args[0] as number, ...args.slice(1));
          };
        }
        const value = Reflect.get(target, property, receiver);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    const a = new GroupConcurrency(flaky, { prefix });
    const short = { limit: 1, ttlMs: 600 };
    const result = await a.withSlot('g', 'A', short, async (signal) => {
      // Четыре срока слота: без продления отсчёта слот считался бы потерянным.
      for (let i = 0; i < 12 && !signal.aborted; i += 1) {
        await sleep(200);
      }
      return signal.aborted ? 'прервана' : 'доработала';
    });
    // Контроль фикстуры: отказ действительно случился.
    expect(failures).toBe(0);
    expect(result).toEqual({ outcome: 'acquired', value: 'доработала' });
  }, 30_000);
});
