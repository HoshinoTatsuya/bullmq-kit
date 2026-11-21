import type { AcquireOutcome } from './acquire-outcome.type';
import type { SlotSubscriber } from './slot-subscriber.type';

/** Настройки потолка групп. */
export interface GroupConcurrencyOptions {
  /**
   * Префикс ключей: `<prefix>:<группа>`. Свой у каждой системы, даже на разных
   * инстансах Redis: один инстанс — вопрос времени, а совпадение ключей значит
   * «чужие держатели занимают слоты системы». Для `acquireMany` в Redis Cluster
   * префикс обязан нести хэш-тег (`{app}:slots`): ключи одного скрипта
   * должны лежать в одном слоте, иначе Redis ответит `CROSSSLOT`.
   */
  readonly prefix: string;
  /**
   * Хук исхода каждого захвата (`acquire`, `reserve`, их `Detailed`-версии,
   * `acquireMany`) — для метрик. Вызывается ПОСЛЕ ответа Redis; исключение и
   * отклонённый промис хука глотаются: иначе слот, уже занятый в Redis, остался
   * бы у вызывающего, который получил исключение и не знает, что держит его.
   * У `acquireMany` при успехе — по разу на группу с её исходом, при отказе —
   * по разу на каждую упёршуюся группу с `limit`.
   */
  readonly onOutcome?: (group: string, outcome: AcquireOutcome) => unknown;
  /**
   * Redis Cluster: префикс обязан нести хэш-тег `{…}`, иначе отказ в
   * конструкторе. По умолчанию — по клиенту (`isCluster` у ioredis Cluster).
   */
  readonly cluster?: boolean;
  /**
   * Соединение-подписчик для ожидания слота (`acquireWaiting`, `withSlot(...,
   * { timeoutMs })`): отдельное соединение (`redis.duplicate()`). Без него
   * ожидание — отказ с причиной; остальное работает и так.
   */
  readonly subscriber?: SlotSubscriber;
}
