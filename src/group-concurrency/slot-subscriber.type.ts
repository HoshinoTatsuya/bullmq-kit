/**
 * Соединение-подписчик для ожидания слота (`acquireWaiting`): ОТДЕЛЬНОЕ
 * соединение ioredis (`redis.duplicate()`) — в режиме подписки соединение
 * других команд не принимает. Подходит сам клиент ioredis.
 */
export interface SlotSubscriber {
  subscribe(...channels: string[]): Promise<unknown>;
  unsubscribe(...channels: string[]): Promise<unknown>;
  on(event: 'message', listener: (channel: string, message: string) => void): unknown;
}
