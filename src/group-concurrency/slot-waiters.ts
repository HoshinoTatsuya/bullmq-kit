import type { SlotSubscriber } from './slot-subscriber.type';

/**
 * Подписки ожидающих слот: один канал на группу, одна подписка на канал на
 * процесс, сколько бы ожидающих в нём ни было. Подписка — ДО первой попытки
 * захвата: публикация между отказом и подпиской иначе терялась бы.
 */
export class SlotWaiters {
  private readonly _listeners = new Map<string, Set<(message: string) => void>>();
  private readonly _subscribed = new Map<string, Promise<unknown>>();

  public constructor(private readonly _subscriber: SlotSubscriber) {
    _subscriber.on('message', (channel, message) => {
      for (const listener of this._listeners.get(channel) ?? []) {
        listener(message);
      }
    });
  }

  /** Слушать канал; ответ — отписка (последний ушедший снимает подписку канала). */
  public async listen(channel: string, listener: (message: string) => void): Promise<() => Promise<void>> {
    let listeners = this._listeners.get(channel);
    if (listeners === undefined) {
      listeners = new Set();
      this._listeners.set(channel, listeners);
      this._subscribed.set(channel, this._subscriber.subscribe(channel));
    }
    listeners.add(listener);
    const own = listeners;
    try {
      await this._subscribed.get(channel);
    } catch (error) {
      // Отказ подписки не остаётся в кэше: следующий слушатель канала
      // подписывается заново, а не получает этот же отказ навсегда.
      own.delete(listener);
      if (own.size === 0 && this._listeners.get(channel) === own) {
        this._listeners.delete(channel);
        this._subscribed.delete(channel);
      }
      throw error;
    }
    return async () => {
      own.delete(listener);
      if (own.size === 0 && this._listeners.get(channel) === own) {
        this._listeners.delete(channel);
        this._subscribed.delete(channel);
        await this._subscriber.unsubscribe(channel).catch(() => undefined);
      }
    };
  }
}
