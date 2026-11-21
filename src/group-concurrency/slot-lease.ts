import { performance } from 'node:perf_hooks';

import { WITH_SLOT } from './with-slot.constant';

/**
 * Удержание слота во время работы `withSlot`: продление раз в `ttlMs / 3` и
 * дедлайн потери от времени ОТПРАВКИ последнего подтверждённого продления.
 *
 * - Дедлайн — свой таймер, а не проверка в ответе продления: запрос в
 *   замёрзшую сеть не отвечает ни успехом, ни отказом, и ветка «отказ» не
 *   исполнялась бы никогда. Проверка повторяется и в каждом тике.
 * - Отсчёт — от отправки: срок в Redis поставлен в момент исполнения, а оно
 *   не раньше отправки; время ответа опаздывает на RTT в небезопасную сторону.
 * - Single-flight: новое продление не уходит, пока висит прежнее, иначе
 *   офлайн-очередь клиента копила бы их и выпустила залпом после восстановления.
 * - Часы — монотонные (`performance.now()`): перевод системных часов не
 *   продлевает и не обрывает слот.
 */
export class SlotLease {
  private _confirmedSentAt: number;
  private _inFlight = false;
  private _stopped = false;
  private _timer: NodeJS.Timeout | undefined;
  private _deadline: NodeJS.Timeout | undefined;
  private readonly _margin: number;

  public constructor(
    private readonly _ttlMs: number,
    acquireSentAt: number,
    private readonly _heartbeat: () => Promise<boolean>,
    private readonly _onLost: (reason: string) => void,
  ) {
    this._confirmedSentAt = acquireSentAt;
    this._margin = Math.max(1, Math.floor(_ttlMs / WITH_SLOT.LOSS_MARGIN_FRACTION));
  }

  /** Момент для отметки отправки захвата — до вызова Redis. */
  public static now(): number {
    return performance.now();
  }

  public start(): void {
    this._timer = setInterval(
      () => this._tick(),
      Math.max(1, Math.floor(this._ttlMs / WITH_SLOT.HEARTBEAT_FRACTION)),
    );
    this._timer.unref();
    this._armDeadline();
  }

  public stop(): void {
    this._stopped = true;
    clearInterval(this._timer);
    clearTimeout(this._deadline);
  }

  private _tick(): void {
    if (this._expired() || this._inFlight) {
      return;
    }
    this._inFlight = true;
    const sentAt = performance.now();
    this._heartbeat().then(
      (alive) => {
        this._inFlight = false;
        if (this._stopped) {
          return;
        }
        if (!alive) {
          this._lose('продление ответило «слота нет» (снят или истёк)');
          return;
        }
        if (sentAt > this._confirmedSentAt) {
          this._confirmedSentAt = sentAt;
          this._armDeadline();
        }
      },
      () => {
        // Отказ продления — не потеря: слот жив до дедлайна от последнего
        // подтверждённого. Следующий тик попробует снова.
        this._inFlight = false;
        this._expired();
      },
    );
  }

  /** Истёк ли запас срока; истёк — слот потерян (и это объявлено). */
  private _expired(): boolean {
    if (this._stopped) {
      return true;
    }
    const left = this._confirmedSentAt + this._ttlMs - this._margin - performance.now();
    if (left > 0) {
      return false;
    }
    this._lose(
      `продление не подтверждено ${this._ttlMs - this._margin} мс с отправки последнего подтверждённого ` +
        `(срок слота ${this._ttlMs} мс): Redis недоступен или не отвечает`,
    );
    return true;
  }

  private _armDeadline(): void {
    clearTimeout(this._deadline);
    const left = this._confirmedSentAt + this._ttlMs - this._margin - performance.now();
    // Таймер бывает раньше монотонных часов на долю миллисекунды: не истёк — взвести снова.
    this._deadline = setTimeout(
      () => {
        if (!this._expired()) {
          this._armDeadline();
        }
      },
      Math.max(0, left),
    );
    this._deadline.unref();
  }

  private _lose(reason: string): void {
    if (this._stopped) {
      return;
    }
    this.stop();
    this._onLost(reason);
  }
}
