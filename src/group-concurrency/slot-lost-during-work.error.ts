import { SlotLostError } from './slot-lost.error';

/**
 * Работа `withSlot` вернула значение, но слот был потерян во время неё: в
 * группу уже могли пройти другие, то есть потолок мог быть превышен, пока
 * работа доделывала своё. Прежде вызов отвечал `acquired` — тихий успех,
 * неотличимый от работы под слотом.
 *
 * Подкласс `SlotLostError`: обработчик потери ловит оба случая одной
 * проверкой. Значение работы не теряется — `value`; исходная потеря, которую
 * видел сигнал работы, — `cause`.
 */
export class SlotLostDuringWorkError extends SlotLostError {
  public readonly value: unknown;

  public constructor(lost: SlotLostError, value: unknown) {
    super(
      lost.group,
      lost.holder,
      `${lost.reason}; работа доработала после потери и вернула значение (value)`,
      {
        cause: lost,
      },
    );
    this.name = SlotLostDuringWorkError.name;
    this.value = value;
  }
}
