/**
 * Слот держателя потерян во время работы: продление ответило «слота нет»
 * (снят `clear`, истёк) или не проходило дольше срока слота. В группу уже
 * могли пройти другие — работа прерывается сигналом, чтобы потолок не
 * превышался дольше, чем она успеет заметить.
 */
export class SlotLostError extends Error {
  public constructor(
    public readonly group: string,
    public readonly holder: string,
    /** Почему слот считается потерянным (ответ продления или дедлайн). */
    public readonly reason: string,
    options?: ErrorOptions,
  ) {
    super(`слот группы ${group} держателя ${holder} потерян: ${reason}`, options);
    this.name = SlotLostError.name;
  }
}
