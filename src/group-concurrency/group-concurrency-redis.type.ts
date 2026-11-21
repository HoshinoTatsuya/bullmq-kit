/** Подмножество клиента ioredis, которое нужно потолку групп. */
export interface GroupConcurrencyRedis {
  eval(script: string, numKeys: number, ...args: (string | number)[]): Promise<unknown>;
}
