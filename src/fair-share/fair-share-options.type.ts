/** Настройки общего пула со справедливой долей. */
export interface FairShareOptions {
  /** Префикс ключей: `<prefix>:<пул>` и служебные `<prefix>~w:<пул>`, `<prefix>~x:<пул>`. */
  readonly prefix: string;
  /** Redis Cluster: префикс обязан нести хэш-тег (три ключа одного скрипта). По умолчанию — по клиенту. */
  readonly cluster?: boolean;
}
