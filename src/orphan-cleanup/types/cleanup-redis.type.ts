/** Часть пайплайна ioredis, которая нужна уборке. `evalsha` — если есть, скрипты идут по SHA. */
export interface CleanupPipeline {
  eval(script: string, numKeys: number, ...args: (string | number)[]): CleanupPipeline;
  evalsha?(sha: string, numKeys: number, ...args: (string | number)[]): CleanupPipeline;
  exec(): Promise<Array<[Error | null, unknown]> | null>;
}

/** Клиент, которым обходится пространство ключей (в кластере — узел). */
export interface CleanupScanner {
  scan(
    cursor: string,
    match: 'MATCH',
    pattern: string,
    count: 'COUNT',
    n: number,
  ): Promise<[string, string[]]>;
}

export interface CleanupRedis extends CleanupScanner {
  lrange(key: string, start: number, stop: number): Promise<string[]>;
  /**
   * `stop` — строкой: в ioredis 6 у `zrange` он числа не принимает (`string | Buffer`),
   * в ioredis 5 — принимает и строку, и число. Числовой `stop` в порте давал
   * TS2345 у каждого потребителя на ioredis 6 при работающем рантайме.
   */
  zrange(key: string, start: number, stop: string): Promise<string[]>;
  zcard(key: string): Promise<number>;
  /** Длина списка статуса — для потолка снимка `maxSnapshotListSize` (по умолчанию конечного). */
  llen?(key: string): Promise<number>;
  eval(script: string, numKeys: number, ...args: (string | number)[]): Promise<unknown>;
  /** Если есть — скрипты идут по SHA (текст скрипта не шлётся на каждый вызов), NOSCRIPT — повтор через eval. */
  evalsha?(sha: string, numKeys: number, ...args: (string | number)[]): Promise<unknown>;
  pipeline(): CleanupPipeline;
}
