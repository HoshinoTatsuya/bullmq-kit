/** Настройки темпа групп. */
export interface GroupRateLimitOptions {
  /**
   * Префикс ключей ведер: `<prefix>:<группа>`. Свой у каждой системы. В Redis
   * Cluster ключ одного вызова — один, хэш-тег не обязателен.
   */
  readonly prefix: string;
}
