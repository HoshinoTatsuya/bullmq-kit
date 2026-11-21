/**
 * Темп группы: не больше `limit` взятий за `intervalMs`, с запасом-всплеском до
 * `limit` (ведро токенов ёмкостью `limit`, наполняется равномерно). «60 в
 * минуту» — `{ limit: 60, intervalMs: 60_000 }`: после простоя можно сразу 60,
 * дальше — по одному в секунду.
 */
export interface Rate {
  readonly limit: number;
  readonly intervalMs: number;
}
