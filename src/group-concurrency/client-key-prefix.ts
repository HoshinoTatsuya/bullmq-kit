/**
 * `keyPrefix` клиента ioredis (и `Redis`, и `Cluster` держат его в
 * `options.keyPrefix`): ioredis дописывает его к ключам команд и к `KEYS`
 * скриптов, но не к ключам, которые скрипт собирает сам. Пустая строка — нет.
 */
export function clientKeyPrefix(client: unknown): string {
  const prefix = (client as { options?: { keyPrefix?: unknown } } | null)?.options?.keyPrefix;
  return typeof prefix === 'string' ? prefix : '';
}
