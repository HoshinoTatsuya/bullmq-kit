/**
 * Хэш-тег ключа по правилу Redis Cluster: от ПЕРВОЙ `{` до первой `}` после
 * неё; пусто между ними или `}` нет — тега нет, и хэшируется весь ключ.
 * Поэтому `app{}{t}` и `{}x{t}` тега не несут, хотя `{t}` в них есть: регулярка
 * «есть непустое `{…}`» принимала их, а Redis клал бы ключи групп в разные
 * слоты — `CROSSSLOT` на первом `acquireMany` в проде.
 */
export function redisHashTag(key: string): string | null {
  const open = key.indexOf('{');
  if (open === -1) {
    return null;
  }
  const close = key.indexOf('}', open + 1);
  if (close === -1 || close === open + 1) {
    return null;
  }
  return key.slice(open + 1, close);
}
