/**
 * Текст отказа для отчёта и журнала, который сам не бросает. Обычное значение —
 * как `String(value)` (у ошибки — «Имя: сообщение»). Но `String()` на значении
 * без прототипа (`Object.create(null)`) или с бросающим `toString` /
 * `Symbol.toPrimitive` бросает TypeError — и ветка, которая разбирает отказ,
 * роняла бы весь прогон. Тогда — `message`, если это строка, иначе тег объекта.
 */
export function errorText(value: unknown): string {
  try {
    return String(value);
  } catch {
    try {
      const message = (value as { message?: unknown } | null)?.message;
      return typeof message === 'string' ? message : Object.prototype.toString.call(value);
    } catch {
      return '[значение отказа без текста]';
    }
  }
}
