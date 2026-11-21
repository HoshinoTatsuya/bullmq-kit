/** Сравнение числовых частей версии по длине `right`: < 0 — `left` младше, 0 — равны, > 0 — старше. */
export function compareVersionParts(left: readonly number[], right: readonly number[]): number {
  for (let i = 0; i < right.length; i += 1) {
    const difference = (left[i] as number) - (right[i] as number);
    if (difference !== 0) {
      return difference;
    }
  }
  return 0;
}
