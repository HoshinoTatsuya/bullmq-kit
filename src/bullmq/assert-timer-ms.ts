import { WITH_SLOT } from '../group-concurrency/with-slot.constant';

/**
 * Длительность для таймера Node: число от 0 до 2^31 − 1. Больше Node молча
 * заменяет на 1 мс (TimeoutOverflowWarning), NaN — тоже: «дедлайн в месяц»
 * прерывал бы задачи сразу, а отложенная «в прошлое» задача возвращалась бы
 * этому же Worker по кругу.
 */
export function assertTimerMs(name: string, value: number): void {
  if (!Number.isFinite(value) || value < 0 || value > WITH_SLOT.MAX_TTL_MS) {
    throw new Error(
      `${name} — число от 0 до ${WITH_SLOT.MAX_TTL_MS} мс (предел таймера Node), получено ${value}`,
    );
  }
}
