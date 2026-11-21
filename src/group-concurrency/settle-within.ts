/**
 * Дождаться промиса, но не дольше `ms`: исход после срока не нужен. Отказ и
 * таймаут — не исключение: так снимают то, что истечёт само (слот по сроку),
 * и недоступный Redis не должен держать вызывающего. Таймер не держит процесс.
 */
export async function settleWithin(promise: Promise<unknown>, ms: number): Promise<void> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<void>((resolve) => {
    timer = setTimeout(resolve, ms);
    timer.unref();
  });
  try {
    await Promise.race([promise.then(undefined, () => undefined), timeout]);
  } finally {
    clearTimeout(timer);
  }
}
