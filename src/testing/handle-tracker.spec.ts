import { createServer } from 'node:net';

import { HandleTracker } from './handle-tracker';

/**
 * Гейт «тест не оставляет ресурсов» (no-lingering-handles.setup.ts) зелен и
 * тогда, когда учёт не видит НИЧЕГО. Здесь — что он видит запрещённое (таймер
 * и сервер, держащие процесс) и не видит разрешённого (снятый, сработавший,
 * `unref()`, закрытый).
 */
describe('HandleTracker', () => {
  it('живой таймер с ref — виден с местом создания; снятый, сработавший и unref — нет', async () => {
    const tracker = new HandleTracker();
    tracker.start();
    const held = setTimeout(() => undefined, 60_000);
    const unrefed = setTimeout(() => undefined, 60_000).unref();
    clearTimeout(setTimeout(() => undefined, 60_000));
    await new Promise((resolve) => setTimeout(resolve, 1));
    tracker.stop();
    try {
      const alive = tracker.alive();
      expect(alive.map(({ type }) => type)).toEqual(['Timeout']);
      expect(alive[0]?.stack).toContain('handle-tracker.spec.ts');
    } finally {
      clearTimeout(held);
      clearTimeout(unrefed);
    }
    expect(tracker.alive()).toEqual([]);
  });

  it('слушающий сервер — виден, закрытый — нет; созданное после stop — не в учёте', async () => {
    const tracker = new HandleTracker();
    tracker.start();
    const server = createServer();
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    tracker.stop();
    const late = setTimeout(() => undefined, 60_000);
    try {
      expect(tracker.alive().map(({ type }) => type)).toEqual(['TCPSERVERWRAP']);
    } finally {
      clearTimeout(late);
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
    // Колбэк close приходит раньше, чем libuv закрыл хэндл (он ещё «закрывается»
    // и держит ref) — закрытие видно со следующего оборота цикла. Гейт поэтому ждёт.
    await new Promise((resolve) => setTimeout(resolve, 1));
    expect(tracker.alive()).toEqual([]);
  });
});
