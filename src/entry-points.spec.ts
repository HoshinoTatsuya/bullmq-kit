/**
 * Входы пакета. `bullmq` — НЕОБЯЗАТЕЛЬНЫЙ peer: проект, которому нужен только
 * `GroupConcurrency`, ставит один ioredis, и корневой вход обязан грузиться без
 * bullmq. Поэтому `acquireOrDelay` живёт во входе `@hoshinotatsuya/bullmq-kit/bullmq`,
 * а не реэкспортируется из корня. Без Redis: подменяется сам модуль `bullmq` —
 * его загрузка бросает, и видно, какой вход его тянет.
 */
describe('входы пакета', () => {
  const forbidBullmq = () => {
    jest.doMock('bullmq', () => {
      throw new Error('bullmq загружен');
    });
  };

  it('корневой вход грузится без bullmq и acquireOrDelay не отдаёт', () => {
    jest.isolateModules(() => {
      forbidBullmq();
      const root = require('./index') as Record<string, unknown>;
      expect(typeof root.GroupConcurrency).toBe('function');
      expect(root).not.toHaveProperty('acquireOrDelay');
    });
  });

  // Контроль подмены: вход /bullmq bullmq действительно грузит, то есть
  // зелёный тест выше — не следствие того, что подмена не сработала.
  it('вход /bullmq грузит bullmq', () => {
    jest.isolateModules(() => {
      forbidBullmq();
      expect(() => require('./bullmq')).toThrow('bullmq загружен');
    });
  });
});
