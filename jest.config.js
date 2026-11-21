/**
 * Полный набор запускается ТОЛЬКО через `pnpm test` (jest-groups: группы
 * ≤300 тестов, ≤2 воркера, последовательно). Прямой `jest` — для одного-двух
 * файлов и тоже с --maxWorkers=2.
 */
/**
 * `BULLMQ_UNDER_TEST=<псевдоним>` — тот же набор против другой поддерживаемой
 * BullMQ (псевдонимы в devDependencies: `bullmq-5`, `bullmq-oldest`,
 * `bullmq-6-oldest`): уборка знает раскладку ключей, и общий номер версии
 * пакета не должен прятать, какую BullMQ сломала очередная правка.
 * `IOREDIS_UNDER_TEST=ioredis-5` — так же для клиента: подмена действует на
 * весь граф модулей, включая ioredis внутри самой BullMQ 6 (у неё он peer).
 */
const bullmqUnderTest = process.env.BULLMQ_UNDER_TEST;
const ioredisUnderTest = process.env.IOREDIS_UNDER_TEST;

/** @type {import('jest').Config} */
module.exports = {
  moduleNameMapper: {
    ...(bullmqUnderTest ? { '^bullmq$': bullmqUnderTest } : {}),
    ...(ioredisUnderTest
      ? { '^ioredis$': ioredisUnderTest, '^ioredis/(.*)$': `${ioredisUnderTest}/$1` }
      : {}),
  },
  testEnvironment: 'node',
  // Каждый тест обязан убрать за собой таймеры и соединения (см. файл).
  setupFilesAfterEnv: ['<rootDir>/src/testing/no-lingering-handles.setup.ts'],
  roots: ['<rootDir>/src'],
  testMatch: ['**/*.spec.ts'],
  // Спеки на Redis Cluster — отдельным прогоном (`test:cluster`, jest.cluster.config.js):
  // им нужен кластер, а не один Redis.
  testPathIgnorePatterns: ['/node_modules/', '\\.cluster\\.spec\\.ts$'],
  transform: { '^.+\\.ts$': ['ts-jest', { tsconfig: 'tsconfig.json' }] },
  maxWorkers: 2,
  clearMocks: true,
  restoreMocks: true,
};
