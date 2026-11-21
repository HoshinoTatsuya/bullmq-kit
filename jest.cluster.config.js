/**
 * Спеки на настоящем Redis Cluster (`scripts/with-redis-cluster.sh`): хэш-теги,
 * скрипты с несколькими ключами, обход по мастерам, публикации между узлами.
 * Запуск — `pnpm run test:cluster` (через jest-groups, как и основной набор).
 */
const base = require('./jest.config.js');

/** @type {import('jest').Config} */
module.exports = {
  ...base,
  testMatch: ['**/*.cluster.spec.ts'],
  testPathIgnorePatterns: ['/node_modules/'],
};
