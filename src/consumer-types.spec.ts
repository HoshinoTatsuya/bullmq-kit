import { join } from 'node:path';

import * as ts from 'typescript';

/**
 * Структурные порты пакета против НАСТОЯЩИХ типов клиента: ioredis 5 и 6,
 * BullMQ 5 и 6 (псевдонимы в devDependencies). Порт описан своим интерфейсом,
 * и рантайм с новой мажорной версией клиента обычно работает, а компиляция
 * потребителя — нет: в ioredis 6 у `zrange` аргумент `stop` перестал принимать
 * число, и `new OrphanedJobsCleanup(redis)` давал TS2345 при работающем коде.
 *
 * Код потребителя компилируется программой TypeScript в памяти: модуль
 * `consumer.ts` рядом с исходниками пакета, импорты пакета — из `src`,
 * клиентов — по имени пакета (так их разрешает потребитель).
 */
const ROOT = join(__dirname, '..');

const consumer = (ioredis: string, bullmq: string): string => `
import { Cluster, Redis } from '${ioredis}';
import { Job, Queue, Worker } from '${bullmq}';
import { FairShare } from './fair-share/fair-share';
import { GroupConcurrency } from './group-concurrency/group-concurrency';
import type { GroupConcurrencyRedis } from './group-concurrency/group-concurrency-redis.type';
import type { SlotSubscriber } from './group-concurrency/slot-subscriber.type';
import { GroupRateLimit } from './group-rate-limit/group-rate-limit';
import { QueueInspector } from './inspect/queue-inspector';
import { OrphanedJobsCleanup } from './orphan-cleanup/orphaned-jobs-cleanup';
import type { CleanupRedis, CleanupScanner } from './orphan-cleanup/types/cleanup-redis.type';
import { acquireOrDelay } from './bullmq/acquire-or-delay';
import { attachDeadLetter } from './bullmq/attach-dead-letter';
import { retryDeadLetter } from './bullmq/retry-dead-letter';
import { WorkerShutdown } from './bullmq/worker-shutdown';
import type { DeadLetterEntry } from './bullmq/dead-letter-entry.type';

declare const redis: Redis;
declare const cluster: Cluster;
declare const worker: Worker;
declare const queue: Queue;
declare const deadLetters: Queue<DeadLetterEntry>;
declare const job: Job;

export const ports: [CleanupRedis, CleanupRedis, CleanupScanner, GroupConcurrencyRedis, GroupConcurrencyRedis, SlotSubscriber] =
  [redis, cluster, redis, redis, cluster, redis.duplicate()];

export const cleanup = new OrphanedJobsCleanup(redis, { queuePrefixes: ['bull:q'] });
export const clusterCleanup = new OrphanedJobsCleanup(cluster, {
  queuePrefixes: ['{bull}:q'],
  scanClients: () => cluster.nodes('master'),
});
export const inspector = new QueueInspector(redis, { queuePrefixes: ['bull:q'] });
export const slots = new GroupConcurrency(redis, { prefix: 'slots', subscriber: redis.duplicate() });
export const clusterSlots = new GroupConcurrency(cluster, { prefix: '{slots}' });
export const fair = new FairShare(redis, { prefix: 'fair' });
export const rate = new GroupRateLimit(redis, { prefix: 'rate' });
export const shutdown = new WorkerShutdown().attach(worker);
export const detach = attachDeadLetter(worker, deadLetters);
export const delayed = (token: string) =>
  acquireOrDelay(job, token, () => slots.acquireDetailed('g', String(job.id), { limit: 1, ttlMs: 1_000 }));
export const retried = (entry: Job<DeadLetterEntry>) => retryDeadLetter(entry, queue);
`;

/** Ошибки компиляции кода потребителя — текстом, с позицией. */
const diagnosticsFor = (ioredis: string, bullmq: string): string[] => {
  const fileName = join(ROOT, 'src', `consumer-${`${ioredis}-${bullmq}`.replace(/[^a-z0-9-]/gi, '-')}.ts`);
  const options: ts.CompilerOptions = {
    target: ts.ScriptTarget.ES2023,
    lib: ['lib.es2023.d.ts'],
    module: ts.ModuleKind.NodeNext,
    moduleResolution: ts.ModuleResolutionKind.NodeNext,
    strict: true,
    exactOptionalPropertyTypes: true,
    noUncheckedIndexedAccess: true,
    esModuleInterop: true,
    skipLibCheck: true,
    noEmit: true,
    types: ['node'],
  };
  const host = ts.createCompilerHost(options, true);
  const source = consumer(ioredis, bullmq);
  const readFile = host.readFile.bind(host);
  const fileExists = host.fileExists.bind(host);
  const getSourceFile = host.getSourceFile.bind(host);
  host.readFile = (name) => (name === fileName ? source : readFile(name));
  host.fileExists = (name) => name === fileName || fileExists(name);
  host.getSourceFile = (name, version, onError, create) =>
    name === fileName
      ? ts.createSourceFile(name, source, version, true)
      : getSourceFile(name, version, onError, create);
  const program = ts.createProgram([fileName], options, host);
  return ts.getPreEmitDiagnostics(program).map((diagnostic) => {
    const text = ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n');
    const at =
      diagnostic.file && diagnostic.start !== undefined
        ? `${diagnostic.file.fileName.replace(`${ROOT}/`, '')}:${diagnostic.file.getLineAndCharacterOfPosition(diagnostic.start).line + 1}`
        : '';
    return `${at} TS${diagnostic.code}: ${text}`;
  });
};

describe('код потребителя компилируется с каждой поддерживаемой парой клиентов', () => {
  it.each([
    ['ioredis', 'bullmq'],
    ['ioredis', 'bullmq-5'],
    ['ioredis-5', 'bullmq-6-oldest'],
    ['ioredis-5', 'bullmq-oldest'],
  ] as const)(
    '%s + %s: ошибок нет',
    (ioredis, bullmq) => {
      expect(diagnosticsFor(ioredis, bullmq)).toEqual([]);
    },
    60_000,
  );

  it('прогон идёт против заявленной ioredis (IOREDIS_UNDER_TEST), а не той же последней', () => {
    const { version } = require('ioredis/package.json') as { version: string };
    expect(Number(version.split('.')[0])).toBe(process.env.IOREDIS_UNDER_TEST === 'ioredis-5' ? 5 : 6);
  });

  it('проверка не холостая: несовместимый порт даёт ошибку', () => {
    // Тот же код с заведомо чужим «клиентом» обязан не компилироваться: иначе
    // зелёные случаи выше означали бы, что импорты разрешились в any.
    const broken = consumer('node:events', 'bullmq');
    expect(broken).toContain("from 'node:events'");
    expect(diagnosticsFor('node:events', 'bullmq').length).toBeGreaterThan(0);
  }, 60_000);
});
