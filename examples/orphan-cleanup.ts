// Periodic cleanup of job hashes that no status index references.
import { OrphanedJobsCleanup } from '@hoshinotatsuya/bullmq-kit/orphan-cleanup';
import { Redis } from 'ioredis';

const redis = new Redis();

const cleanup = new OrphanedJobsCleanup(redis, {
  queuePrefixes: ['bull:emails', 'bull:reports'],
  // Without a logger a refused queue (unknown BullMQ version, nested queue) is visible only in the report.
  logger: console,
  // Replicas running the same schedule do the work once.
  runLockKey: 'myapp:orphan-cleanup-lock',
  // First run on an unfamiliar installation: report only, delete nothing.
  dryRun: true,
});

export async function runCleanup(): Promise<void> {
  const report = await cleanup.run();
  console.log(`scanned ${report.scanned}, orphans ${report.orphans}, deleted ${report.deleted}`);
  console.log('would delete:', report.orphanKeys, 'kept because:', report.kept);
  if (report.unsupportedQueues.length > 0) {
    console.warn('not cleaned:', report.unsupportedQueues);
  }
  if (report.highOrphanShare) {
    console.warn('high orphan share: look for the cause of the desync');
  }
}

setInterval(() => void runCleanup(), 6 * 60 * 60_000).unref();
