// BullMQ Worker: per-tenant concurrency without burning attempts, graceful shutdown, dead-letter queue.
import { GroupConcurrency } from '@hoshinotatsuya/bullmq-kit';
import {
  acquireOrDelay,
  attachDeadLetter,
  type DeadLetterEntry,
  retryDeadLetter,
  WorkerShutdown,
} from '@hoshinotatsuya/bullmq-kit/bullmq';
import { type Job, Queue, Worker } from 'bullmq';
import { Redis } from 'ioredis';

type ReportJob = { readonly tenantId: string };

// BullMQ needs maxRetriesPerRequest: null on its own connection; the slot client gets a command timeout.
const connection = new Redis({ maxRetriesPerRequest: null });
const slots = new GroupConcurrency(new Redis({ commandTimeout: 5_000 }), { prefix: 'myapp:tenant-slots' });

const queue = new Queue<ReportJob>('reports', { connection });
const dead = new Queue<DeadLetterEntry>('reports-dead', { connection });
const shutdown = new WorkerShutdown();

const worker = new Worker<ReportJob>(
  'reports',
  shutdown.track(async (job: Job<ReportJob>, token, signal) => {
    const holder = `${job.queueName}:${job.id}`;
    // At the limit the job goes to `delayed` with exponential backoff (no attempt spent).
    await acquireOrDelay(job, token, () =>
      slots.acquireDetailed(job.data.tenantId, holder, { limit: 3, ttlMs: 60_000 }),
    );
    try {
      signal.throwIfAborted();
      // ... work that listens to `signal`
    } finally {
      await slots.release(job.data.tenantId, holder);
    }
  }),
  { connection },
);
shutdown.attach(worker);

// Jobs that failed for good are copied with reason, stack trace and data.
const detach = attachDeadLetter(worker, dead, { onError: (error) => console.warn(String(error)) });

process.once('SIGTERM', () => {
  void (async () => {
    // Stay below the orchestrator's grace period (e.g. 30 s in Kubernetes).
    const report = await shutdown.close({ deadlineMs: 25_000 });
    console.log(
      'drained:',
      report.drained,
      'interrupted:',
      report.interrupted,
      'handed back:',
      report.handedBack,
    );
    detach();
  })();
});

export async function replayDeadLetters(): Promise<void> {
  for (const entry of await dead.getJobs(['wait'])) {
    await retryDeadLetter(entry, queue); // 'retried' (same job) or 'readded' (same id)
  }
}
