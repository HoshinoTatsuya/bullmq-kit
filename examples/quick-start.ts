// Quick start: at most 3 concurrent jobs per tenant, shared by every replica.
import { ACQUIRE_OUTCOME, GroupConcurrency } from '@hoshinotatsuya/bullmq-kit';
import { Redis } from 'ioredis';

const redis = new Redis(process.env.REDIS_URL ?? 'redis://127.0.0.1:6379');
const slots = new GroupConcurrency(redis, { prefix: 'myapp:tenant-slots' });

export async function handle(tenantId: string, jobId: string): Promise<string> {
  // ttlMs is how long a slot survives a crashed holder, not how long the work takes:
  // withSlot renews the slot every ttlMs / 3 while the work runs.
  const result = await slots.withSlot(
    tenantId,
    `reports:${jobId}`,
    { limit: 3, ttlMs: 30_000 },
    async (signal) => {
      // Long work must listen to `signal`: it is aborted with SlotLostError when the slot is lost.
      signal.throwIfAborted();
      return `report for ${tenantId}`;
    },
  );
  if (result.outcome === ACQUIRE_OUTCOME.limit) {
    // The tenant already runs 3 jobs. retryAfterMs is the worst case (all holders crash), not a schedule.
    throw new Error(`busy, worst-case retry in ${result.retryAfterMs ?? 'never'} ms`);
  }
  return result.value;
}

export async function manual(tenantId: string, jobId: string): Promise<void> {
  // The same with explicit acquire/release.
  const outcome = await slots.acquire(tenantId, jobId, { limit: 3, ttlMs: 10 * 60_000 });
  if (outcome === ACQUIRE_OUTCOME.limit) {
    return;
  }
  try {
    // ... work; renew long work with: await slots.heartbeat(tenantId, jobId, 10 * 60_000)
  } finally {
    await slots.release(tenantId, jobId);
  }
}
