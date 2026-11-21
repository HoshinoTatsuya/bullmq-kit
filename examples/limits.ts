// Rate per group, fair share of a pool, several groups at once, waiting instead of polling.
import { ACQUIRE_OUTCOME, FairShare, GroupConcurrency, GroupRateLimit } from '@hoshinotatsuya/bullmq-kit';
import { Redis } from 'ioredis';

const redis = new Redis();

// 60 calls per minute per API key, bursts up to 60.
const rate = new GroupRateLimit(redis, { prefix: 'myapp:rate' });
export async function callApi(apiKey: string): Promise<boolean> {
  const taken = await rate.take(apiKey, { limit: 60, intervalMs: 60_000 });
  // For a rate the hint is exact: a token is available in exactly retryAfterMs (unless someone else takes it).
  return taken.outcome !== ACQUIRE_OUTCOME.limit;
}

// One pool of 20 workers; a big tenant cannot starve the small ones that are waiting.
const pool = new FairShare(redis, { prefix: 'myapp:pool' });
export async function inPool(tenantId: string, jobId: string, weight: number): Promise<void> {
  const taken = await pool.acquire('scans', tenantId, jobId, { poolLimit: 20, ttlMs: 60_000, weight });
  if (taken.outcome === ACQUIRE_OUTCOME.limit) {
    return;
  }
  try {
    // ... work
  } finally {
    await pool.release('scans', tenantId, jobId);
  }
}

// Waiting for a slot needs a dedicated subscriber connection.
const slots = new GroupConcurrency(redis, { prefix: '{myapp}:slots', subscriber: redis.duplicate() });

export async function projectAndTenant(holder: string, projectId: string, tenantId: string): Promise<void> {
  // All or nothing: a refusal in one group changes nothing in the others.
  const taken = await slots.acquireMany(
    holder,
    [
      { group: `project:${projectId}`, limit: 1 },
      { group: `tenant:${tenantId}`, limit: 5 },
    ],
    30_000,
  );
  if (taken.outcome === ACQUIRE_OUTCOME.limit) {
    console.log('blocked by', taken.blockedBy);
    return;
  }
  try {
    // ... work
  } finally {
    await slots.release(`project:${projectId}`, holder);
    await slots.release(`tenant:${tenantId}`, holder);
  }
}

export async function waitForSlot(group: string, holder: string): Promise<unknown> {
  // Queue up and be woken by `release` instead of polling; `limit` after 60 s.
  const result = await slots.withSlot(group, holder, { limit: 3, ttlMs: 30_000 }, async () => 'done', {
    timeoutMs: 60_000,
  });
  return result.outcome === ACQUIRE_OUTCOME.limit ? null : result.value;
}
