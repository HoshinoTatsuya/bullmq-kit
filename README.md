# @hoshinotatsuya/bullmq-kit

[![npm](https://img.shields.io/npm/v/@hoshinotatsuya/bullmq-kit.svg)](https://www.npmjs.com/package/@hoshinotatsuya/bullmq-kit)
[![CI](https://github.com/HoshinoTatsuya/bullmq-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/HoshinoTatsuya/bullmq-kit/actions/workflows/ci.yml)

[Русская версия](./README.ru.md) · [Changelog](./CHANGELOG.md)

Redis building blocks for BullMQ workers, each one written against a failure seen in real operation:

- **`GroupConcurrency`** — at most N concurrent jobs per group (tenant, project, IP), shared by all replicas, with lease renewal, waiting instead of polling, and all-or-nothing capture of several groups;
- **`GroupRateLimit`** — a token bucket per group (N starts per interval);
- **`FairShare`** — one pool split fairly between groups by weight;
- **`acquireOrDelay`** — at the limit, move the BullMQ job to `delayed` with exponential backoff instead of failing it or burning an attempt;
- **`WorkerShutdown`** — drain a Worker within an orchestrator grace period, abort what does not finish, report what was interrupted;
- **`attachDeadLetter` / `retryDeadLetter`** — copy jobs that failed for good into a dead-letter queue and replay them;
- **`OrphanedJobsCleanup`** — delete job hashes that no status index references, with a BullMQ version gate and an atomic check-and-delete;
- **`QueueInspector` / `toPrometheus`** — queue health (stuck active jobs, oldest waiting job, overdue delayed jobs, failed growth) as Prometheus text.

Why not a plain `INCR`/`DECR` counter for the per-group limit? A release arrives from several places (each failed attempt, stalled-job recovery, a retry), and one extra `DECR` hands out a slot nobody freed. In a measurement on a live Redis a limit of 3 let 5 jobs run, with no log line. Here a group is a set of holders with deadlines: release is idempotent, a crashed holder frees its slot by deadline, and all decisions are atomic Lua scripts using the Redis server clock.

## Install

```sh
npm install @hoshinotatsuya/bullmq-kit ioredis
# for the /bullmq entry point (acquireOrDelay, WorkerShutdown, dead-letter queue):
npm install bullmq
```

| Peer dependency | Range | Needed by |
|---|---|---|
| `ioredis` | `^5 \|\| ^6` | every entry point (any client with `eval` works for the limits) |
| `bullmq` | `^5 \|\| ^6`, optional | only `@hoshinotatsuya/bullmq-kit/bullmq` |

Node.js ≥ 24. The package is CommonJS with type declarations; it loads from ESM too.

## Quick start

```ts
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
```

All snippets in this README are excerpts from [`examples/`](./examples), which is type-checked against the package source on every `pnpm run typecheck`; a test (`src/readme-examples.spec.ts`) fails if a snippet here stops matching the examples.

## Entry points

| Import from | Exports | Requires |
|---|---|---|
| `@hoshinotatsuya/bullmq-kit` | `GroupConcurrency`, `GroupRateLimit`, `FairShare`, `ACQUIRE_OUTCOME`, `SlotLostError`, `SlotLostDuringWorkError`, types | `ioredis` |
| `@hoshinotatsuya/bullmq-kit/bullmq` | `acquireOrDelay`, `delayResolver`, `DELAY_ON_LIMIT_DEFAULTS`, `WorkerShutdown`, `ShutdownDeadlineError`, `attachDeadLetter`, `retryDeadLetter`, `DEAD_LETTER`, types | `ioredis`, `bullmq` |
| `@hoshinotatsuya/bullmq-kit/orphan-cleanup` | `OrphanedJobsCleanup`, `BULLMQ_KEYS`, `BULLMQ_SUPPORTED_VERSIONS`, `CLEANUP_DEFAULTS`, `CLEANUP_STOP_REASON`, `ORPHAN_VERDICT`, `unsupportedBullmqVersion`, types | `ioredis` (does not load `bullmq`) |
| `@hoshinotatsuya/bullmq-kit/inspect` | `QueueInspector`, `toPrometheus`, `INSPECT_STATUSES`, type `QueueHealth` | `ioredis` (does not load `bullmq`) |

`acquireOrDelay` is deliberately **not** exported from the root: `bullmq` is an optional peer, and the root entry must load in a project without it (a test loads the root with `bullmq` blocked). For `moduleResolution: node` the subpaths are also described in `typesVersions`. Every exported constant is deep-frozen.

## Usage and API

### `GroupConcurrency`

`new GroupConcurrency(redis, options)`

| Option | Default | Meaning |
|---|---|---|
| `prefix` | — (required) | key prefix, keys are `<prefix>:<group>`. Use a distinct prefix per system even on separate Redis instances. In Redis Cluster `acquireMany` and waiting need a hash tag (`{myapp}:slots`) |
| `onOutcome(group, outcome)` | — | metrics hook, called after Redis answered each capture. Exceptions and rejected promises from the hook are swallowed (otherwise a slot already taken in Redis would leak to a caller that saw an exception). `group` is external input — use it as a Prometheus label only if the number of groups is bounded |
| `cluster` | detected from the client (`isCluster`) | Redis Cluster mode: a prefix without a hash tag `{…}` is rejected in the constructor instead of surfacing as `CROSSSLOT` later |
| `subscriber` | — | a dedicated connection (`redis.duplicate()`) for `acquireWaiting` and `withSlot(…, wait)`. Without it waiting is rejected; everything else works |

The client only needs `eval(script, numKeys, ...args)` (`GroupConcurrencyRedis`).

| Method | Returns | Notes |
|---|---|---|
| `acquire(group, holder, { limit, ttlMs })` | `'acquired' \| 'held' \| 'limit'` | the same holder again → `held`, no extra slot spent, deadline extended. Expired holders are removed by the same call |
| `acquireDetailed(group, holder, slot)` | `{ outcome, retryAfterMs }` | `retryAfterMs` at `limit` is the **worst case**: when a slot frees up if no holder releases or renews (the holder whose expiry actually opens a slot, also when the limit was lowered). `null` — not `limit`, or the group is closed (`limit: 0`) |
| `reserve(group, holder, slot)` / `reserveDetailed` | same as above | like `acquire`, but does **not** extend a slot the holder already holds: a short preparation lease must not shorten the lease of running work |
| `withSlot(group, holder, slot, work, wait?)` | `{ outcome: 'acquired' \| 'held', value } \| { outcome: 'limit', retryAfterMs }` | acquire, run `work(signal)` with renewal every `ttlMs / 3`, release in `finally`. With `wait` it waits for a slot first (see below) |
| `acquireWaiting(group, holder, slot, { timeoutMs, signal?, pollMs? })` | `{ outcome, retryAfterMs }` | queue up (arrival order) and get woken by `release`; `limit` on timeout or abort |
| `acquireMany(holder, [{ group, limit }, …], ttlMs)` | `{ outcome, retryAfterMs, blockedBy }` | one script, all or nothing; a refusal changes nothing, not even deadlines of slots already held. Release each group with `release` |
| `heartbeat(group, holder, ttlMs)` | `boolean` | renews only a **live** slot (`ZADD XX`); after expiry or `clear` returns `false` and takes nothing |
| `release(group, holder)` | `boolean` | idempotent; wakes the first live waiter |
| `count(group)` | `number` | live holders |
| `holders(group)` | `{ holder, expiresInMs }[]` | live holders by deadline |
| `clear(group)` | `number` | remove all holders (stuck slots after an incident). Running holders lose their slots and the limit is exceeded while they finish |

`limit` is an integer ≥ 0 or `Infinity`: `0` closes the group, `Infinity` removes the limit (holders are still tracked). `ttlMs` is an integer in `(0, 2³¹ − 1]` (Node's timer limit; a larger value would silently become 1 ms). Compare outcomes with `ACQUIRE_OUTCOME.acquired | held | limit`, not with string literals.

**`withSlot` and slot loss.** The slot counts as lost if a renewal answers "no slot" **or** no renewal has been confirmed for `ttlMs − ttlMs/10` since the *send time* of the last confirmed one (its own timer, monotonic clock). A request into a frozen network answers neither success nor failure — with `maxRetriesPerRequest: null` (required by BullMQ) or with ioredis defaults — and measuring from the send time is the safe side: Redis set the deadline no earlier than the send. On loss the work's `signal` is aborted with `SlotLostError` (`group`, `holder`, `reason`). If the work returns a value anyway, `withSlot` **rejects** with `SlotLostDuringWorkError` (a subclass; the value is in `.value`, the loss in `.cause`): `acquired` means "all of the work ran under the slot". Renewals are single-flight (an offline queue does not accumulate them), and the release in `finally` is bounded by the slot's own lifetime. With outcome `held` (the same holder already held the slot) `withSlot` renews but does not release it.

**Holder ids must be unique system-wide.** Two queues with one prefix have overlapping job ids; use `` `${queue.name}:${job.id}` ``. A stalled job retried on a second worker while the first still runs is the same holder: the limit is exceeded by one for that time, and the first copy releases the second one's slot. If that is unacceptable, include the job's lock token in the holder.

**Waiting instead of polling.** Waiters queue up in a ZSET per group; `release` publishes the name of the first *live* waiter, so one freed slot wakes one waiter. A slot freed by expiry or `clear` publishes nothing and is picked up by the fallback re-check (`pollMs`, default 1 000 ms, at most `2³¹ − 1`). A waiter registration lives three re-checks, so a dead waiter does not block the queue. A plain `acquire` does not respect the queue — mixing is allowed, but order then holds only among waiters. An already aborted `signal` returns `limit` without touching Redis.

```ts
// Waiting for a slot needs a dedicated subscriber connection.
const slots = new GroupConcurrency(redis, { prefix: '{myapp}:slots', subscriber: redis.duplicate() });
```

```ts
export async function waitForSlot(group: string, holder: string): Promise<unknown> {
  // Queue up and be woken by `release` instead of polling; `limit` after 60 s.
  const result = await slots.withSlot(group, holder, { limit: 3, ttlMs: 30_000 }, async () => 'done', {
    timeoutMs: 60_000,
  });
  return result.outcome === ACQUIRE_OUTCOME.limit ? null : result.value;
}
```

**Several groups at once.**

```ts
  // All or nothing: a refusal in one group changes nothing in the others.
  const taken = await slots.acquireMany(
    holder,
    [
      { group: `project:${projectId}`, limit: 1 },
      { group: `tenant:${tenantId}`, limit: 5 },
    ],
    30_000,
  );
```

`retryAfterMs` of `acquireMany` is the maximum over the blocking groups (all of them must free up), `null` if one of them is closed. A repeated group in the list is rejected before Redis. In Redis Cluster the hash tag is checked by Redis's own rule (from the first `{` to the next `}`, empty means no tag), so `app{}{t}` is rejected.

### `GroupRateLimit`

```ts
// 60 calls per minute per API key, bursts up to 60.
const rate = new GroupRateLimit(redis, { prefix: 'myapp:rate' });
export async function callApi(apiKey: string): Promise<boolean> {
  const taken = await rate.take(apiKey, { limit: 60, intervalMs: 60_000 });
  // For a rate the hint is exact: a token is available in exactly retryAfterMs (unless someone else takes it).
  return taken.outcome !== ACQUIRE_OUTCOME.limit;
}
```

`take(group, { limit, intervalMs }, cost = 1)` — a token bucket of capacity `limit`, refilled evenly over `intervalMs`, server clock. External APIs limit the *rate*, not concurrency: a concurrency limit of 3 would still let through a hundred short calls per second. The answer is an `AcquireResult` with `exact: true`, so `acquireOrDelay` delays exactly by the hint instead of backing off. A taken token is not returned. The bucket key lives until the bucket is full again, so keys from external input do not accumulate. `cost` must be an integer in `[1, limit]` (a larger cost would wait forever).

### `FairShare`

```ts
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
```

`acquire(pool, group, holder, { poolLimit, ttlMs, weight = 1, waitTtlMs = 60_000 })` → `{ outcome, retryAfterMs, share, held }`. A group's share is `max(1, ⌊poolLimit · weight / Σ weights of active groups⌋)`; active groups are those with holders and those that were refused recently (`waitTtlMs`). A group gets a slot if the pool is not full and it is below its share, or if no *waiting* group is below its share — free capacity is never idle, and one group may take the whole pool while nobody else needs it. There is no preemption: excess is given back as work finishes. Set `waitTtlMs` no lower than the retry interval of the group's jobs (`maxDelayMs` of `acquireOrDelay`), otherwise the group "disappears" between retries. Also `release(pool, group, holder)` and `holders(pool)`. In Redis Cluster the prefix needs a hash tag (one script uses three keys).

### `acquireOrDelay` (`/bullmq`)

```ts
    const holder = `${job.queueName}:${job.id}`;
    // At the limit the job goes to `delayed` with exponential backoff (no attempt spent).
    await acquireOrDelay(job, token, () =>
      slots.acquireDetailed(job.data.tenantId, holder, { limit: 3, ttlMs: 60_000 }),
    );
```

`acquireOrDelay(job, token, acquire, options?)` accepts any capture with a hint (`acquireDetailed`, `reserveDetailed`, `acquireMany`, `GroupRateLimit.take`, `FairShare.acquire`) and returns the success outcome. At `limit` it calls `job.moveToDelayed(now + delay, token)` and throws BullMQ's `DelayedError`: the job is neither failed nor charged an attempt.

The delay is `minDelayMs · 2ⁿ`, where `n` is the number of previous delays of this job (`attemptsStarted − attemptsMade − 1`), capped by the smaller of `maxDelayMs` and the hint (for a closed group: `closedGroupDelayMs`), plus a random jitter in `[0, max(jitterMs, delay · jitterFraction))`. An exact hint (`GroupRateLimit`) is used as is.

| Option | Default (`DELAY_ON_LIMIT_DEFAULTS`) |
|---|---|
| `minDelayMs` | 100 |
| `maxDelayMs` | 60 000 |
| `closedGroupDelayMs` | 60 000 |
| `jitterMs` | 250 |
| `jitterFraction` | 0.25 |
| `maxTotalWaitMs` | not set; when set, a job waiting longer since **creation** (`job.timestamp`, so including its own `delay`) fails with `UnrecoverableError` |
| `now`, `random` | `Date.now`, `Math.random` (for tests) |

- Every delay is a new *start* of the job: `attemptsStarted` grows, so a Worker's `maxStartedAttempts` counts delays too — leave room for them or do not set it. `attemptsMade` is not affected.
- Invalid options and an empty `token` are rejected **before** the capture (without the token BullMQ would substitute `'0'` and `moveToDelayed` would skip the lock check). A failing `moveToDelayed` (lock lost) is rethrown as is.
- `delayResolver(options)(retryAfterMs, delaysSoFar)` exposes the delay computation.
- Redis calls have no deadline of their own: a client with `maxRetriesPerRequest: null`, required by BullMQ, holds commands forever while Redis is unreachable. Give the limiter its own client with `commandTimeout`.

### `WorkerShutdown` and `ShutdownDeadlineError` (`/bullmq`)

```ts
const worker = new Worker<ReportJob>(
  'reports',
  shutdown.track(async (job: Job<ReportJob>, token, signal) => {
```

```ts
process.once('SIGTERM', () => {
  void (async () => {
    // Stay below the orchestrator's grace period (e.g. 30 s in Kubernetes).
    const report = await shutdown.close({ deadlineMs: 25_000 });
```

`track(processor)` wraps the processor (in-flight accounting and a `signal` that the shutdown deadline aborts, merged with BullMQ's own signal); `attach(worker)` binds the Worker; `inFlight()` lists running job ids; `close({ deadlineMs, abortGraceMs = 1000, handBackDelayMs = 1000 })` → `{ drained, interrupted, handedBack, durationMs }`:

1. `pause(true)` — stop taking jobs without waiting for running ones;
2. wait for started jobs until `deadlineMs` (tracked by the wrapper, not by events: a job delayed with `DelayedError` emits neither `completed` nor `failed`);
3. on deadline abort their signals with `ShutdownDeadlineError`, give them `abortGraceMs`, then `close(true)`; otherwise a normal `close()`;
4. report. Interrupted jobs will run again on another worker (stalled recovery) — that is why their ids are returned.

A job that the Worker picked up after shutdown started (BullMQ's pause does not cancel a blocking fetch already in progress) is not run: it is handed back with `moveToDelayed(now + handBackDelayMs)` and listed in `handedBack`. All three durations must be numbers in `[0, 2³¹ − 1]`; otherwise `close` rejects before the first step, and the rejection is not cached. A second `close` returns the same report.

### Dead-letter queue (`/bullmq`)

```ts
// Jobs that failed for good are copied with reason, stack trace and data.
const detach = attachDeadLetter(worker, dead, { onError: (error) => console.warn(String(error)) });
```

```ts
export async function replayDeadLetters(): Promise<void> {
  for (const entry of await dead.getJobs(['wait'])) {
    await retryDeadLetter(entry, queue); // 'retried' (same job) or 'readded' (same id)
  }
}
```

`attachDeadLetter(worker, deadQueue, { onError? })` returns a detach function. A job is copied only when it failed **for good** — judged by the job state (`failed`), not by the event, which BullMQ emits on every failed attempt. The copy (`DeadLetterEntry`: `queue`, `jobId`, `name`, `data`, `opts`, `failedReason`, `stacktrace`, `attemptsMade`, `failedAt`) has a deterministic id (queue, job id, failure time), so a repeated event creates no duplicate. Write failures go to `onError` (otherwise they are ignored: an exception in a Worker event handler would crash the process).

`retryDeadLetter(entry, targetQueue)`: if the original is still in `failed` — `retry()` it (`'retried'`); if retention already removed it — add it again with the same id and safe options only (`DEAD_LETTER.RETRY_OPTS`: `attempts`, `backoff`, `priority`, `removeOnComplete`, `removeOnFail`, `lifo`) (`'readded'`); if it exists in another state — reject (a retry would duplicate it). Then the dead-letter entry is removed. A queue-name mismatch is rejected.

### `OrphanedJobsCleanup` (`/orphan-cleanup`)

A job hash that no status index references (a desync after a connection drop in the middle of a transition) is an orphan. BullMQ never deletes it.

```ts
const cleanup = new OrphanedJobsCleanup(redis, {
  queuePrefixes: ['bull:emails', 'bull:reports'],
  // Without a logger a refused queue (unknown BullMQ version, nested queue) is visible only in the report.
  logger: console,
  // Replicas running the same schedule do the work once.
  runLockKey: 'myapp:orphan-cleanup-lock',
  // First run on an unfamiliar installation: report only, delete nothing.
  dryRun: true,
});
```

**Version gate.** Before each queue the cleanup reads `<queue>:meta` field `version` (`bullmq:X.Y.Z`, written by every `new Queue`). A queue outside the tested ranges — or without the field — is **not cleaned** and is listed in `report.unsupportedQueues` with a reason. Tested ranges (`BULLMQ_SUPPORTED_VERSIONS.RANGES`): **5.72.0 … 5.81.x** and **6.0.0 … 6.7.x**; the full suite runs against each of the four bounds. Pre-releases are older than their release and are refused. `unsupportedBullmqVersion(raw)` returns the refusal reason or `null`.

**One script per candidate.** A candidate (absent from the index snapshot) is checked and deleted by one Lua script, so a job cannot enter an index between check and delete. The script keeps the hash if: it is not a hash; a prefix before any `:` in the job id is itself a BullMQ queue (`kept.nested`); the job has a live `:lock` (`kept.locked`); the id is in any status index — `prioritized`, `delayed`, `completed`, `failed`, `waiting-children`, `stalled`, `wait`, `active`, `paused` (an index of an unexpected type → `kept.unverifiable`); it is younger than `graceMs` by its `timestamp` against the Redis clock; it lacks `name` and `opts` (`kept.notJob`); it has no `timestamp` (`kept.untimestamped`, unless `deleteUntimestamped`); a pending deduplicated job (`dn:`) points at it. Otherwise it deletes the hash and the same tails BullMQ deletes (`:logs`, `:dependencies`, `:processed`, `:failed`, `:unsuccessful`) and a deduplication key `de:` that points at it. `:lock` is never deleted.

| Option | Default (`CLEANUP_DEFAULTS`) | Meaning |
|---|---|---|
| `queuePrefixes` | — (required, non-empty) | `bull:<name>` per queue. An empty list is rejected: `:*` from an empty prefix would walk all of Redis |
| `dryRun` | `false` | delete nothing; `orphanKeys` lists what would be deleted |
| `deleteUntimestamped` | `false` | delete hashes without `timestamp` on the second sighting, not earlier than `graceMs` |
| `graceMs` | 60 000 | a younger hash is never deleted |
| `maxJobsPerRun` | 5 000 | job hashes checked per run |
| `maxScannedKeysPerRun` | 200 000 | keys returned by SCAN per run, tails included |
| `maxRunMs` | 60 000 | run duration cap (checked between batches) |
| `maxSnapshotZsetSize` | 100 000 | larger sorted sets are not snapshotted; their jobs are checked by `ZSCORE` in the script |
| `maxSnapshotListSize` | 1 000 000 | a longer status list skips the queue for this run (`unsnapshottedIndexes`); `Infinity` disables the cap (the client then needs no `llen`) |
| `maxListScan` | 1 000 000 | `LPOS … MAXLEN` per candidate; a longer list without the id near its start → `kept.unverifiable`. `Infinity` — no cap; `0` is rejected |
| `maxListWorkPerBatch` | 1 000 000 | shrink delete batches so that candidates × list length stays below this |
| `scanCount`, `batchSize`, `snapshotPage` | 500, 100, 10 000 | SCAN `COUNT`, candidates per pipeline, snapshot page; `0` is rejected |
| `highOrphanShare` | 0.1 | share of orphans that sets `report.highOrphanShare` |
| `reportKeysLimit` | 100 | keys returned in `orphanKeys` and per reason in `keptKeys` |
| `logger` | — | `{ log, warn, debug? }` (works with `console` and Nest `Logger`) |
| `scanClients` | the main client | Redis Cluster: SCAN covers one node — pass the masters, as a list or a function (`() => cluster.nodes('master')`, called every run, so failover is followed) |
| `runLockKey` | — | `SET NX PX` run lock for `maxRunMs` + 30 s; taken → `lockedOut: true` |
| `now` | `Date.now` | for tests |

`run()` → `CleanupReport`: `scanned`, `scannedKeys`, `deleted`, `durationMs`, `orphans`, `orphanKeys`, `highOrphanShare`, `dryRun`, `kept` (`alive`, `locked`, `young`, `unknownAge`, `notHash`, `unverifiable`, `untimestamped`, `notJob`, `nested`, `errors`), `keptKeys`, `truncatedBy` (`CLEANUP_STOP_REASON`: `jobs` | `keys` | `time` | `null`), `unsnapshottedIndexes`, `unsupportedQueues`, `lockedOut`. Concurrent `run()` calls on one instance share one run. Scripts run by SHA (`EVALSHA`, falling back to `EVAL` on `NOSCRIPT`). Between batches the process yields with `setImmediate`. `ORPHAN_VERDICT` names the script's per-candidate answers; `BULLMQ_KEYS` is the key vocabulary (checked against BullMQ's own `QueueKeys.getKeys()` in every test run). `OrphanedJobsCleanup.isJobKeySuffix(suffix)` says whether a key suffix after the queue prefix is a job hash (not a service key, a per-job tail or a nested space).

**Not supported:** BullMQ Pro groups (their jobs live in group keys the cleanup does not know and would look orphaned — do not run it on Pro queues); a client with ioredis `keyPrefix` (rejected in the constructor; BullMQ does not support it either). The cleanup sets no TTL on live hashes: a safe TTL depends on how long your jobs may wait.

### `QueueInspector` and `toPrometheus` (`/inspect`)

```ts
const inspector = new QueueInspector(new Redis(), { queuePrefixes: ['bull:emails', 'bull:reports'] });

createServer((_request, response) => {
  inspector.inspect().then(
    (health) => {
      response.setHeader('content-type', 'text/plain; version=0.0.4');
      response.end(toPrometheus(health));
    },
```

`new QueueInspector(redis, { queuePrefixes, maxActiveScan = 1000, orphans = false })`; `inspect()` → `QueueHealth[]`, one Lua script per queue (consistent, server clock, read-only):

| Field | Meaning |
|---|---|
| `counts` | size of each index in `INSPECT_STATUSES` (`wait`, `active`, `paused`, `delayed`, `prioritized`, `waiting-children`, `completed`, `failed`) |
| `paused` | `meta.paused` (BullMQ 5 and 6) |
| `unexpectedTypes` | status keys of a wrong type — not read; their count `0` means "not read" |
| `stuckActive` | `{ count, scanned, sample }`: the `maxActiveScan` **oldest** active jobs without `:lock` |
| `oldestWaitAgeMs` | age of the oldest waiting job over `wait` and `paused`; `null` — none |
| `delayedOverdueMs` | how late the earliest delayed job is; `null` — none |
| `failedIncrease` | growth of `failed` since this instance's previous snapshot; `null` on the first |
| `orphans` | orphan count via a dry-run cleanup with `orphans: true` (or cleanup options); a SCAN of the whole queue space — run it rarely; `null` otherwise |

`toPrometheus(health, metricPrefix = 'bullmq_queue')` renders gauges `_jobs{queue,state}`, `_paused`, `_unexpected_type_keys`, `_stuck_active`, `_oldest_wait_age_seconds`, `_delayed_overdue_seconds`, `_orphans`. Unknown values are omitted rather than rendered as `0`. Labels are escaped; the only label besides `state` is the queue prefix. On BullMQ 6 a paused queue keeps its jobs in `wait`, so exclude `bullmq_queue_paused == 1` from alerts on wait age.

## Guarantees and limits

- **Atomicity:** every decision of `GroupConcurrency`, `GroupRateLimit`, `FairShare` and every orphan check-and-delete is one Lua script; time is the Redis server's `TIME`, not the replica's clock.
- **Idempotent release:** `release` is a `ZREM`; repeating it changes nothing. A crashed holder frees its slot after `ttlMs`.
- **Bounded key space:** group keys expire with their latest holder; rate buckets expire when full; waiter queues expire with their registrations. Memory is bounded by the rate of new groups × `ttlMs`, not by their total number (≈ 141 bytes per group with one holder).
- **Throughput ceiling:** one Redis thread. On a laptop with Redis 7.4 in docker: 2.6 k acquire+release pairs/s for one caller, 42 k/s for 128 callers (p99 7.3 ms); cost does not depend on the number of holders in a group. A hash tag puts all groups of one prefix on one cluster shard — use several prefixes to scale out.
- **`retryAfterMs` is a worst case**, not a schedule: with renewal it is almost always the full `ttlMs`. Waiting on it loses throughput by orders of magnitude (measured: 200 jobs, limit 10, 20 ms work, `ttlMs` 30 s — 91 s with the hint as delay, 2.5 s with exponential backoff, 0.47 s with waiting).
- **Orphan cleanup cost** is orphans × length of the status lists (`LPOS` walks a list fully for an absent id): about 1 ms per orphan per 100 k list items, during which Redis serves others more slowly. `maxListScan`, `maxListWorkPerBatch` and `maxJobsPerRun` bound it; numbers in [`bench/`](./bench).
- **The dead-letter copy is best effort:** it is made by an event in the worker process; a process that dies between the failure and the copy does not make it.
- **Not provided:** a wrapper over the whole BullMQ API, a scheduler of repeatable jobs (BullMQ has job schedulers), slot holders without a deadline, preemption in `FairShare`.
- **Messages:** error messages, log lines and Prometheus `HELP` texts are in Russian. Programmatic fields (`outcome`, `reason`, report fields, error classes) are stable; do not parse message texts.

## Failure behavior

The package fails closed — "could not determine" never means "allowed":

- a Redis error is an exception, not `false`: "could not ask" and "not allowed" are different answers, and the caller picks the policy;
- invalid arguments (empty group or holder, `ttlMs ≤ 0` or above `2³¹ − 1`, fractional or negative `limit`, `cost` above capacity, missing hash tag in cluster mode, empty `queuePrefixes`, zero batch sizes) are rejected **before** any Redis call or in the constructor;
- orphan cleanup does not delete when in doubt: an unknown or missing BullMQ version, a nested queue, an index of an unexpected type, a list too long to check, a non-numeric or missing timestamp, a hash without `name`/`opts` — all keep the hash and are counted in `kept` or `unsupportedQueues`. Without a `logger`, a refusal is visible only in the report;
- `withSlot` treats an unconfirmed renewal as a lost slot after `ttlMs − ttlMs/10`, aborts the work and rejects even if the work returned;
- a frozen network: test it with a client that has `commandTimeout`; the BullMQ client (`maxRetriesPerRequest: null`) waits forever by design.

## Compatibility

| | Supported and tested |
|---|---|
| Node.js | ≥ 24 |
| Redis | 7.4 and 8; Redis Cluster 7.4 (three masters) |
| `ioredis` | 5 and 6 |
| `bullmq` | 5 and 6 (peer `^5 \|\| ^6`); orphan cleanup only for 5.72.0–5.81.x and 6.0.0–6.7.x |
| TypeScript (consumer) | 5.9, 6.0, 7 — CJS and ESM, checked on the packed tarball |

Do not test against `ioredis-mock`: its Lua uses 32-bit integers, `TIME × 1000` overflows, and slot deadlines do not work there at all. Dragonfly rejects undeclared keys by default (BullMQ itself requires `--default_lua_flags=allow-undeclared-keys` there).

**Migrating from a hand-written counter:** use a new key prefix. ZSET commands on an old string key answer `WRONGTYPE` right after rollout; old keys simply expire.

## Testing and development

```sh
pnpm install
pnpm run typecheck   # src and examples/
pnpm run lint        # biome
pnpm test            # five runs, see below; needs docker
pnpm run test:consumers  # the packed tarball in throwaway projects; needs network access to the npm registry
pnpm run build
```

`pnpm test` runs the suite five times through `jest-groups` (groups of at most 300 tests, at most 2 workers, one group after another), each time against a throwaway Redis in docker, and fails if any test was skipped:

| Script | BullMQ | ioredis | Redis |
|---|---|---|---|
| `test:latest` | latest 6 | 6 | 8 |
| `test:bullmq5` | latest 5 | 6 | 7.4 |
| `test:bullmq6-oldest` | 6.0.0 | 5 | 8 |
| `test:oldest` | 5.72.0 | 5 | 7.4 |
| `test:cluster` | latest 6 | 6 | Redis Cluster 7.4 (docker, or local `redis-server` processes) |

Set `REDIS_TEST_URL` / `REDIS_CLUSTER_TEST_NODES` to use an existing Redis instead of docker. Failures are injected for real: a freezing TCP proxy (socket open, no bytes), failed renewals, `SCRIPT FLUSH`, malformed script replies, pipeline errors. Benchmarks are in [`bench/`](./bench) (`pnpm build && scripts/with-redis.sh node bench/<script>.cjs`).

## License

[MIT](./LICENSE) © 2026 Hoshino Tatsuya
