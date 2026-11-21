# Changelog

All notable changes to this package are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the package uses
[semantic versioning](https://semver.org/) with the pre-1.0 convention that a
minor version may break the API (each such version has a **Migration** section).

## [0.9.1] - 2026-11-21

First public release as `@hoshinotatsuya/bullmq-kit`, MIT; documentation in English and Russian; no functional changes.

### Changed
- Development tooling: `@hoshinotatsuya/arch-gates` ^0.12.2, `@hoshinotatsuya/jest-groups` ^0.7.2.
- README examples live in `examples/` and are type-checked by `pnpm run typecheck`; a test keeps the README snippets identical to them.
- CI moved to GitHub Actions; releases are published from version tags with npm provenance.

## [0.9.0] - 2026-11-21

### Changed (breaking)
- `withSlot` now **rejects** with `SlotLostDuringWorkError` (a subclass of `SlotLostError`, exported from the root) when the slot was lost during the work, even if the work returned a value. Previously such work resolved `{ outcome: 'acquired', value }` — a silent success while other holders may already have entered the group. The work's value is in `.value`, the loss in `.cause`.
- `SlotLostError` has a public `reason` field and an optional fourth constructor argument `ErrorOptions` (compatible for code that constructs it).
- `maxListScan` defaults to 1 000 000 (`CLEANUP_DEFAULTS.MAX_LIST_SCAN`), previously `Infinity`. Within the default snapshot cap the behavior is unchanged; a lifted snapshot cap no longer opens an unbounded `LPOS` script (measured on a 3 M `wait` list: neighbour PING p99 31 ms without the cap, 9.5 ms with it).

### Fixed
- Orphan cleanup no longer crashes on an unusual Redis rejection (an object without a prototype or with a throwing `toString`) while building the reason text.

### Migration
- Where the result is needed even after a loss (idempotent work): `catch (e) { if (e instanceof SlotLostDuringWorkError) use(e.value) }`. Otherwise treat the rejection as a normal reason to retry the job.
- To keep the old unbounded scan: `maxListScan: Infinity`.

## [0.8.0] - 2026-11-17

### Changed (breaking)
- `QueueHealth.unexpectedTypes` (new required field) and metric `bullmq_queue_unexpected_type_keys`: a status key of the wrong type used to fail the whole snapshot with `WRONGTYPE`.
- `stuckActive` scans the tail of `active` (the oldest jobs); `sample` is ordered oldest to newest.
- `oldestWaitAgeMs` covers both `wait` and `paused`.
- `acquireWaiting` / `withSlot(…, wait)` with an already aborted `signal` returns `limit` without taking a slot.
- `WorkerShutdown.close` rejects `deadlineMs` / `abortGraceMs` / `handBackDelayMs` outside `[0, 2³¹ − 1]` and `NaN`; the rejection is not cached.
- `acquireWaiting` rejects `pollMs` above `2³¹ − 1`.

### Fixed
- Orphan cleanup: candidates collected but not checked before `maxRunMs` are checked first on the next run; with several `scanClients` a node finished in the current cycle is not walked again until all nodes are done; a status index of the wrong type refuses only its queue.
- `release` wakes the first *live* waiter; a failed subscription is not cached forever; `FairShare` answers a repeated capture (`held`) with the group's share instead of `0`.

### Migration
- Code that builds `QueueHealth` by hand must add `unexpectedTypes`.

## [0.7.0] - 2026-11-17

### Added
- Support for **ioredis 6** and **BullMQ 6** (peers `^5 || ^6`). Orphan cleanup handles BullMQ 6.0.0–6.7.x (relay keys, pause without the `paused` list).
- Consumer matrix (`pnpm run test:consumers`) on the packed tarball.

### Changed (breaking)
- `CleanupRedis.zrange(key, start, stop)` takes `stop` as a string (ioredis 6 typing).
- `BULLMQ_SUPPORTED_VERSIONS` is `RANGES` — one `{ MIN, MAX_MINOR }` per tested major — instead of `MIN` / `MAX_MINOR` / `MAJOR`.
- A key `<queue>::…` (empty segment after the prefix) is never an orphan candidate.
- `QueueHealth.paused` (new required field) and metric `bullmq_queue_paused`.

### Migration
- Custom clients or test doubles: accept `stop: string` in `zrange`.
- Read the range of your major: `RANGES.find((r) => r.MIN[0] === major)`.
- Code that builds `QueueHealth` by hand must add `paused`.

## [0.6.0] - 2026-11-15

### Added
- `acquireWaiting` and `withSlot(…, { timeoutMs })`: wait for a release instead of polling.
- `GroupRateLimit` (token bucket per group, exact hint) and `FairShare` (fair share of one pool by weight).
- `/inspect`: `QueueInspector`, `toPrometheus`.
- `/bullmq`: `WorkerShutdown`; `attachDeadLetter`, `retryDeadLetter`.
- Orphan cleanup report mode (`dryRun` + `keptKeys`); the test suite also runs on a real Redis Cluster.

### Changed (breaking)
- Orphan cleanup no longer deletes hashes without `timestamp` by default (`deleteUntimestamped: true` restores it), requires `name` and `opts` next to `timestamp`, refuses a queue that contains another BullMQ queue under its prefix, caps `maxSnapshotListSize` at 1 000 000, accepts BullMQ 5 only up to minor 5.81, and rejects clients with ioredis `keyPrefix`. New `kept` fields `locked`, `notJob`, `nested` and report field `keptKeys`.
- `acquireOrDelay` backs off exponentially from `minDelayMs` instead of waiting for `retryAfterMs` (200 jobs, limit 10, `ttlMs` 30 s: 2.5 s instead of 91 s); an empty `token` is rejected; new option `jitterFraction`; `delayResolver(options)(retryAfterMs, delaysSoFar)`.
- `withSlot` with outcome `held` does not release the slot.
- `ttlMs` above `2³¹ − 1` is rejected; `GroupConcurrencyRedis` needs only `eval`; hash tags follow Redis's rule (`app{}{t}` is rejected in cluster mode).

### Fixed
- Orphan cleanup could delete live jobs of a queue whose prefix is nested in the cleaned one (data loss).
- `withSlot` did not notice a lost slot when the network froze (two holders at limit 1): the deadline now runs from the send time of the last confirmed renewal, renewals are single-flight, release is bounded by the slot lifetime.

### Migration
- Test fixtures that create job hashes by hand must write `name` and `opts`.
- Separate nested queue prefixes; set `maxSnapshotListSize: Infinity` for the old unbounded snapshot.
- Release `held` slots yourself only if you know the earlier capture is dead.

## [0.5.1] - 2026-11-15

### Changed
- Documentation only: why the orphan check walks a status list fully.

## [0.5.0] - 2026-11-15

### Added
- Orphan cleanup moved into this package as the `/orphan-cleanup` subpath (it used to be a separate package), with a BullMQ version gate: a queue whose `meta.version` is outside the tested range, or missing, is not cleaned and is listed in `report.unsupportedQueues`.
- The `exports` map is checked by a test; all exported constants are deep-frozen (`BULLMQ_KEYS.INTERNAL` is a frozen array, not a `Set`).

## [0.4.0] - 2026-11-15

### Added
- `withSlot(group, holder, slot, work)`: acquire, renew every `ttlMs / 3`, release in `finally`; `ttlMs` becomes the slot's return time after a crash, not the work duration. A lost slot aborts the work's signal with `SlotLostError`.
- `acquireOrDelay(…, { maxTotalWaitMs })`: a job waiting longer than the cap since creation fails with `UnrecoverableError`.
- Redis Cluster: a prefix without a hash tag is rejected in the constructor.

### Changed
- Documentation: a holder id must be unique system-wide (`` `${queue.name}:${job.id}` ``).

## Earlier versions

0.1.0–0.3.1 (2026-11-10 … 2026-11-14): `GroupConcurrency`, `reserve`, `ACQUIRE_OUTCOME`, explicit entry points.
