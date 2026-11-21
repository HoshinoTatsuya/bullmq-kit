#!/usr/bin/env bash
# Матрица потребителей: собранный пакет (npm pack) ставится в одноразовые
# проекты с разными мажорами клиентов (ioredis 5/6, BullMQ 5/6, без BullMQ) и
# TypeScript (5.9, 6.0, 7). Установка — npm БЕЗ --legacy-peer-deps: npm строг
# к peerDependencies, и узкий peer пакета даёт ERESOLVE у каждого потребителя
# новой версии клиента (так ломалась 0.6.0 на ioredis 6). В каждом проекте:
# tsc над кодом, который передаёт НАСТОЯЩИЕ клиенты в API пакета (структурные
# порты обязаны принять их типы), проверка «ни один экспорт не стал any», и
# рантайм каждого входа пакета тем условием exports, которое выбирает
# потребитель (require для CJS, import для ESM).
#
# Запуск: scripts/consumer-matrix.sh (через run-serial, как и тесты).
# Каталоги — из mkdtemp, удаляются в конце.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/bullmq-kit-consumers-XXXXXX")"
cleanup() {
  case "${WORK}" in
    "${TMPDIR:-/tmp}"/bullmq-kit-consumers-*) rm -rf "${WORK}" ;;
  esac
}
trap cleanup EXIT

(cd "${ROOT}" && pnpm build >/dev/null && npm pack --silent --pack-destination "${WORK}" >/dev/null)
TARBALL="$(ls "${WORK}"/hoshinotatsuya-bullmq-kit-*.tgz)"

# Код потребителя: настоящие Redis/Cluster ioredis и Worker/Queue/Job BullMQ — в API пакета.
CONSUMER_REDIS_TS="import { Cluster, Redis } from 'ioredis';
import { FairShare, GroupConcurrency, GroupRateLimit, type GroupConcurrencyRedis, type SlotSubscriber } from '@hoshinotatsuya/bullmq-kit';
import { BULLMQ_SUPPORTED_VERSIONS, OrphanedJobsCleanup, unsupportedBullmqVersion, type CleanupRedis } from '@hoshinotatsuya/bullmq-kit/orphan-cleanup';
import { QueueInspector, toPrometheus, type QueueHealth } from '@hoshinotatsuya/bullmq-kit/inspect';

// skipLibCheck прячет ошибки в декларациях пакета, а нерезолвящийся импорт в них
// молча становится any. Утверждение ниже не компилируется, если хоть один экспорт стал any.
type IsAny<T> = 0 extends 1 & T ? true : false;
type NotAny<T> = IsAny<T> extends true ? never : true;
export const typed: [
  NotAny<typeof OrphanedJobsCleanup>, NotAny<typeof QueueInspector>, NotAny<typeof GroupConcurrency>,
  NotAny<QueueHealth>, NotAny<typeof BULLMQ_SUPPORTED_VERSIONS>,
] = [true, true, true, true, true];

declare const redis: Redis;
declare const cluster: Cluster;
export const ports: [CleanupRedis, CleanupRedis, GroupConcurrencyRedis, SlotSubscriber] = [redis, cluster, cluster, redis.duplicate()];
export const cleanup = new OrphanedJobsCleanup(redis, { queuePrefixes: ['bull:q'] });
export const clusterCleanup = new OrphanedJobsCleanup(cluster, { queuePrefixes: ['{bull}:q'], scanClients: () => cluster.nodes('master') });
export const inspector = new QueueInspector(redis, { queuePrefixes: ['bull:q'] });
export const slots = new GroupConcurrency(redis, { prefix: 'slots', subscriber: redis.duplicate() });
export const fair = new FairShare(cluster, { prefix: '{fair}' });
export const rate = new GroupRateLimit(redis, { prefix: 'rate' });
export const paused = (health: QueueHealth): boolean => health.paused;
export const text: string = toPrometheus([]);
export const latestSupported: string | null = unsupportedBullmqVersion('bullmq:6.7.7');
"

CONSUMER_BULLMQ_TS="import { Job, Queue, Worker } from 'bullmq';
import { acquireOrDelay, attachDeadLetter, retryDeadLetter, WorkerShutdown, type DeadLetterEntry } from '@hoshinotatsuya/bullmq-kit/bullmq';
import { slots } from './redis-consumer';

declare const worker: Worker;
declare const queue: Queue;
declare const deadLetters: Queue<DeadLetterEntry>;
declare const job: Job;
export const shutdown = new WorkerShutdown().attach(worker);
export const detach = attachDeadLetter(worker, deadLetters);
export const delayed = (token: string) =>
  acquireOrDelay(job, token, () => slots.acquireDetailed('g', String(job.id), { limit: 1, ttlMs: 1_000 }));
export const retried = (entry: Job<DeadLetterEntry>) => retryDeadLetter(entry, queue);
"

# Рантайм: каждый вход грузится, уборка строится над ленивым клиентом (без Redis),
# а гейт версии принимает последнюю BullMQ своего мажора.
RUNTIME_BODY='
  const { Redis } = ioredis;
  const redis = new Redis({ lazyConnect: true });
  const cleanup = new orphan.OrphanedJobsCleanup(redis, { queuePrefixes: ["bull:q"] });
  const inspector = new inspect.QueueInspector(redis, { queuePrefixes: ["bull:q"] });
  const slots = new root.GroupConcurrency(redis, { prefix: "slots" });
  if (!cleanup || !inspector || !slots || (kit !== null && typeof kit.WorkerShutdown !== "function")) process.exit(1);
  if (orphan.unsupportedBullmqVersion("bullmq:6.7.7") !== null || orphan.unsupportedBullmqVersion("bullmq:5.81.5") !== null) process.exit(2);
  redis.disconnect();
'

failures=0
run_case() {
  local label="$1" type="$2" module="$3" resolution="$4" ioredis="$5" bullmq="$6" typescript="$7" types_node="$8"
  local dir="${WORK}/${label}"
  mkdir -p "${dir}/src"
  local bullmq_dep=""
  if [[ -n "${bullmq}" ]]; then
    bullmq_dep="\"bullmq\": \"${bullmq}\","
  fi
  cat >"${dir}/package.json" <<JSON
{
  "name": "consumer-${label}",
  "private": true,
  "type": "${type}",
  "dependencies": {
    "@hoshinotatsuya/bullmq-kit": "file:${TARBALL}",
    ${bullmq_dep}
    "ioredis": "${ioredis}"
  },
  "devDependencies": {
    "@types/node": "${types_node}",
    "typescript": "${typescript}"
  }
}
JSON
  cat >"${dir}/tsconfig.json" <<JSON
{
  "compilerOptions": {
    "module": "${module}",
    "moduleResolution": "${resolution}",
    "target": "ES2023",
    "strict": true,
    "exactOptionalPropertyTypes": true,
    "noUncheckedIndexedAccess": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "types": ["node"],
    "noEmit": true
  },
  "include": ["src"]
}
JSON
  printf '%s' "${CONSUMER_REDIS_TS}" >"${dir}/src/redis-consumer.ts"
  if [[ -n "${bullmq}" ]]; then
    # ESM с nodenext требует расширение в относительном импорте.
    if [[ "${type}" == "module" ]]; then
      printf '%s' "${CONSUMER_BULLMQ_TS}" | sed "s#'./redis-consumer'#'./redis-consumer.js'#" >"${dir}/src/bullmq-consumer.ts"
    else
      printf '%s' "${CONSUMER_BULLMQ_TS}" >"${dir}/src/bullmq-consumer.ts"
    fi
  fi
  if ! (cd "${dir}" && npm install --no-audit --no-fund --loglevel=error >"${dir}/install.log" 2>&1); then
    echo "${label}: установка npm (без --legacy-peer-deps) — КРАСНАЯ"; grep -E 'ERESOLVE|peer|error' "${dir}/install.log" | head -6
    failures=$((failures + 1)); return
  fi
  local installed
  installed="$(cd "${dir}" && node -p 'const v = (n) => { try { return require(n + "/package.json").version } catch { return "—" } }; ["ioredis " + v("ioredis"), "bullmq " + v("bullmq"), "typescript " + v("typescript")].join(", ")')"
  echo "${label}: установлено ${installed}"
  if (cd "${dir}" && node_modules/.bin/tsc -p tsconfig.json >"${dir}/tsc.log" 2>&1); then
    echo "${label}: tsc — зелёный"
  else
    echo "${label}: tsc — КРАСНЫЙ"; head -8 "${dir}/tsc.log"; failures=$((failures + 1))
  fi
  # Без BullMQ вход /bullmq не грузится (bullmq — необязательный peer); остальные обязаны.
  local runtime kit_esm="null" kit_cjs="null"
  if [[ -n "${bullmq}" ]]; then
    kit_esm="await import('@hoshinotatsuya/bullmq-kit/bullmq')"
    kit_cjs="require('@hoshinotatsuya/bullmq-kit/bullmq')"
  fi
  if [[ "${type}" == "module" ]]; then
    runtime="const ioredis = await import('ioredis'); const root = await import('@hoshinotatsuya/bullmq-kit'); const kit = ${kit_esm}; const orphan = await import('@hoshinotatsuya/bullmq-kit/orphan-cleanup'); const inspect = await import('@hoshinotatsuya/bullmq-kit/inspect'); ${RUNTIME_BODY}"
  else
    runtime="const ioredis = require('ioredis'); const root = require('@hoshinotatsuya/bullmq-kit'); const kit = ${kit_cjs}; const orphan = require('@hoshinotatsuya/bullmq-kit/orphan-cleanup'); const inspect = require('@hoshinotatsuya/bullmq-kit/inspect'); ${RUNTIME_BODY}"
  fi
  if (cd "${dir}" && node --input-type="${type}" -e "${runtime}" >"${dir}/runtime.log" 2>&1); then
    echo "${label}: рантайм входов — зелёный"
  else
    echo "${label}: рантайм входов — КРАСНЫЙ"; head -5 "${dir}/runtime.log"; failures=$((failures + 1))
  fi
}

# label | type | module | resolution | ioredis | bullmq | typescript | @types/node
run_case latest-esm-ts7 module nodenext nodenext 6.0.0 6.7.7 7.0.2 26.6.6
run_case latest-cjs-ts6 commonjs node16 node16 6.0.0 6.7.7 6.0.3 24.0.15
run_case bullmq5-ioredis6 commonjs nodenext nodenext 6.0.0 5.81.5 5.9.3 24.0.15
run_case bullmq6-ioredis5 commonjs nodenext nodenext 5.11.1 6.0.0 6.0.3 24.0.15
run_case oldest commonjs node16 node16 5.11.1 5.72.0 5.9.3 24.0.15
run_case without-bullmq module nodenext nodenext 6.0.0 "" 7.0.2 26.6.6

if [[ "${failures}" -gt 0 ]]; then
  echo "матрица потребителей: красных ${failures}"
  exit 1
fi
echo "матрица потребителей: все зелёные"
