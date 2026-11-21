#!/usr/bin/env bash
# Поднимает Redis Cluster из трёх мастеров, отдаёт адреса в
# REDIS_CLUSTER_TEST_NODES (`хост:порт,…`) и запускает переданную команду.
# Адреса уже заданы — ничего не поднимает. Есть redis-server (CI: `apk add
# redis`) — узлы процессами; нет — в ОДНОМ контейнере docker: узлы видят друг
# друга по 127.0.0.1, а клиент с хоста — по тем же портам (announce-ip
# 127.0.0.1, порты проброшены один в один). Всё снимается при любом исходе.
set -euo pipefail
if [[ -n "${REDIS_CLUSTER_TEST_NODES:-}" ]]; then
  exec "$@"
fi
IMAGE="${REDIS_TEST_IMAGE:-redis:7.4-alpine}"
NODE_ARGS="--cluster-enabled yes --cluster-announce-ip 127.0.0.1 --save '' --appendonly no --daemonize yes"
WORKDIR="$(mktemp -d)"
NAME=""
cleanup() {
  if [[ -n "${NAME}" ]]; then
    docker rm -f -v "${NAME}" >/dev/null 2>&1 || true
  fi
  if [[ -f "${WORKDIR}/pids" ]]; then
    while read -r pid; do kill "${pid}" 2>/dev/null || true; done < "${WORKDIR}/pids"
  fi
  rm -rf -- "${WORKDIR}"
}
trap cleanup EXIT

started=""
for _ in 1 2 3 4 5; do
  BASE=$(( 20000 + RANDOM % 20000 ))
  PORTS="${BASE} $((BASE + 1)) $((BASE + 2))"
  if command -v redis-server >/dev/null 2>&1; then
    ok=1
    for port in ${PORTS}; do
      if ! redis-server --port "${port}" --dir "${WORKDIR}" --cluster-config-file "nodes-${port}.conf" \
        --cluster-enabled yes --cluster-announce-ip 127.0.0.1 --save '' --appendonly no \
        --daemonize yes --pidfile "${WORKDIR}/${port}.pid" >/dev/null; then
        ok=0
      fi
    done
    sleep 0.3
    for port in ${PORTS}; do
      if [[ -f "${WORKDIR}/${port}.pid" ]]; then cat "${WORKDIR}/${port}.pid" >> "${WORKDIR}/pids"; fi
    done
    if [[ "${ok}" == 1 ]]; then
      started="local"
      break
    fi
  else
    NAME="bullmq-kit-test-cluster-$$-${BASE}"
    if docker run -d --rm --name "${NAME}" \
      -p "127.0.0.1:${BASE}:${BASE}" -p "127.0.0.1:$((BASE + 1)):$((BASE + 1))" -p "127.0.0.1:$((BASE + 2)):$((BASE + 2))" \
      "${IMAGE}" sh -c "for p in ${PORTS}; do redis-server --port \$p --cluster-config-file /tmp/nodes-\$p.conf ${NODE_ARGS}; done; sleep 86400" >/dev/null; then
      started="docker"
      break
    fi
    NAME=""
  fi
done
if [[ -z "${started}" ]]; then
  echo "Redis Cluster не поднялся: порты заняты или нет docker/redis-server" >&2
  exit 1
fi

cli() {
  if [[ "${started}" == "docker" ]]; then
    docker exec "${NAME}" redis-cli "$@"
  else
    redis-cli "$@"
  fi
}
for port in ${PORTS}; do
  for _ in $(seq 1 50); do
    if cli -p "${port}" ping 2>/dev/null | grep -q PONG; then break; fi
    sleep 0.2
  done
done
NODES=""
for port in ${PORTS}; do NODES="${NODES} 127.0.0.1:${port}"; done
# shellcheck disable=SC2086
cli --cluster create ${NODES} --cluster-replicas 0 --cluster-yes >/dev/null
for _ in $(seq 1 100); do
  if cli -p "${BASE}" cluster info | grep -q "cluster_state:ok"; then break; fi
  sleep 0.2
done
cli -p "${BASE}" cluster info | grep -q "cluster_state:ok" || { echo "кластер не собрался" >&2; exit 1; }
REDIS_CLUSTER_TEST_NODES="$(echo ${NODES} | tr ' ' ',')" "$@"
