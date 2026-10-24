#!/usr/bin/env bash
# Cluster health: broker controller quorum + schema registry reachability.
# Exits non-zero when either part is unhealthy.
set -uo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$dir/compose.yaml"
if [ -f "$dir/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$dir/.env"
  set +a
fi
kafka_port="${KAFKA_HOST_PORT:-9092}"
registry_port="${REGISTRY_HOST_PORT:-8081}"
status=0

quorum="$(docker compose -f "$compose_file" exec -T kafka \
  /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9093 describe --status 2>/dev/null)"
if printf '%s' "$quorum" | grep -qE '^LeaderId:[[:space:]]*[0-9]+'; then
  leader="$(printf '%s' "$quorum" | sed -n 's/^LeaderId:[[:space:]]*//p')"
  voters="$(printf '%s' "$quorum" | grep -c '"id"')"
  echo "broker: ok (leader $leader, $voters voter(s), port $kafka_port)"
else
  echo "broker: FAIL - no active controller quorum leader"
  status=1
fi

if curl -fsS "http://localhost:${registry_port}/subjects" >/dev/null 2>&1; then
  echo "schema-registry: ok (port $registry_port)"
else
  echo "schema-registry: FAIL - not reachable on port $registry_port"
  status=1
fi

exit $status
