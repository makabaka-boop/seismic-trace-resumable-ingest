#!/bin/sh
# Regression entrypoint that exercises the compose API_PORT override.
#
# Brings up db+api with the host-side API port overridden (default 9000),
# confirms the API answers through that published port, and then runs the
# original acceptance suite (compaction tests excluded) in the one-shot
# verify container over the compose network.
#
# Usage:
#   API_PORT=9000 ./scripts/run_regression_with_port.sh
set -e

PORT="${API_PORT:-9000}"
export API_PORT="$PORT"

echo ">> Starting stack with host API port ${PORT}"
docker compose up --build -d db api

echo ">> Waiting for http://localhost:${PORT}/health"
i=0
until curl -fsS "http://localhost:${PORT}/health" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 60 ]; then
    echo "API did not become healthy on port ${PORT}" >&2
    docker compose logs api >&2
    exit 1
  fi
  sleep 1
done
echo ">> API reachable through overridden port ${PORT}"

echo ">> Running original regression suite (compaction tests excluded)"
docker compose --profile verify run --rm --build verify \
  pytest -v --tb=short --ignore=tests/test_07_compaction.py
