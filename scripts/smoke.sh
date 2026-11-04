#!/usr/bin/env bash
# Start the server separately, then run this smoke test. Requires curl only.
set -euo pipefail
base="${BASE_URL:-http://127.0.0.1:8080}"
health="$(curl --fail --silent --show-error "$base/healthz")"
[[ "$health" == *'"ok"'* ]] || { echo "Health check failed" >&2; exit 1; }
body='{"kind":"sum","payload":{"numbers":[4,5]},"max_attempts":2}'
resp="$(curl --fail --silent --show-error -X POST "$base/v1/jobs" -H 'Content-Type: application/json' -H "Idempotency-Key: smoke-$(date +%s)-$$" -d "$body")"
[[ "$resp" == *'"state":"queued"'* || "$resp" == *'"state":"running"'* || "$resp" == *'"state":"succeeded"'* ]] || { echo "Unexpected response: $resp" >&2; exit 1; }
# Extract the hex ID without requiring jq.
job_id="$(printf '%s' "$resp" | sed -n 's/.*"id":"\([a-f0-9]*\)".*/\1/p')"
[[ -n "$job_id" ]] || { echo "No job id in $resp" >&2; exit 1; }
for _ in {1..40}; do
  result="$(curl --fail --silent --show-error "$base/v1/jobs/$job_id")"
  if [[ "$result" == *'"state":"succeeded"'* && "$result" == *'"sum":9'* ]]; then
    echo "PASS: job $job_id succeeded with sum=9"
    exit 0
  fi
  sleep 0.1
done
echo "FAIL: job did not succeed: $result" >&2
exit 1
