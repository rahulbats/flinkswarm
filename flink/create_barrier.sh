#!/usr/bin/env bash
# (Re)create the persistent Flink barrier statement `fs-barrier`.
#
# The barrier is the INSERT in setup_queries.sql. It has to be a PERSISTENT
# statement (confluent flink statement create) — a query run in the web
# workspace or `flink shell` stops when that session ends.
#
#   flink/create_barrier.sh
#
# Override the target with env vars if this isn't the demo environment:
#   COMPUTE_POOL=lfcp-xxxx DATABASE=lkc-xxxx ENVIRONMENT=env-xxxx flink/create_barrier.sh
set -euo pipefail

COMPUTE_POOL="${COMPUTE_POOL:-lfcp-j597202}"
DATABASE="${DATABASE:-lkc-gq935zm}"       # Kafka cluster id
ENVIRONMENT="${ENVIRONMENT:-env-0k5xq9}"
NAME="${NAME:-fs-barrier}"

SQL="INSERT INTO \`agent.synthesis.ready\`
SELECT \`claim_id\`,
       COUNT(*) AS \`agent_count\`,
       LISTAGG(\`agent_name\` || ': ' || \`latest_result\`, ' ||| ') AS \`aggregated_payload\`
FROM (
  SELECT \`claim_id\`, \`agent_name\`, LAST_VALUE(\`result\`) AS \`latest_result\`
  FROM \`agent.results.completed\`
  GROUP BY \`claim_id\`, \`agent_name\`
)
GROUP BY \`claim_id\`;"

# drop any existing statement of the same name first (idempotent re-runs)
confluent flink statement delete "$NAME" --force 2>/dev/null || true

confluent flink statement create "$NAME" \
  --compute-pool "$COMPUTE_POOL" \
  --database "$DATABASE" \
  --environment "$ENVIRONMENT" \
  --wait \
  --property sql.state-ttl='4 hours' \
  --sql "$SQL"

echo
echo "status:  confluent flink statement describe $NAME"
