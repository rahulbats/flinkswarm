#!/usr/bin/env bash
# Full reset — wipe Kafka data AND Flink state, then rebuild the pipeline.
# Use before a fresh demo when you want to reuse claim ids like CLM-1001.
#
#   scripts/reset.sh              # reuse the uploaded PTF jar (fast)
#   scripts/reset.sh --rebuild    # also rebuild + re-upload the PTF jar
#
# Env overrides (defaults are the demo environment):
#   COMPUTE_POOL=lfcp-… DATABASE=lkc-… ENVIRONMENT=env-… scripts/reset.sh
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

COMPUTE_POOL="${COMPUTE_POOL:-lfcp-j597202}"
DATABASE="${DATABASE:-lkc-gq935zm}"
ENVIRONMENT="${ENVIRONMENT:-env-0k5xq9}"
TOPICS=(agent.tasks.dispatched agent.results.completed agent.synthesis.ready agent.decisions.final)

banner "RESET" "wipes all 4 topics + Flink barrier state, then rebuilds"

echo "== stop the barrier =="
confluent flink statement delete fs-barrier-ptf --force >/dev/null 2>&1 || true

echo "== drop the Flink-owned synthesis table (removes its topic + schema) =="
confluent flink statement delete fs-reset-drop --force >/dev/null 2>&1 || true
confluent flink statement create fs-reset-drop \
  --compute-pool "$COMPUTE_POOL" --database "$DATABASE" --environment "$ENVIRONMENT" --wait \
  --sql "DROP TABLE IF EXISTS \`agent.synthesis.ready\`;" >/dev/null || true

echo "== delete the data topics =="
for t in "${TOPICS[@]}"; do
  confluent kafka topic delete "$t" --force >/dev/null 2>&1 && echo "   - $t" || echo "   . $t (already gone)"
done

echo "== recreate topics + register schemas =="
"$ROOT/scripts/create_topics.sh"
"$PY" -m flinkswarm.register_schemas

echo "== rebuild the Flink barrier (fresh state via a new uid) =="
export BARRIER_UID="flinkswarm-barrier-$(date +%s)"
if [ "${1:-}" = "--rebuild" ]; then
  COMPUTE_POOL="$COMPUTE_POOL" DATABASE="$DATABASE" ENVIRONMENT="$ENVIRONMENT" "$ROOT/flink/create_barrier.sh"
else
  COMPUTE_POOL="$COMPUTE_POOL" DATABASE="$DATABASE" ENVIRONMENT="$ENVIRONMENT" "$ROOT/flink/create_barrier.sh" --sql-only
fi

echo
echo "reset complete. start the panes, then: scripts/dispatch.sh CLM-1001"
