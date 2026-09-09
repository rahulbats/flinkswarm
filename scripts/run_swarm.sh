#!/usr/bin/env bash
# Run the whole swarm locally against Confluent Cloud (one process per agent
# + the orchestrator service). Ctrl-C stops everything.
set -euo pipefail
cd "$(dirname "$0")/.."

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

AGENT_NAME=ClaimDataAgent python -m flinkswarm.worker &
pids+=($!)
AGENT_NAME=PolicyDocAgent python -m flinkswarm.worker &
pids+=($!)
python -m flinkswarm.orchestrator serve &
pids+=($!)

echo "swarm up (pids: ${pids[*]}). Dispatch a task with:"
echo "  python -m flinkswarm.orchestrator dispatch --claim CLM-1001 --prompt 'Adjudicate coverage for CLM-1001.'"
wait
