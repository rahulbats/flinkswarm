#!/usr/bin/env bash
# One worker agent in the foreground — for the 3-terminal demo.
#   scripts/worker.sh ClaimDataAgent
#   scripts/worker.sh PolicyDocAgent
# Set LLM_LOG_PROMPTS=1 to also print every model request/response.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

NAME="${1:?usage: scripts/worker.sh <AgentName from agent-spec.yaml>}"
banner "$NAME" "consumes agent.tasks.dispatched → produces agent.results.completed"
exec env AGENT_NAME="$NAME" "$PY" -m flinkswarm.worker
