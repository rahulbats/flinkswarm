#!/usr/bin/env bash
# The orchestrator serve loop in the foreground — third demo terminal.
#   scripts/orchestrator.sh
# ORCHESTRATOR_GROUP=<name> to re-read agent.synthesis.ready from the start.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

banner "CoverageOrchestrator" "consumes agent.synthesis.ready → produces agent.decisions.final"
exec "$PY" -m flinkswarm.orchestrator serve
