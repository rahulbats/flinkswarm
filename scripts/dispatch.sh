#!/usr/bin/env bash
# Kick off one claim — the fourth terminal, run on demand.
#   scripts/dispatch.sh CLM-1001
#   scripts/dispatch.sh CLM-1002 "Adjudicate coverage for claim CLM-1002."
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

CLAIM="${1:?usage: scripts/dispatch.sh <claim_id> [prompt]}"
PROMPT="${2:-Adjudicate coverage for claim ${CLAIM}.}"
exec "$PY" -m flinkswarm.orchestrator dispatch --claim "$CLAIM" --prompt "$PROMPT"
