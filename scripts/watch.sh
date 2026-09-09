#!/usr/bin/env bash
# Tail a topic — handy as a fifth pane during the demo.
#   scripts/watch.sh decisions      (default)
#   scripts/watch.sh results | synthesis | tasks
set -euo pipefail
case "${1:-decisions}" in
  tasks)     T=agent.tasks.dispatched ;;
  results)   T=agent.results.completed ;;
  synthesis) T=agent.synthesis.ready ;;
  decisions) T=agent.decisions.final ;;
  *) T="$1" ;;
esac
echo "consuming $T  (Ctrl-C to stop)"
exec confluent kafka topic consume "$T" -b --print-key
