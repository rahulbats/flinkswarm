#!/usr/bin/env bash
# Create the FlinkSwarm topics on Confluent Cloud.
# Prereqs: `confluent login`, and `confluent kafka cluster use <id>`.
set -euo pipefail

PARTITIONS="${PARTITIONS:-6}"
# agent.synthesis.ready is NOT here — Flink owns it (created by CREATE TABLE in
# flink/create_barrier.sh, so it gets a typed schema, not raw BYTES).
TOPICS=(
  "agent.tasks.dispatched"
  "agent.results.completed"
  "agent.decisions.final"
)

for t in "${TOPICS[@]}"; do
  if confluent kafka topic describe "$t" >/dev/null 2>&1; then
    echo "= $t (exists)"
  else
    confluent kafka topic create "$t" --partitions "$PARTITIONS"
    echo "+ $t"
  fi
done
