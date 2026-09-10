#!/usr/bin/env bash
# Build + upload the AgentBarrier PTF and (re)create the persistent Flink
# statements that run the barrier on Confluent Cloud.
#
#   flink/create_barrier.sh            # full: build, upload, register, run
#   flink/create_barrier.sh --sql-only # skip the jar build/upload, just re-run the statement
#
# Override the target with env vars if this isn't the demo environment:
#   COMPUTE_POOL=lfcp-… DATABASE=lkc-… ENVIRONMENT=env-… ARTIFACT_URI=confluent-artifact://cfa-…/ver-… \
#     flink/create_barrier.sh --sql-only
set -euo pipefail
cd "$(dirname "$0")"

COMPUTE_POOL="${COMPUTE_POOL:-lfcp-j597202}"
DATABASE="${DATABASE:-lkc-gq935zm}"        # Kafka cluster id
ENVIRONMENT="${ENVIRONMENT:-env-0k5xq9}"
REGION="${REGION:-us-west-2}"
CLOUD="${CLOUD:-aws}"
EXPECTED_AGENTS="${EXPECTED_AGENTS:-2}"    # = number of spec.workers in agent-spec.yaml

fc() {  # confluent flink statement create <name> <sql> [extra flags...]
  local name="$1" sql="$2"; shift 2
  confluent flink statement delete "$name" --force >/dev/null 2>&1 || true
  confluent flink statement create "$name" \
    --compute-pool "$COMPUTE_POOL" --database "$DATABASE" --environment "$ENVIRONMENT" \
    --sql "$sql" "$@"
}

if [ "${1:-}" != "--sql-only" ]; then
  echo "== building PTF (JDK 21) =="
  export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home}"
  ( cd ptf && mvn -q clean package )

  echo "== uploading artifact =="
  ART=$(confluent flink artifact create flinkswarm-barrier-ptf \
        --artifact-file ptf/target/flinkswarm-barrier-ptf.jar \
        --cloud "$CLOUD" --region "$REGION" -o json)
  ARTIFACT_URI="confluent-artifact://$(echo "$ART" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["id"]+"/"+d["version"])')"
  echo "   $ARTIFACT_URI"
fi
: "${ARTIFACT_URI:?set ARTIFACT_URI=confluent-artifact://cfa-.../ver-... for --sql-only}"

echo "== the Kafka key column must be STRING (raw utf-8 claim_id) =="
fc fs-alter-key "ALTER TABLE \`agent.results.completed\` MODIFY \`key\` STRING;" --wait || true

echo "== register the PTF =="
fc fs-fn-agentbarrier \
  "CREATE FUNCTION AgentBarrier AS 'io.flinkswarm.flink.AgentBarrier' USING JAR '$ARTIFACT_URI';" --wait

echo "== barrier job (persistent) =="
fc fs-barrier-ptf \
  "INSERT INTO \`agent.synthesis.ready\`
   SELECT \`claim_id\`, \`aggregated_payload\`
   FROM AgentBarrier(
     input          => TABLE \`agent.results.completed\` PARTITION BY \`key\`,
     expectedAgents => $EXPECTED_AGENTS,
     uid            => 'flinkswarm-barrier-v4')" \
  --property sql.state-ttl='4 hours' \
  --property sql.tables.scan.startup.mode=earliest-offset

echo
echo "status:  confluent flink statement describe fs-barrier-ptf"
