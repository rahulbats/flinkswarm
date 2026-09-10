#!/usr/bin/env bash
# Build + upload the AgentBarrier PTF and (re)create the persistent Flink
# statements that run the barrier on Confluent Cloud.
#
#   flink/create_barrier.sh            # full: build jar, re-upload, register, run
#   flink/create_barrier.sh --sql-only # reuse the uploaded artifact, just (re)run
#
# Override the target with env vars if this isn't the demo environment:
#   COMPUTE_POOL=lfcp-… DATABASE=lkc-… ENVIRONMENT=env-… flink/create_barrier.sh
set -euo pipefail
cd "$(dirname "$0")"

COMPUTE_POOL="${COMPUTE_POOL:-lfcp-j597202}"
DATABASE="${DATABASE:-lkc-gq935zm}"        # Kafka cluster id
ENVIRONMENT="${ENVIRONMENT:-env-0k5xq9}"
REGION="${REGION:-us-west-2}"
CLOUD="${CLOUD:-aws}"
EXPECTED_AGENTS="${EXPECTED_AGENTS:-2}"    # = number of spec.workers in agent-spec.yaml
BARRIER_UID="${BARRIER_UID:-flinkswarm-barrier-v4}"
ART_NAME="flinkswarm-barrier-ptf"

fc() {  # confluent flink statement create <name> <sql> [extra flags...]
  local name="$1" sql="$2"; shift 2
  confluent flink statement delete "$name" --force >/dev/null 2>&1 || true
  confluent flink statement create "$name" \
    --compute-pool "$COMPUTE_POOL" --database "$DATABASE" --environment "$ENVIRONMENT" \
    --sql "$sql" "$@"
}

if [ "${1:-}" = "--sql-only" ]; then
  ID=$(confluent flink artifact list --cloud "$CLOUD" --region "$REGION" -o json \
        | python3 -c "import sys,json;print(next(a['id'] for a in json.load(sys.stdin) if a['name']=='$ART_NAME'))")
  VER=$(confluent flink artifact describe "$ID" --cloud "$CLOUD" --region "$REGION" --environment "$ENVIRONMENT" -o json \
        | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('version') or d.get('versions',[{}])[-1].get('version'))")
  ARTIFACT_URI="confluent-artifact://$ID/$VER"
else
  echo "== building PTF (JDK 21) =="
  export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home}"
  ( cd ptf && mvn -q clean package )

  echo "== uploading artifact =="
  OLD=$(confluent flink artifact list --cloud "$CLOUD" --region "$REGION" -o json \
        | python3 -c "import sys,json;print(next((a['id'] for a in json.load(sys.stdin) if a['name']=='$ART_NAME'),''))")
  [ -n "$OLD" ] && confluent flink artifact delete "$OLD" --cloud "$CLOUD" --region "$REGION" --force >/dev/null 2>&1 || true
  ART=$(confluent flink artifact create "$ART_NAME" \
        --artifact-file ptf/target/flinkswarm-barrier-ptf.jar \
        --cloud "$CLOUD" --region "$REGION" -o json)
  ARTIFACT_URI="confluent-artifact://$(echo "$ART" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["id"]+"/"+d["version"])')"
fi
echo "   artifact: $ARTIFACT_URI"

echo "== key column must be STRING (raw utf-8 claim_id) =="
fc fs-alter-key "ALTER TABLE \`agent.results.completed\` MODIFY \`key\` STRING;" --wait || true

echo "== output table (append, Flink-owned) =="
fc fs-synth-drop "DROP TABLE IF EXISTS \`agent.synthesis.ready\`;" --wait
fc fs-synth-create \
  "CREATE TABLE \`agent.synthesis.ready\` (\`claim_id\` STRING, \`aggregated_payload\` STRING)
   DISTRIBUTED BY HASH(\`claim_id\`) INTO 6 BUCKETS
   WITH ('changelog.mode' = 'append', 'value.format' = 'json-registry');" --wait

echo "== (re)register the PTF =="
fc fs-fn-drop "DROP FUNCTION IF EXISTS AgentBarrier;" --wait
fc fs-fn-agentbarrier \
  "CREATE FUNCTION AgentBarrier AS 'io.flinkswarm.flink.AgentBarrier' USING JAR '$ARTIFACT_URI';" --wait

echo "== barrier job (persistent, uid=$BARRIER_UID) =="
fc fs-barrier-ptf \
  "INSERT INTO \`agent.synthesis.ready\`
   SELECT \`claim_id\`, \`aggregated_payload\`
   FROM AgentBarrier(
     input          => TABLE \`agent.results.completed\` PARTITION BY \`key\`,
     expectedAgents => $EXPECTED_AGENTS,
     uid            => '$BARRIER_UID')" \
  --property sql.state-ttl='4 hours' \
  --property sql.tables.scan.startup.mode=earliest-offset

echo
echo "status:  confluent flink statement describe fs-barrier-ptf"
