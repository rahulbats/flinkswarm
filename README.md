# FlinkSwarm

An event-driven **agent swarm** on **Confluent Cloud Kafka** with a **Flink**
barrier that does scatter/gather across sub-agents.

```
                    agent.tasks.dispatched
  orchestrator ────────────────┬───────────────► ClaimDataAgent  (KEDA-scaled)
   (dispatch)                   └───────────────► PolicyDocAgent  (KEDA-scaled)
                                                        │
                                                        │ agent.results.completed
                                                        ▼
                                        ┌───────────────────────────────┐
                                        │  Flink: AgentBarrierAggregator │
                                        │  PARTITION BY `key` (claim_id) │
                                        │  wait for N agents to report   │
                                        └───────────────┬───────────────┘
                                                        │ agent.synthesis.ready
                                                        ▼
                                            orchestrator (serve)
                                                        │ agent.decisions.final
                                                        ▼
```

`claim_id` is the Kafka message key on every topic (raw string), so the barrier
partitions with no reshuffle and everything co-partitions on the claim.

Each worker consumes the **same** task topic under its own consumer group and
applies its own instructions + tools (from `agent-spec.yaml`). The Flink PTF
buffers results per claim and emits one aggregated payload once every expected
agent has reported. The orchestrator turns that into a final decision.

## Layout

| Path | What |
|---|---|
| `agent-spec.yaml` | The swarm definition (topics, workers, tools, instructions, barrier). Shape mirrors the future Go operator's CRD. |
| `flinkswarm/config.py` | Env settings + spec loader |
| `flinkswarm/events.py` | Wire events, field-aligned with the Flink DDL |
| `flinkswarm/kafka.py` | Confluent producer/consumer factories (SASL_SSL) |
| `flinkswarm/llm.py` | Tool-calling agent over an OpenAI-compatible endpoint (local MLX / Qwen) |
| `flinkswarm/tools/` | `TOOL_REGISTRY` + `get_claim_from_cosmos`, `get_policy_from_blob` (mocked) |
| `flinkswarm/worker.py` | `GenericSwarmWorker` — one process per agent |
| `flinkswarm/orchestrator.py` | `dispatch` (produce task) + `serve` (consume barrier → decide) |
| `flink/src/main/java/io/flinkswarm/flink/AgentBarrierAggregator.java` | PTF barrier (Flink 2.0) |
| `flink/pom.xml` | Builds the UDF jar to upload as a Confluent Flink artifact (build with JDK 21) |
| `flink/setup_queries.sql` | Confluent Cloud Flink DDL + the barrier job |
| `k8s/` | Deployment + KEDA ScaledObject (lag-based autoscaling) |
| `scripts/` | `create_topics.sh`, `run_swarm.sh` |

## Prerequisites

- Python 3.11+
- A local MLX server exposing an OpenAI-compatible API, e.g.
  `mlx_lm.server --model mlx-community/Qwen2.5-7B-Instruct-4bit --port 10240`
- A Confluent Cloud Kafka cluster + API key/secret
- Schema Registry enabled on the environment (Stream Governance Essentials, free)
  + a Schema Registry API key/secret
- Confluent Cloud for Apache Flink (compute pool) for the barrier
- `confluent` CLI, and **JDK 21** + Maven to build the PTF jar

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env   # Confluent Cloud Kafka + Schema Registry + MLX values

confluent login
confluent kafka cluster use <cluster-id>
./scripts/create_topics.sh
```

Values go through Schema Registry as JSON Schema when `SCHEMA_REGISTRY_URL` is
set (that's also what makes the topics show up as typed Flink tables). Unset, the
workers fall back to plain JSON — fine for a local sanity check, not for Flink.

### 1. Register the schemas

Creating the topics does **not** create schemas ("data contracts"). Register the
three the Python side produces (`agent.synthesis.ready` is created by Flink):

```bash
python -m flinkswarm.register_schemas          # tasks / results / decisions
python -m flinkswarm.register_schemas --check   # show what's registered
```

### 2. Flink barrier

Build with **JDK 21** — Confluent Cloud rejects artifacts built with a newer
JDK, and the jar manifest records the build JDK, so `--release` alone is not
enough. There is no CLI "update" for an artifact — delete + recreate to push a
new jar.

```bash
cd flink
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
mvn clean package                   # -> target/flinkswarm-flink-udf.jar
confluent flink artifact create flinkswarm-barrier \
    --artifact-file target/flinkswarm-flink-udf.jar --cloud aws --region us-west-2 -o json
```

Then run the statements in `flink/setup_queries.sql` (easiest in
`confluent flink shell --compute-pool <lfcp-…>`), pasting the printed artifact
id + version into the `USING JAR 'confluent-artifact://…'` line. The
`agent.results.completed` table is inferred from the schema registered in step 1;
the `ALTER TABLE … MODIFY \`key\` STRING` in the file turns its raw key column
from `VARBINARY` into a usable `STRING`.

## Run locally

```bash
./scripts/run_swarm.sh        # 2 workers + orchestrator serve loop

# in another shell:
python -m flinkswarm.orchestrator dispatch \
    --claim CLM-1001 --prompt "Adjudicate coverage for claim CLM-1001."
```

Watch `agent.decisions.final` with `confluent kafka topic consume agent.decisions.final -b`.

## Tests

```bash
pytest -q          # unit tests, no Kafka/LLM needed (agent loop is faked)
```

## Notes / TODO

- **Barrier timeout**: Flink 2.0 PTF has no timer API, so a claim where an agent
  never reports stays buffered in the barrier forever. Workers always emit a
  result row (even on failure), so only a hard crash / lost message stalls a
  claim. Add a deadline sweeper to the orchestrator, or move to Flink 2.1 PTF
  timers once the Confluent Cloud runtime supports them.
- **Tools are mocked** (`_FAKE_CLAIMS` / `_FAKE_POLICIES` in `flinkswarm/tools/`).
  Wire them to the real claim/policy store.
- **Go operator** (later): reconcile `SwarmDeployment` → Deployments + KEDA
  ScaledObjects + the Flink statement. `agent-spec.yaml` is already shaped for it.
```
