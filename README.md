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
                                        │  Flink SQL: GROUP BY claim_id  │
                                        │  COUNT(DISTINCT agent_name)    │
                                        │  + LISTAGG(result)  → upsert   │
                                        └───────────────┬───────────────┘
                                                        │ agent.synthesis.ready (upsert)
                                                        ▼
                                            orchestrator (serve)
                                        acts once agent_count == N, once/claim
                                                        │ agent.decisions.final
                                                        ▼
```

`claim_id` is the Kafka message key on every topic.

Each worker consumes the **same** task topic under its own consumer group and
applies its own instructions + tools (from `agent-spec.yaml`). Flink groups
results per claim and emits a growing upsert row (`agent_count`, concatenated
findings); the orchestrator waits until `agent_count` equals the worker count,
then synthesizes a decision — once per claim.

> The barrier is **pure Flink SQL** because Confluent Cloud for Apache Flink does
> not support `ProcessTableFunction` (only stateless scalar/table UDFs). A PTF
> version for open-source Flink is in `flink/ptf-open-source-only/`.

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
| `flink/setup_queries.sql` | Confluent Cloud Flink: the pure-SQL upsert barrier |
| `flink/ptf-open-source-only/` | PTF barrier for open-source Flink (unsupported on Confluent Cloud) |
| `k8s/` | Deployment + KEDA ScaledObject (lag-based autoscaling) |
| `scripts/` | `worker.sh` · `orchestrator.sh` · `dispatch.sh` · `watch.sh` · `create_topics.sh` |

## Prerequisites

- Python 3.11+
- A local MLX server exposing an OpenAI-compatible API, e.g.
  `mlx_lm.server --model mlx-community/Qwen2.5-7B-Instruct-4bit --port 10240`
- A Confluent Cloud Kafka cluster + API key/secret
- Schema Registry enabled on the environment (Stream Governance Essentials, free)
  + a Schema Registry API key/secret
- Confluent Cloud for Apache Flink (a compute pool) for the barrier
- `confluent` CLI

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

No jar to build — it's pure SQL. In the Flink shell:

```bash
confluent flink shell --compute-pool <lfcp-…> --environment <env-…> --database <lkc-…>
```

1. `CREATE TABLE \`agent.synthesis.ready\` (…) WITH ('changelog.mode' = 'upsert', …)` — in `confluent flink shell`.
2. The `INSERT … GROUP BY claim_id` barrier — create it as a **persistent** statement, not in the shell / web workspace (those stop when you disconnect):

```bash
confluent flink statement create fs-barrier \
  --compute-pool <lfcp-…> --database <lkc-…> --environment <env-…> \
  --wait --property sql.state-ttl='4 hours' \
  --sql "INSERT INTO \`agent.synthesis.ready\` SELECT \`claim_id\`, COUNT(*) AS \`agent_count\`, LISTAGG(\`agent_name\` || ': ' || \`latest_result\`, ' ||| ') AS \`aggregated_payload\` FROM (SELECT \`claim_id\`, \`agent_name\`, LAST_VALUE(\`result\`) AS \`latest_result\` FROM \`agent.results.completed\` GROUP BY \`claim_id\`, \`agent_name\`) GROUP BY \`claim_id\`;"
```

`agent.results.completed` is inferred as a table from the registered schema —
nothing to create. Check the barrier with `confluent flink statement describe fs-barrier`.

## Run locally

**Demo layout — one process per pane** (each script prints a banner, then that
component's log — clean to narrate):

| Pane | Command | Does |
|---|---|---|
| 1 | `scripts/worker.sh ClaimDataAgent` | consumes `tasks.dispatched` → `results.completed` |
| 2 | `scripts/worker.sh PolicyDocAgent` | same, its own consumer group |
| 3 | `scripts/orchestrator.sh` | consumes `synthesis.ready` → `decisions.final` |
| 4 | `scripts/dispatch.sh CLM-1001` | kicks off one claim |
| 5 | `scripts/watch.sh decisions` | tails the final decision topic |

Add `LLM_LOG_PROMPTS=1` in front of a `worker.sh` / `orchestrator.sh` to print
every model request and response in that pane.

**One-shot** (all three in the background, multiplexed):

```bash
./scripts/run_swarm.sh
scripts/dispatch.sh CLM-1001
```

## Tests

```bash
pytest -q          # unit tests, no Kafka/LLM needed (agent loop is faked)
```

## Notes / TODO

- **Barrier timeout / unbounded state**: the `GROUP BY claim_id` keeps one state
  entry per claim forever, and a claim where an agent never reports never
  advances. Set a statement state TTL (`SET 'sql.state-ttl' = '4 hours';` before
  the INSERT) so stale claims are GC'd. Workers always emit a result row (even on
  failure), so only a hard crash / lost message stalls a claim.
- **Orchestrator dedupe** is an in-memory `set` of decided claim_ids — decisions
  can be re-emitted after an orchestrator restart. Make `agent.decisions.final`
  the source of truth if that matters.
- **Tools are mocked** (`_FAKE_CLAIMS` / `_FAKE_POLICIES` in `flinkswarm/tools/`).
  Wire them to the real claim/policy store.
- **Go operator** (later): reconcile `SwarmDeployment` → Deployments + KEDA
  ScaledObjects + the Flink statement. `agent-spec.yaml` is already shaped for it.
```
