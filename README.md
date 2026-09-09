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
| `scripts/` | `create_topics.sh`, `run_swarm.sh` |

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

Run the two statements in `flink/setup_queries.sql`:

1. `CREATE TABLE \`agent.synthesis.ready\` (…) WITH ('changelog.mode' = 'upsert', …)`
2. the long-running `INSERT … SELECT claim_id, COUNT(DISTINCT agent_name), LISTAGG(…) … GROUP BY claim_id`

`agent.results.completed` is inferred as a table from the schema registered in
step 1 — nothing to create. The `INSERT` stays `RUNNING`; confirm with
`confluent flink statement list`.

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
