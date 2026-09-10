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
                                        │  Flink PTF: AgentBarrier      │
                                        │  PARTITION BY claim_id         │
                                        │  keyed state, wait for all N   │
                                        │  emit ONE row per claim        │
                                        └───────────────┬───────────────┘
                                                        │ agent.synthesis.ready (append)
                                                        ▼
                                            orchestrator (serve)
                                          synthesize LLM verdict, once/claim
                                                        │ agent.decisions.final
                                                        ▼
```

`claim_id` is the Kafka message key on every topic.

Each worker consumes the **same** task topic under its own consumer group and
applies its own instructions + tools (from `agent-spec.yaml`). The Flink
`ProcessTableFunction` [`AgentBarrier`](flink/ptf/) buffers one result per agent
in keyed state and emits **exactly one** aggregated row per claim once every
agent has reported — a clean append stream. The orchestrator consumes that and
runs the final LLM adjudication.

> Confluent Cloud for Apache Flink runs Flink 2.1 and **does** support PTFs.
> A pure-SQL `GROUP BY` barrier (emits an upsert row per agent; orchestrator
> gates on a count) is kept as a documented fallback in `flink/setup_queries.sql`.

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
| `flink/ptf/` | `AgentBarrier` — the Flink 2.1 PTF barrier (Java) |
| `flink/create_barrier.sh` | build + upload the PTF, register it, run the barrier statement |
| `flink/setup_queries.sql` | the same SQL, annotated, + the pure-SQL fallback |
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

One script builds the PTF jar, uploads it, registers the function, and creates
the persistent barrier statement:

```bash
COMPUTE_POOL=lfcp-… DATABASE=lkc-… ENVIRONMENT=env-… flink/create_barrier.sh
```

It creates:
- `CREATE TABLE agent.synthesis.ready` — append, `(claim_id, aggregated_payload)`
- `CREATE FUNCTION AgentBarrier … USING JAR 'confluent-artifact://…'`
- `fs-barrier-ptf` — the long-running `INSERT … FROM AgentBarrier(input => TABLE agent.results.completed PARTITION BY key, expectedAgents => 2, …)`

`agent.results.completed` is inferred from the schema you registered in step 1
(the script's `ALTER TABLE … MODIFY key STRING` makes its raw key column usable).
Check it: `confluent flink statement describe fs-barrier-ptf`. Details and the
pure-SQL fallback are in [flink/setup_queries.sql](flink/setup_queries.sql).

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

- **Barrier timeout**: `AgentBarrier` clears its state when it emits, so state =
  only in-flight claims (the statement also carries `sql.state-ttl='4 hours'`).
  A claim where an agent never reports still stalls, though — the PTF should
  register a Flink 2.1 timer and emit a `"partial": true` payload on timeout
  (the orchestrator already handles that field). See `flink/ptf/README.md`.
- **Orchestrator dedupe** is an in-memory `set` of decided claim_ids — decisions
  can be re-emitted after an orchestrator restart. Make `agent.decisions.final`
  the source of truth if that matters.
- **Tools are mocked** (`_FAKE_CLAIMS` / `_FAKE_POLICIES` in `flinkswarm/tools/`).
  Wire them to the real claim/policy store.
- **Go operator** (later): reconcile `SwarmDeployment` → Deployments + KEDA
  ScaledObjects + the Flink statement. `agent-spec.yaml` is already shaped for it.
```
