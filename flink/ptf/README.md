# AgentBarrier — the Flink PTF barrier

`AgentBarrier` is the scatter/gather barrier as a **Flink 2.1 ProcessTableFunction**,
which **is supported on Confluent Cloud for Apache Flink**
(<https://docs.confluent.io/cloud/current/flink/how-to-guides/create-ptf.html>).

Partitioned by `claim_id`, it buffers one result per sub-agent in keyed state and
`collect()`s **exactly one** aggregated row once `expectedAgents` distinct agents
have reported. Because it emits once on an append stream, the orchestrator just
consumes and synthesizes — no count gate, no upsert changelog to de-duplicate
(contrast the pure-SQL `GROUP BY` barrier in `../setup_queries.sql`).

On emit it calls `ctx.clearAllState()`, so the claim is **forgotten** — you can
re-dispatch the same `claim_id` and it's adjudicated fresh, no pipeline reset
needed. (A stray late result for an already-decided claim just starts a new,
harmless one-entry accumulation that the statement's `sql.state-ttl` sweeps up.)

Output value is a JSON string:
```json
{"claim_id":"CLM-1001","partial":false,"results":{"ClaimDataAgent":"…","PolicyDocAgent":"…"}}
```

## Build (JDK 21)

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
mvn clean package        # -> target/flinkswarm-barrier-ptf.jar
```

Two things Confluent Cloud requires and that bit us the first time:
1. **Build against Flink 2.1** (`flink.version` in pom.xml). Confluent Cloud runs
   2.1; a jar built against 2.0 fails to register with *"Unable to extract a type
   inference from method"*.
2. **`-parameters` compiler flag** (set in pom.xml). Without it, `@StateHint`
   objects and named arguments can't be resolved.

## Deploy

`../create_barrier.sh` does the whole thing (build, `confluent flink artifact
create`, `CREATE FUNCTION`, and the persistent barrier `INSERT`). By hand:

```sql
CREATE FUNCTION AgentBarrier
  AS 'io.flinkswarm.flink.AgentBarrier'
  USING JAR 'confluent-artifact://<cfa-id>/<ver-id>';

INSERT INTO `agent.synthesis.ready`
SELECT `claim_id`, `aggregated_payload`
FROM AgentBarrier(
  input          => TABLE `agent.results.completed` PARTITION BY `key`,
  expectedAgents => 2,
  uid            => 'flinkswarm-barrier-v4');
```

Named args must match the Java parameter names exactly (`expectedAgents`, not
`expected_agents`).

## Timeout (not yet wired)

Flink 2.1 PTFs support timers (`Context` + `ctx.timeContext(Instant.class)` +
`registerOnTime` + an `onTimer(BarrierState)` callback), with the
`REQUIRE_ON_TIME` argument trait on the input table. That would let the barrier
emit a `"partial": true` payload for a claim where an agent never reports. The
orchestrator already handles a `partial` payload; the PTF just doesn't set it
yet.
