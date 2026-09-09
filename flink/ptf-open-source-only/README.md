# AgentBarrierAggregator (PTF) — open-source / self-managed Flink only

This is the barrier as a Flink 2.0 **ProcessTableFunction**: partitioned by
`claim_id`, keeps one buffered result per sub-agent in managed state, emits a
single aggregated row once every expected agent has reported.

**It does not run on Confluent Cloud for Apache Flink.** Confluent Cloud's UDF
support is limited to stateless `ScalarFunction` and `TableFunction` — no PTF, no
user-defined aggregates, no UDF state
(<https://docs.confluent.io/cloud/current/flink/concepts/user-defined-functions.html>).
Loading it there fails with `Unable to extract a type inference from method`.

For Confluent Cloud, the barrier is the pure-SQL `GROUP BY` in
`../setup_queries.sql`. Use this PTF only against an open-source Flink 2.0+
cluster or another distribution that supports PTFs.

## Build (JDK 21)

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
mvn clean package        # -> target/flinkswarm-flink-udf.jar
```

## Register + run (open-source Flink SQL)

```sql
CREATE FUNCTION AgentBarrierAggregator
  AS 'io.flinkswarm.flink.AgentBarrierAggregator'
  USING JAR '/path/to/flinkswarm-flink-udf.jar';

INSERT INTO agent_synthesis_ready
SELECT claim_id AS `key`, claim_id, aggregated_payload
FROM TABLE(AgentBarrierAggregator(
  input => TABLE agent_results_completed PARTITION BY `key`,
  uid   => 'flinkswarm-barrier-v1'));
```

`EXPECTED_AGENTS` is a constant in the Java (currently 2). The output value
schema is `(claim_id, aggregated_payload)` where `aggregated_payload` is
`{"claim_id":...,"results":{"<agent>":"<text>", ...}}` — differs from the
pure-SQL barrier, which emits `agent_count` + a LISTAGG string. Reconcile
`flinkswarm/events.py::SynthesisReady` and the orchestrator if you switch.
