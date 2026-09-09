-- ==========================================================================
-- FlinkSwarm — Confluent Cloud for Apache Flink
-- ==========================================================================
-- Order of operations:
--   1. Enable Schema Registry (Stream Governance) on the environment.
--   2. `python -m flinkswarm.register_schemas` — registers <topic>-value for
--      all four topics. The topics then appear as tables in the Flink catalog.
--   3. Run the statements below. Easiest via:
--        confluent flink shell --compute-pool <lfcp-...>
--      then USE CATALOG / USE to select your environment + kafka cluster.
--
-- Table names contain dots (agent.results.completed), so they must be
-- backtick-quoted in SQL.
--
-- Do NOT declare 'key.format' = 'raw' here: Confluent Cloud Flink then requires
-- the key column to be named `key`, which fights the 2-column PTF output. Let
-- Flink derive the key from DISTRIBUTED BY. The workers write a raw-string key
-- on the input topics; the orchestrator reads claim_id from the value, not the
-- key, so the key encoding on agent.synthesis.ready does not matter.
-- ==========================================================================

-- Confirm the input table showed up from the registered schema:
--   DESCRIBE `agent.results.completed`;
-- Expect columns: claim_id, agent_name, status, result, error.


-- 1. Output table (also creates the backing topic + its value schema).
CREATE TABLE IF NOT EXISTS `agent.synthesis.ready` (
    claim_id           STRING,
    aggregated_payload STRING
) DISTRIBUTED BY HASH(claim_id) INTO 6 BUCKETS
WITH (
    'changelog.mode' = 'append',
    'value.format'   = 'json-registry'
);


-- 2. Register the PTF. Build + upload the jar first (JDK 21):
--      cd flink
--      export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
--      mvn clean package
--      confluent flink artifact create flinkswarm-barrier \
--          --artifact-file target/flinkswarm-flink-udf.jar --cloud aws --region us-west-2
--    then paste the printed artifact id + version below.
CREATE FUNCTION AgentBarrierAggregator
    AS 'io.flinkswarm.flink.AgentBarrierAggregator'
    USING JAR 'confluent-artifact://cfa-369qj0/ver-3kgg2m';


-- 3. The barrier job (long-running). `expected_agents` = number of
--    spec.workers in agent-spec.yaml. One output row per claim_id once all
--    agents report — the "workers done" signal for the orchestrator.
--
--    Flink 2.0 PTF has no timer API, so there is no timeout: a claim where an
--    agent never reports stays buffered. Workers always emit a result row
--    (even on failure), so only a hard crash / lost message stalls a claim.
INSERT INTO `agent.synthesis.ready`
SELECT claim_id, aggregated_payload
FROM TABLE(
    AgentBarrierAggregator(
        input           => TABLE `agent.results.completed` PARTITION BY claim_id,
        expected_agents => 2,
        uid             => 'flinkswarm-barrier-v1'
    )
);
