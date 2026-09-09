-- ==========================================================================
-- FlinkSwarm — Confluent Cloud for Apache Flink
-- ==========================================================================
-- Design: claim_id is the Kafka message key on every topic (raw string). The
-- barrier partitions by that key, so there is no reshuffle, and downstream
-- joins / compaction on claim_id line up for free.
--
-- Order of operations (from scratch):
--   1. Delete the 4 topics + their `-value` (and any `-key`) SR subjects.
--   2. ./scripts/create_topics.sh
--   3. python -m flinkswarm.register_schemas
--        -> registers value schemas for tasks / results / decisions.
--           agent.synthesis.ready is created by Flink below.
--   4. Run the statements in this file:
--        confluent flink shell --compute-pool <lfcp-...>
--        USE CATALOG `<environment>`;
--        USE `<kafka-cluster>`;
--
-- Table names contain dots, so backtick-quote them in SQL.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- 1. Make the inferred key column a STRING (it comes through as VARBINARY
--    because there is no key schema — this is the documented fix).
--    Run DESCRIBE first if ALTER says the table is not found.
-- --------------------------------------------------------------------------
-- DESCRIBE `agent.results.completed`;
ALTER TABLE `agent.results.completed` MODIFY `key` STRING;


-- --------------------------------------------------------------------------
-- 2. Output table. claim_id is the Kafka key (raw string) AND is kept in the
--    value so the message is self-contained.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `agent.synthesis.ready` (
    `key`                STRING,
    `claim_id`           STRING,
    `aggregated_payload` STRING
) DISTRIBUTED BY HASH(`key`) INTO 6 BUCKETS
WITH (
    'changelog.mode' = 'append',
    'key.format'     = 'raw',
    'value.format'   = 'json-registry'
);


-- --------------------------------------------------------------------------
-- 3. Register the PTF. Build + upload the jar first (JDK 21):
--      cd flink
--      export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
--      mvn clean package
--      confluent flink artifact create flinkswarm-barrier \
--          --artifact-file target/flinkswarm-flink-udf.jar --cloud aws --region us-west-2
--    then paste the printed artifact id + version below.
-- --------------------------------------------------------------------------
CREATE FUNCTION AgentBarrierAggregator
    AS 'io.flinkswarm.flink.AgentBarrierAggregator'
    USING JAR 'confluent-artifact://cfa-zw0vo7/ver-719952';


-- --------------------------------------------------------------------------
-- 4. The barrier job (long-running). The number of agents to wait for is
--    EXPECTED_AGENTS in AgentBarrierAggregator.java (currently 2). The PTF
--    output is (claim_id, aggregated_payload); we re-derive the Kafka `key`
--    column from claim_id here.
-- --------------------------------------------------------------------------
INSERT INTO `agent.synthesis.ready`
SELECT `claim_id` AS `key`, `claim_id`, `aggregated_payload`
FROM TABLE(
    AgentBarrierAggregator(
        input => TABLE `agent.results.completed` PARTITION BY `key`,
        uid   => 'flinkswarm-barrier-v1'
    )
);
