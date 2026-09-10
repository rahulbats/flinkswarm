-- ==========================================================================
-- FlinkSwarm — the barrier on Confluent Cloud for Apache Flink
-- ==========================================================================
-- Confluent Cloud runs Flink 2.1, which supports ProcessTableFunction (PTF).
-- The barrier is `AgentBarrier` (flink/ptf/): partitioned by claim_id, it keeps
-- one result per agent in keyed state and emits EXACTLY ONE row per claim once
-- all agents report — a clean append stream, no downstream gating.
--
--   flink/create_barrier.sh          builds the jar, uploads it, and creates
--                                    every statement below as a persistent
--                                    `confluent flink statement`.
--
-- The SQL is reproduced here for reference. Run it via create_barrier.sh, not
-- the web workspace (a workspace query stops when you close the tab).
--
-- Order of operations from scratch:
--   1. Schema Registry enabled on the environment.
--   2. python -m flinkswarm.register_schemas   (tasks / results / decisions)
--   3. flink/create_barrier.sh
--
-- Table names contain dots -> backtick-quote them.
-- ==========================================================================

-- 1. The Kafka message key on agent.results.completed is a raw utf-8 string
--    (claim_id). Flink infers it as VARBINARY; make it STRING so the PTF can
--    read it with input.getFieldAs("key").
ALTER TABLE `agent.results.completed` MODIFY `key` STRING;

-- 2. Output table — an APPEND stream, one row per claim. claim_id is carried in
--    the value (the PTF puts it there), so no PRIMARY KEY / key.format needed.
CREATE TABLE IF NOT EXISTS `agent.synthesis.ready` (
    `claim_id`           STRING,
    `aggregated_payload` STRING
) DISTRIBUTED BY HASH(`claim_id`) INTO 6 BUCKETS
WITH (
    'changelog.mode' = 'append',
    'value.format'   = 'json-registry'
);

-- 3. Register the PTF. flink/create_barrier.sh builds + uploads the jar and
--    substitutes the artifact URI.
CREATE FUNCTION AgentBarrier
    AS 'io.flinkswarm.flink.AgentBarrier'
    USING JAR 'confluent-artifact://<cfa-id>/<ver-id>';

-- 4. The barrier job (persistent statement `fs-barrier-ptf`).
--    expectedAgents = number of spec.workers in agent-spec.yaml.
--    Created with:  --property sql.tables.scan.startup.mode=earliest-offset
--                   --property sql.state-ttl='4 hours'
INSERT INTO `agent.synthesis.ready`
SELECT `claim_id`, `aggregated_payload`
FROM AgentBarrier(
    input          => TABLE `agent.results.completed` PARTITION BY `key`,
    expectedAgents => 2,
    uid            => 'flinkswarm-barrier-v4'
);

-- ==========================================================================
-- Pure-SQL fallback (no jar) — a GROUP BY barrier. Emits an UPSERT row per
-- claim on every agent, so the orchestrator must gate on a count and dedupe.
-- Kept for reference; the PTF above is the real design.
-- ==========================================================================
-- CREATE TABLE `agent.synthesis.ready` (
--   `claim_id` STRING, `agent_count` BIGINT, `aggregated_payload` STRING,
--   PRIMARY KEY (`claim_id`) NOT ENFORCED
-- ) DISTRIBUTED BY HASH(`claim_id`) INTO 6 BUCKETS
-- WITH ('changelog.mode'='upsert','key.format'='json-registry','value.format'='json-registry');
--
-- INSERT INTO `agent.synthesis.ready`
-- SELECT `claim_id`, COUNT(*) AS `agent_count`,
--        LISTAGG(`agent_name` || ': ' || `latest_result`, ' ||| ') AS `aggregated_payload`
-- FROM (
--   SELECT `claim_id`, `agent_name`, LAST_VALUE(`result`) AS `latest_result`
--   FROM `agent.results.completed` GROUP BY `claim_id`, `agent_name`
-- ) GROUP BY `claim_id`;
