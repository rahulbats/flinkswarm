-- ==========================================================================
-- FlinkSwarm — barrier on Confluent Cloud for Apache Flink (pure SQL)
-- ==========================================================================
-- Confluent Cloud Flink does NOT support ProcessTableFunction / user-defined
-- aggregates / UDF state (only stateless ScalarFunction + TableFunction). So
-- the "wait for all N agents per claim" barrier is a plain GROUP BY that emits
-- an UPSERT stream — one row per claim_id, updated as each agent reports. The
-- orchestrator acts once agent_count reaches the worker count in agent-spec.yaml.
-- (The PTF version, for open-source / self-managed Flink, is in
--  flink/ptf-open-source-only/.)
--
-- Order of operations (from scratch):
--   1. Schema Registry enabled on the environment.
--   2. python -m flinkswarm.register_schemas   (tasks / results / decisions)
--   3. Run statement 1 (CREATE TABLE) below, in `confluent flink shell`.
--   4. Register statement 2 (the INSERT) as a PERSISTENT statement:
--          flink/create_barrier.sh
--      Do NOT just run it in the web workspace / flink shell — a session query
--      stops when that session ends. `create_barrier.sh` wraps the exact SQL
--      below in `confluent flink statement create fs-barrier ...`.
--
-- Table names contain dots -> backtick-quote them.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- 1. Output table: upsert stream keyed by claim_id.
--    If an earlier attempt created this table/topic with a different schema:
--      DROP TABLE `agent.synthesis.ready`;
--      -- then delete the topic + its -value subject, or the CREATE will
--      -- fail with "Schema Registry subject ... doesn't match".
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `agent.synthesis.ready` (
    `claim_id`           STRING,
    `agent_count`        BIGINT,
    `aggregated_payload` STRING,
    PRIMARY KEY (`claim_id`) NOT ENFORCED
) DISTRIBUTED BY HASH(`claim_id`) INTO 6 BUCKETS
WITH (
    'changelog.mode' = 'upsert',
    'key.format'     = 'json-registry',
    'value.format'   = 'json-registry'
);


-- --------------------------------------------------------------------------
-- 2. The barrier job (statement name: fs-barrier). Register it with
--    `flink/create_barrier.sh`, which runs exactly the INSERT below inside
--    `confluent flink statement create ... --property sql.state-ttl='4 hours'`.
--    The state-ttl caps the GROUP BY / LISTAGG state so old claims are GC'd
--    (LISTAGG on a retracting input is flagged STATE_INTENSIVE without it).
--
--    Manage it: confluent flink statement {describe,list,delete} fs-barrier
--
-- Inner GROUP BY keeps only the latest result per (claim_id, agent_name), so
-- re-runs / retries of a worker don't inflate the payload. The outer COUNT(*)
-- is the number of distinct agents that have reported; the orchestrator acts
-- when it reaches the worker count in agent-spec.yaml.
-- --------------------------------------------------------------------------
INSERT INTO `agent.synthesis.ready`
SELECT
    `claim_id`,
    COUNT(*)                                                   AS `agent_count`,
    LISTAGG(`agent_name` || ': ' || `latest_result`, ' ||| ')  AS `aggregated_payload`
FROM (
    SELECT `claim_id`, `agent_name`, LAST_VALUE(`result`) AS `latest_result`
    FROM `agent.results.completed`
    GROUP BY `claim_id`, `agent_name`
)
GROUP BY `claim_id`;
