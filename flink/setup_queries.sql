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
--   3. Run the statements below:
--        confluent flink shell --compute-pool <lfcp-...> \
--            --environment <env-...> --database <lkc-...>
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
-- 2. The barrier job (long-running).
--    Optional: cap unbounded GROUP BY state so old claims are GC'd.
--      SET 'sql.state-ttl' = '4 hours';
-- --------------------------------------------------------------------------
-- Inner GROUP BY keeps only the latest result per (claim_id, agent_name), so
-- re-runs / retries of a worker don't inflate the payload.
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
