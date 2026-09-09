package io.flinkswarm.flink;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * Barrier / scatter-gather aggregator for FlinkSwarm (Flink 2.0 PTF).
 *
 * <p>Partitioned by the Kafka message key (the {@code key} column == claim_id).
 * Buffers one result row per sub-agent in keyed PTF state and emits a single
 * aggregated JSON payload once {@link #EXPECTED_AGENTS} distinct agents have
 * reported. That output row is the "all workers done for this claim" signal the
 * orchestrator consumes.
 *
 * <p>Output is {@code (claim_id, aggregated_payload)}; the INSERT re-derives the
 * Kafka {@code key} column from {@code claim_id}.
 *
 * <pre>
 * INSERT INTO `agent.synthesis.ready`
 * SELECT claim_id AS `key`, claim_id, aggregated_payload
 * FROM TABLE(AgentBarrierAggregator(
 *   input => TABLE `agent.results.completed` PARTITION BY `key`,
 *   uid   => 'flinkswarm-barrier-v1'));
 * </pre>
 *
 * <p>Flink 2.0 PTF has no timer API, so there is no timeout: a claim where an
 * agent never reports stays buffered. Workers always emit a result row (even on
 * failure), so only a hard crash / lost message stalls a claim.
 */
@FunctionHint(output = @DataTypeHint("ROW<claim_id STRING, aggregated_payload STRING>"))
public class AgentBarrierAggregator extends ProcessTableFunction<Row> {

    /** Number of sub-agents to wait for. Matches spec.workers in agent-spec.yaml. */
    public static final int EXPECTED_AGENTS = 2;

    public static class BarrierState {
        public Map<String, String> responses = new HashMap<>();
    }

    public void eval(
            @StateHint BarrierState state,
            @ArgumentHint(ArgumentTrait.TABLE_AS_SET) Row input) {

        String claimId = input.getFieldAs("key"); // partition key == claim_id
        String agentName = input.getFieldAs("agent_name");
        String status = input.getFieldAs("status");
        String result = input.getFieldAs("result");

        state.responses.put(
                agentName,
                "FAILURE".equalsIgnoreCase(status) ? "[agent reported FAILURE]" : result);

        if (state.responses.size() >= EXPECTED_AGENTS) {
            collect(Row.of(claimId, buildPayload(claimId, state.responses)));
            state.responses = new HashMap<>();
        }
    }

    private static String buildPayload(String claimId, Map<String, String> responses) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"claim_id\":\"").append(jsonEscape(claimId)).append("\",\"results\":{");
        boolean first = true;
        for (Map.Entry<String, String> e : responses.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            sb.append('"').append(jsonEscape(e.getKey())).append("\":\"")
              .append(jsonEscape(e.getValue())).append('"');
            first = false;
        }
        return sb.append("}}").toString();
    }

    private static String jsonEscape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "");
    }
}
