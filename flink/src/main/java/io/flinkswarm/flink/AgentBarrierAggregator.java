package io.flinkswarm.flink;

import java.util.Map;
import java.util.TreeMap;

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
 * <p>Partitioned by the Kafka message key (`key` column = claim_id). Buffers one
 * result row per sub-agent in keyed PTF state and emits a single aggregated JSON
 * payload once {@code expected_agents} distinct agents have reported. That output
 * row is the "all workers done for this claim" signal the orchestrator consumes.
 *
 * <p>Output: `key` (the claim_id, becomes the Kafka key again) + `claim_id`
 * (same value, kept in the message value so it is self-contained) +
 * `aggregated_payload` (a JSON string).
 *
 * <p>SQL:
 * <pre>
 * INSERT INTO `agent.synthesis.ready`
 * SELECT `key`, claim_id, aggregated_payload
 * FROM TABLE(AgentBarrierAggregator(
 *   input           => TABLE `agent.results.completed` PARTITION BY `key`,
 *   expected_agents => 2,
 *   uid             => 'flinkswarm-barrier-v1'));
 * </pre>
 *
 * <p>Flink 2.0 PTF has no timer API, so there is no timeout: a claim where an
 * agent never reports stays buffered. Workers always emit a result row (even on
 * failure), so only a hard crash / lost message stalls a claim.
 */
@FunctionHint(
    output = @DataTypeHint("ROW<`key` STRING, `claim_id` STRING, `aggregated_payload` STRING>"))
public class AgentBarrierAggregator extends ProcessTableFunction<Row> {

    public static class BarrierState {
        public String claimId;
        /** agent_name -> result text, ordered for deterministic output. */
        public Map<String, String> responses = new TreeMap<>();
        public boolean emitted = false;
    }

    public void eval(
            @StateHint BarrierState state,
            @ArgumentHint(value = ArgumentTrait.TABLE_AS_SET, name = "input") Row input,
            @ArgumentHint(name = "expected_agents") Integer expectedAgents) {

        if (state.emitted) {
            return; // straggler after the barrier already fired for this key
        }

        String claimId = input.getFieldAs("key"); // partition key == claim_id
        String agentName = input.getFieldAs("agent_name");
        String status = input.getFieldAs("status");
        String result = input.getFieldAs("result");

        state.claimId = claimId;
        state.responses.put(
                agentName,
                "FAILURE".equalsIgnoreCase(status) ? "[agent reported FAILURE]" : result);

        int expected = (expectedAgents == null || expectedAgents <= 0) ? 1 : expectedAgents;
        if (state.responses.size() >= expected) {
            String payload = buildPayload(state);
            collect(Row.of(state.claimId, state.claimId, payload));
            state.emitted = true;
            state.responses = new TreeMap<>();
        }
    }

    private static String buildPayload(BarrierState state) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"claim_id\":\"").append(jsonEscape(state.claimId)).append("\",\"results\":{");
        boolean first = true;
        for (Map.Entry<String, String> e : state.responses.entrySet()) {
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
