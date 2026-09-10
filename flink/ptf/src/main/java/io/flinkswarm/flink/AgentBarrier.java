package io.flinkswarm.flink;

import static org.apache.flink.table.annotation.ArgumentTrait.SET_SEMANTIC_TABLE;

import java.util.Map;
import java.util.TreeMap;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * Scatter/gather barrier for FlinkSwarm, as a Flink 2.1 ProcessTableFunction.
 *
 * <p>Partitioned by the Kafka key ({@code key} column == claim_id). Buffers one
 * result per sub-agent in keyed PTF state and emits exactly one aggregated row
 * once {@code expectedAgents} distinct agents have reported. On emit it calls
 * {@code ctx.clearAllState()} — the claim is forgotten, so re-dispatching the
 * same claim_id is adjudicated fresh (a duplicate/late result for a decided
 * claim just starts a new, harmless single-entry accumulation that the
 * statement's state TTL sweeps up).
 *
 * <p>Emitting once, on an append stream, is why this is cleaner than the
 * pure-SQL {@code GROUP BY} barrier (which emits an upsert row on every agent
 * and pushes the "all in?" gate to the orchestrator).
 *
 * <p>Output value is a JSON string:
 * <pre>{"claim_id":"CLM-1001","partial":false,"results":{"ClaimDataAgent":"...","PolicyDocAgent":"..."}}</pre>
 *
 * <p>SQL:
 * <pre>
 * INSERT INTO `agent.synthesis.ready`
 * SELECT claim_id, aggregated_payload
 * FROM AgentBarrier(
 *   input          => TABLE `agent.results.completed` PARTITION BY `key`,
 *   expectedAgents => 2,
 *   uid            => 'flinkswarm-barrier-v4');
 * </pre>
 *
 * <p>Build with JDK 21 and the {@code -parameters} compiler flag (see pom.xml).
 */
@DataTypeHint("ROW<claim_id STRING, aggregated_payload STRING>")
public class AgentBarrier extends ProcessTableFunction<Row> {

    public static class BarrierState {
        /** agent_name -> result text, ordered for a deterministic payload. */
        public Map<String, String> responses = new TreeMap<>();
    }

    public void eval(
            Context ctx,
            @StateHint BarrierState state,
            @ArgumentHint(SET_SEMANTIC_TABLE) Row input,
            Integer expectedAgents) {

        String claimId = input.getFieldAs("key"); // partition key == claim_id
        String agentName = input.getFieldAs("agent_name");
        String status = input.getFieldAs("status");
        String result = input.getFieldAs("result");

        state.responses.put(
                agentName,
                "FAILURE".equalsIgnoreCase(status) ? "[agent reported FAILURE]" : result);

        int expected = (expectedAgents == null || expectedAgents <= 0) ? 1 : expectedAgents;
        if (state.responses.size() >= expected) {
            collect(Row.of(claimId, payload(claimId, state.responses, false)));
            state.responses.clear();
            ctx.clearAllState(); // forget the claim — a re-dispatch decides it anew
        }
    }

    static String payload(String claimId, Map<String, String> responses, boolean partial) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"claim_id\":\"").append(esc(claimId)).append("\"")
          .append(",\"partial\":").append(partial)
          .append(",\"results\":{");
        boolean first = true;
        for (Map.Entry<String, String> e : responses.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            sb.append('"').append(esc(e.getKey())).append("\":\"").append(esc(e.getValue())).append('"');
            first = false;
        }
        return sb.append("}}").toString();
    }

    private static String esc(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "").replace("\t", " ");
    }
}
