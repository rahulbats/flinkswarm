import asyncio
import json

from flinkswarm.events import (
    DecisionFinal,
    ResultCompleted,
    SynthesisReady,
    TaskDispatched,
    dumps,
    loads,
)
from flinkswarm.llm import Agent, Tool
from flinkswarm.orchestrator import _split_decision
from flinkswarm.tools import TOOL_REGISTRY


def test_event_roundtrip_ignores_unknown_fields():
    raw = dumps(ResultCompleted("CLM-1", "ClaimDataAgent", "SUCCESS", "ok"))
    payload = json.loads(raw)
    payload["extra_field_from_flink"] = 1
    back = loads(ResultCompleted, json.dumps(payload).encode())
    assert back.claim_id == "CLM-1"
    assert back.agent_name == "ClaimDataAgent"


def test_task_dispatched_defaults():
    t = loads(TaskDispatched, dumps(TaskDispatched("CLM-9", "do it")))
    assert t.context == {}
    assert t.dispatched_at > 0


def test_registry_has_starter_tools():
    assert "get_claim_from_cosmos" in TOOL_REGISTRY
    assert "get_policy_from_blob" in TOOL_REGISTRY


def test_split_decision_parses_json_blob():
    d, r = _split_decision('Sure! {"decision": "COVERED", "rationale": "clause SEC-II-A"} done')
    assert d == "COVERED"
    assert "SEC-II-A" in r


def test_split_decision_falls_back_to_first_line():
    d, r = _split_decision("NOT COVERED\nflood is excluded")
    assert d == "NOT COVERED"
    assert "flood" in r


def test_value_codec_plain_json_roundtrip():
    from flinkswarm.config import SchemaRegistrySettings
    from flinkswarm.kafka import ValueCodec

    codec = ValueCodec(ResultCompleted, SchemaRegistrySettings())  # SR disabled
    evt = ResultCompleted("CLM-1", "ClaimDataAgent", "SUCCESS", "ok")
    raw = codec.encode(evt, "agent.results.completed")
    back = codec.decode(raw, "agent.results.completed")
    assert back == evt


def test_event_schemas_are_valid_json():
    for cls in (TaskDispatched, ResultCompleted, SynthesisReady, DecisionFinal):
        schema = json.loads(cls.JSON_SCHEMA)
        assert schema["type"] == "object"
        assert "claim_id" in schema["properties"]


def test_synthesis_and_decision_events():
    s = loads(SynthesisReady, dumps(SynthesisReady(agent_count=2, aggregated_payload="ClaimDataAgent: ok ||| PolicyDocAgent: ok")))
    assert s.agent_count == 2
    assert "PolicyDocAgent" in s.aggregated_payload
    assert s.claim_id == ""  # filled from the Kafka key by the orchestrator
    dec = loads(DecisionFinal, dumps(DecisionFinal("CLM-1", "COVERED", "because")))
    assert dec.tool_calls == []


class _FakeCompletions:
    def __init__(self, script):
        self._script = list(script)

    async def create(self, **kwargs):
        return self._script.pop(0)


def _msg(content=None, tool_calls=None):
    class M:
        def __init__(self):
            self.content = content
            self.tool_calls = tool_calls or []

        def model_dump(self, exclude_none=False):
            return {"role": "assistant", "content": self.content}

    class Choice:
        def __init__(self):
            self.message = M()

    class Resp:
        def __init__(self):
            self.choices = [Choice()]

    return Resp()


def _tool_call(cid, name, args):
    class F:
        def __init__(self):
            self.name = name
            self.arguments = json.dumps(args)

    class TC:
        def __init__(self):
            self.id = cid
            self.function = F()

    return TC()


def test_agent_runs_tool_loop(monkeypatch):
    from flinkswarm.config import LLMSettings

    agent = Agent(
        name="T",
        instructions="test",
        tools=[
            Tool(
                name="echo",
                description="echo",
                parameters={"type": "object", "properties": {"x": {"type": "string"}}},
                fn=lambda x: {"echoed": x},
            )
        ],
        llm=LLMSettings(),
    )
    agent._client.chat.completions = _FakeCompletions(
        [
            _msg(tool_calls=[_tool_call("c1", "echo", {"x": "hi"})]),
            _msg(content="final answer"),
        ]
    )
    result = asyncio.run(agent.run("go"))
    assert result.text == "final answer"
    assert result.tool_calls == ["echo"]
