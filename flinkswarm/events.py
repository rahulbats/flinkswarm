"""Wire events + their JSON Schemas.

Field names are kept in sync with the Flink tables. The schemas here are what
gets registered in Confluent Schema Registry (subject `<topic>-value`), which is
also how the topics show up as typed tables in Confluent Cloud Flink.
"""

from __future__ import annotations

import dataclasses
import json
import time
from dataclasses import dataclass, field
from typing import Any, TypeVar

T = TypeVar("T")

_DRAFT7 = "http://json-schema.org/draft-07/schema#"


@dataclass
class TaskDispatched:
    """Produced by the orchestrator to `agent.tasks.dispatched` (key = claim_id).

    Every worker consumes this topic under its own consumer group and applies
    its own instructions + tools to the same prompt. Flink does not read this
    topic, so `context` is left as an open object.
    """

    claim_id: str
    prompt: str
    context: dict = field(default_factory=dict)
    dispatched_at: float = field(default_factory=time.time)

    JSON_SCHEMA = json.dumps(
        {
            "$schema": _DRAFT7,
            "title": "TaskDispatched",
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "claim_id": {"type": "string"},
                "prompt": {"type": "string"},
                "context": {"type": "object", "additionalProperties": True},
                "dispatched_at": {"type": "number"},
            },
            "required": ["claim_id", "prompt"],
        }
    )


@dataclass
class ResultCompleted:
    """Produced by each worker to `agent.results.completed` (key = claim_id).

    Read by the Flink barrier, so the schema is closed for clean column
    inference: claim_id, agent_name, status, result, error.
    """

    claim_id: str
    agent_name: str
    status: str  # SUCCESS | FAILURE
    result: str
    error: str | None = None

    JSON_SCHEMA = json.dumps(
        {
            "$schema": _DRAFT7,
            "title": "ResultCompleted",
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "claim_id": {"type": "string"},
                "agent_name": {"type": "string"},
                "status": {"type": "string"},
                "result": {"type": "string"},
                "error": {"type": ["string", "null"]},
            },
            "required": ["claim_id", "agent_name", "status", "result"],
        }
    )


@dataclass
class SynthesisReady:
    """Emitted by the Flink PTF barrier (`AgentBarrier`) to `agent.synthesis.ready`
    as an append stream — exactly one row per claim once all agents report.

    The value schema for this topic is owned by the Flink `CREATE TABLE`, so this
    schema is only used for permissive read-side validation. `aggregated_payload`
    is a JSON string: {"claim_id":..., "partial":bool, "results":{agent: text}}.

    `claim_id` is not in the value (Flink puts it in the Kafka key via
    DISTRIBUTED BY) — the orchestrator fills it in from the payload JSON.
    """

    aggregated_payload: str
    claim_id: str = ""

    JSON_SCHEMA = json.dumps(
        {
            "$schema": _DRAFT7,
            "title": "SynthesisReady",
            "type": "object",
            "properties": {
                "claim_id": {"type": "string"},
                "aggregated_payload": {"type": "string"},
            },
        }
    )


@dataclass
class DecisionFinal:
    """Produced by the orchestrator to `agent.decisions.final` (key = claim_id)."""

    claim_id: str
    decision: str
    rationale: str
    tool_calls: list[str] = field(default_factory=list)
    decided_at: float = field(default_factory=time.time)

    JSON_SCHEMA = json.dumps(
        {
            "$schema": _DRAFT7,
            "title": "DecisionFinal",
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "claim_id": {"type": "string"},
                "decision": {"type": "string"},
                "rationale": {"type": "string"},
                "tool_calls": {"type": "array", "items": {"type": "string"}},
                "decided_at": {"type": "number"},
            },
            "required": ["claim_id", "decision", "rationale"],
        }
    )


def to_dict(event: Any) -> dict:
    return dataclasses.asdict(event)


def from_dict(cls: type[T], data: dict) -> T:
    known = {f.name for f in dataclasses.fields(cls)}  # type: ignore[arg-type]
    return cls(**{k: v for k, v in data.items() if k in known})


# Back-compat helpers (plain JSON, no Schema Registry) — used by tests and by
# the fallback path in kafka.py when SCHEMA_REGISTRY_URL is unset.
def dumps(event: Any) -> bytes:
    return json.dumps(to_dict(event)).encode("utf-8")


def loads(cls: type[T], raw: bytes | str) -> T:
    data = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    return from_dict(cls, data)
