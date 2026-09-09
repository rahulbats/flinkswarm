"""Kafka plumbing: producer/consumer factories + JSON value (de)serialization.

Values go through Confluent Schema Registry (JSON Schema) when
SCHEMA_REGISTRY_URL is set — which is also what makes the topics appear as
typed tables in Confluent Cloud Flink. Without it, values fall back to plain
JSON so local runs still work.

Keys are always a raw UTF-8 string (the claim_id) — `key.format = 'raw'` in
Flink.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .config import KafkaSettings, SchemaRegistrySettings
from .events import from_dict, to_dict

logger = logging.getLogger("flinkswarm.kafka")


def build_producer(kafka: KafkaSettings, **overrides) -> Producer:
    cfg = kafka.base_config()
    cfg.update(
        {
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
            "compression.type": "lz4",
        }
    )
    cfg.update(overrides)
    return Producer(cfg)


def build_consumer(kafka: KafkaSettings, group_id: str, **overrides) -> Consumer:
    cfg = kafka.base_config()
    cfg.update(
        {
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # commit explicitly after processing
            "partition.assignment.strategy": "cooperative-sticky",
        }
    )
    cfg.update(overrides)
    return Consumer(cfg)


def delivery_report(err, msg) -> None:
    if err is not None:
        logger.error("delivery failed for %s: %s", msg.topic() if msg else "?", err)


def schema_registry_client(sr: SchemaRegistrySettings) -> SchemaRegistryClient | None:
    if not sr.enabled:
        return None
    return SchemaRegistryClient(sr.client_config())


# key encoding ------------------------------------------------------------- #
def encode_key(claim_id: str) -> bytes:
    return claim_id.encode("utf-8")


def decode_key(raw: bytes | None) -> str:
    """Raw-string key (what the workers/orchestrator produce)."""
    return raw.decode("utf-8") if raw else "UNKNOWN"


class KeyCodec:
    """Read the claim_id from a Kafka key that may be a raw string OR a
    Schema-Registry JSON object (Flink writes the PRIMARY KEY column as a
    json-registry key, e.g. {"claim_id": "CLM-1001"}).
    """

    def __init__(self, sr: SchemaRegistrySettings, client: SchemaRegistryClient | None = None):
        self._deser: JSONDeserializer | None = None
        if sr.enabled:
            client = client or schema_registry_client(sr)
            self._deser = JSONDeserializer(None, from_dict=lambda d, ctx: d, schema_registry_client=client)

    def claim_id(self, raw: bytes | None, topic: str) -> str:
        if not raw:
            return "UNKNOWN"
        if self._deser is not None and raw[:1] == b"\x00":  # SR wire-format magic byte
            try:
                obj = self._deser(raw, SerializationContext(topic, MessageField.KEY))
                if isinstance(obj, dict):
                    return str(obj.get("claim_id") or next(iter(obj.values()), "UNKNOWN"))
                return str(obj)
            except Exception:
                logger.exception("failed to decode SR key on %s", topic)
        return raw.decode("utf-8", errors="replace")


# value codec ------------------------------------------------------------- #
class ValueCodec:
    """JSON value serde for one event type.

    Uses Schema Registry when `sr` is enabled (auto-registers `<topic>-value`
    on first produce); otherwise plain JSON.
    """

    def __init__(self, event_cls: type, sr: SchemaRegistrySettings, client: SchemaRegistryClient | None = None):
        self._cls = event_cls
        self._ser: JSONSerializer | None = None
        self._deser: JSONDeserializer | None = None

        if sr.enabled:
            client = client or schema_registry_client(sr)
            self._ser = JSONSerializer(
                event_cls.JSON_SCHEMA,
                client,
                to_dict=lambda obj, ctx: to_dict(obj),
            )
            self._deser = JSONDeserializer(
                event_cls.JSON_SCHEMA,
                from_dict=lambda d, ctx: from_dict(event_cls, d),
            )

    def encode(self, event: Any, topic: str) -> bytes:
        if self._ser is not None:
            return self._ser(event, SerializationContext(topic, MessageField.VALUE))
        return json.dumps(to_dict(event)).encode("utf-8")

    def decode(self, raw: bytes, topic: str) -> Any:
        if self._deser is not None:
            return self._deser(raw, SerializationContext(topic, MessageField.VALUE))
        return from_dict(self._cls, json.loads(raw))
