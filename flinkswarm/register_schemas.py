"""Register (or update) the JSON Schemas for every swarm topic in Confluent
Schema Registry, so the topics show up as typed tables in Flink before any
data is produced.

    python -m flinkswarm.register_schemas          # register / update
    python -m flinkswarm.register_schemas --check   # print current, no writes

Auto-registration on first produce also works; this just makes it explicit and
lets you set the tables up before running the swarm.
"""

from __future__ import annotations

import argparse
import sys

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from .config import SchemaRegistrySettings, SwarmSpec
from .events import DecisionFinal, ResultCompleted, TaskDispatched


def _subjects(spec: SwarmSpec) -> dict[str, type]:
    # agent.synthesis.ready is owned by the Flink CREATE TABLE, so its value
    # schema is registered by Flink, not here.
    return {
        f"{spec.topics.tasks}-value": TaskDispatched,
        f"{spec.topics.results}-value": ResultCompleted,
        f"{spec.topics.decisions}-value": DecisionFinal,
    }


def main() -> None:
    parser = argparse.ArgumentParser(prog="flinkswarm.register_schemas")
    parser.add_argument("--check", action="store_true", help="show current versions, do not write")
    args = parser.parse_args()

    sr = SchemaRegistrySettings.from_env()
    if not sr.enabled:
        sys.exit("SCHEMA_REGISTRY_URL is not set")

    client = SchemaRegistryClient(sr.client_config())
    spec = SwarmSpec.load()

    for subject, event_cls in _subjects(spec).items():
        if args.check:
            try:
                v = client.get_latest_version(subject)
                print(f"{subject}: v{v.version} (id {v.schema_id})")
            except Exception as exc:  # noqa: BLE001
                print(f"{subject}: not registered ({type(exc).__name__})")
            continue

        schema_id = client.register_schema(subject, Schema(event_cls.JSON_SCHEMA, schema_type="JSON"))
        print(f"{subject}: registered (id {schema_id})")


if __name__ == "__main__":
    main()
