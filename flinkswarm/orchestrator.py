"""Coverage orchestrator.

Two subcommands:

    # fan a task out to the swarm
    python -m flinkswarm.orchestrator dispatch --claim CLM-1001 \
        --prompt "Adjudicate coverage for claim CLM-1001."

    # long-running: consume the Flink barrier output and synthesize a decision
    python -m flinkswarm.orchestrator serve
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal

from confluent_kafka import KafkaError

from .config import KafkaSettings, LLMSettings, SchemaRegistrySettings, SwarmSpec
from .events import DecisionFinal, SynthesisReady, TaskDispatched
from .kafka import (
    ValueCodec,
    build_consumer,
    build_producer,
    delivery_report,
    encode_key,
    schema_registry_client,
)
from .llm import Agent

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("flinkswarm.orchestrator")


def _kafka(spec: SwarmSpec) -> KafkaSettings:
    return KafkaSettings.from_env(spec.fallback_bootstrap)


def dispatch(claim_id: str, prompt: str, context: dict | None = None) -> None:
    spec = SwarmSpec.load()
    producer = build_producer(_kafka(spec))
    codec = ValueCodec(TaskDispatched, SchemaRegistrySettings.from_env())
    event = TaskDispatched(claim_id=claim_id, prompt=prompt, context=context or {})
    producer.produce(
        spec.topics.tasks,
        key=encode_key(claim_id),
        value=codec.encode(event, spec.topics.tasks),
        on_delivery=delivery_report,
    )
    producer.flush(10)
    logger.info("dispatched task for %s to %s", claim_id, spec.topics.tasks)


class OrchestratorService:
    def __init__(self) -> None:
        self.spec = SwarmSpec.load()
        self.agent = Agent(
            name=self.spec.orchestrator.name,
            instructions=self.spec.orchestrator.instructions,
            tools=[],
            llm=LLMSettings.from_env(),
        )
        kafka = _kafka(self.spec)
        # Bump ORCHESTRATOR_GROUP to re-read agent.synthesis.ready from the start.
        group = os.getenv("ORCHESTRATOR_GROUP", "flinkswarm-orchestrator")
        self.consumer = build_consumer(kafka, group_id=group)
        self.producer = build_producer(kafka)

        sr = SchemaRegistrySettings.from_env()
        sr_client = schema_registry_client(sr)
        self._synthesis_codec = ValueCodec(SynthesisReady, sr, sr_client)
        self._decision_codec = ValueCodec(DecisionFinal, sr, sr_client)
        logger.info("orchestrator schema registry: %s", "on" if sr.enabled else "off (plain JSON)")

        # The Flink PTF barrier emits exactly one append row per claim once all
        # agents have reported. `_decided` guards against a re-emit if a worker
        # somehow produces a second result for a claim.
        self._decided: set[str] = set()
        self._stop = asyncio.Event()

    def request_stop(self, *_: object) -> None:
        self._stop.set()

    async def _synthesize(self, event: SynthesisReady) -> None:
        blocks, partial = _format_findings(event.aggregated_payload)
        note = " Some agents did not report before the barrier timed out." if partial else ""
        prompt = (
            f"Claim {event.claim_id}. The sub-agent findings follow, one block "
            f"per agent.{note} Produce a single JSON object with keys `decision` "
            f"and `rationale`.\n\n{blocks}"
        )
        run = await self.agent.run(prompt)

        decision, rationale = _split_decision(run.text)
        out = DecisionFinal(
            claim_id=event.claim_id,
            decision=decision,
            rationale=rationale,
            tool_calls=run.tool_calls,
        )
        self.producer.produce(
            self.spec.topics.decisions,
            key=encode_key(event.claim_id),
            value=self._decision_codec.encode(out, self.spec.topics.decisions),
            on_delivery=delivery_report,
        )
        self.producer.poll(0)
        logger.info("decided %s: %s", event.claim_id, decision)

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.request_stop)

        self.consumer.subscribe([self.spec.topics.synthesis])
        logger.info(
            "orchestrator listening on %s -> %s",
            self.spec.topics.synthesis,
            self.spec.topics.decisions,
        )

        while not self._stop.is_set():
            msg = await loop.run_in_executor(None, self.consumer.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("consume error: %s", msg.error())
                continue

            if msg.value() is None:
                self.consumer.commit(msg, asynchronous=False)
                continue

            try:
                event = self._synthesis_codec.decode(msg.value(), self.spec.topics.synthesis)
                if not event.claim_id:
                    event.claim_id = _claim_id_of(event.aggregated_payload)
            except Exception:
                logger.exception("bad synthesis payload, skipping")
                self.consumer.commit(msg, asynchronous=False)
                continue

            if event.claim_id in self._decided:
                pass  # barrier already fired for this claim
            else:
                try:
                    await self._synthesize(event)
                    self._decided.add(event.claim_id)
                    self.producer.flush(10)
                except Exception:
                    logger.exception("synthesis failed for %s, will retry if re-emitted", event.claim_id)

            self.consumer.commit(msg, asynchronous=False)

        self.producer.flush(10)
        self.consumer.close()


def _parse_payload(aggregated_payload: str) -> dict:
    """The PTF emits {"claim_id":..., "partial":bool, "results":{agent: text}}."""
    try:
        obj = json.loads(aggregated_payload)
        return obj if isinstance(obj, dict) else {"results": {"": str(obj)}}
    except (TypeError, json.JSONDecodeError):
        return {"results": {"": aggregated_payload}}


def _claim_id_of(aggregated_payload: str) -> str:
    return str(_parse_payload(aggregated_payload).get("claim_id", "UNKNOWN"))


def _format_findings(aggregated_payload: str) -> tuple[str, bool]:
    obj = _parse_payload(aggregated_payload)
    results = obj.get("results", {}) or {}
    blocks = "\n\n".join(f"--- {name} ---\n{text}" for name, text in results.items())
    return blocks, bool(obj.get("partial", False))


def _split_decision(text: str) -> tuple[str, str]:
    """Best-effort parse of the model's answer into (decision, rationale)."""
    text = text.strip()
    try:
        start, end = text.index("{"), text.rindex("}") + 1
        obj = json.loads(text[start:end])
        return str(obj.get("decision", "UNKNOWN")).strip(), str(obj.get("rationale", text)).strip()
    except (ValueError, json.JSONDecodeError):
        first = text.splitlines()[0] if text else "UNKNOWN"
        return first[:120], text


def main() -> None:
    parser = argparse.ArgumentParser(prog="flinkswarm.orchestrator")
    sub = parser.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("dispatch", help="produce a task to the swarm")
    d.add_argument("--claim", required=True)
    d.add_argument("--prompt", required=True)
    d.add_argument("--context", default="{}", help="JSON object")

    sub.add_parser("serve", help="consume the Flink barrier output and decide")

    args = parser.parse_args()
    if args.cmd == "dispatch":
        dispatch(args.claim, args.prompt, json.loads(args.context))
    else:
        asyncio.run(OrchestratorService().start())


if __name__ == "__main__":
    main()
