"""Generic swarm worker.

One process = one worker agent. It consumes tasks, runs the LLM agent with the
tools/instructions declared for its name in `agent-spec.yaml`, and produces a
result event. The Flink barrier joins results across workers per claim_id.

Run:
    AGENT_NAME=ClaimDataAgent python -m flinkswarm.worker
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from confluent_kafka import KafkaError

from .config import KafkaSettings, LLMSettings, SchemaRegistrySettings, SwarmSpec
from .events import ResultCompleted, TaskDispatched
from .kafka import (
    ValueCodec,
    build_consumer,
    build_producer,
    decode_key,
    delivery_report,
    encode_key,
    schema_registry_client,
)
from .llm import Agent
from .tools import TOOL_REGISTRY

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("flinkswarm.worker")


class GenericSwarmWorker:
    def __init__(self) -> None:
        self.spec = SwarmSpec.load()
        self.agent_name = os.environ.get("AGENT_NAME") or self.spec.expected_agents[0]
        wspec = self.spec.worker(self.agent_name)

        # Env overrides win over spec (useful for K8s ConfigMap injection later).
        instructions = os.getenv("AGENT_INSTRUCTIONS", wspec.instructions)
        tool_names = wspec.tools
        missing = [t for t in tool_names if t not in TOOL_REGISTRY]
        if missing:
            raise RuntimeError(f"{self.agent_name}: tools not in registry: {missing}")

        self.agent = Agent(
            name=self.agent_name,
            instructions=instructions,
            tools=[TOOL_REGISTRY[t] for t in tool_names],
            llm=LLMSettings.from_env(),
        )

        self.in_topic = os.getenv("IN_TOPIC", self.spec.topics.tasks)
        self.out_topic = os.getenv("OUT_TOPIC", self.spec.topics.results)

        kafka = KafkaSettings.from_env(self.spec.fallback_bootstrap)
        self.consumer = build_consumer(kafka, group_id=f"flinkswarm-{self.agent_name}")
        self.producer = build_producer(kafka)

        sr = SchemaRegistrySettings.from_env()
        sr_client = schema_registry_client(sr)
        self._task_codec = ValueCodec(TaskDispatched, sr, sr_client)
        self._result_codec = ValueCodec(ResultCompleted, sr, sr_client)
        logger.info("[%s] schema registry: %s", self.agent_name, "on" if sr.enabled else "off (plain JSON)")

        self._stop = asyncio.Event()

    def request_stop(self, *_: object) -> None:
        logger.info("[%s] shutdown requested", self.agent_name)
        self._stop.set()

    async def _handle(self, key: str, task: TaskDispatched) -> None:
        try:
            run = await self.agent.run(task.prompt, context={"claim_id": task.claim_id, **task.context})
            event = ResultCompleted(
                claim_id=task.claim_id,
                agent_name=self.agent_name,
                status="SUCCESS",
                result=run.text,
            )
        except Exception as exc:
            logger.exception("[%s] task failed for %s", self.agent_name, task.claim_id)
            event = ResultCompleted(
                claim_id=task.claim_id,
                agent_name=self.agent_name,
                status="FAILURE",
                result="",
                error=f"{type(exc).__name__}: {exc}",
            )

        self.producer.produce(
            self.out_topic,
            key=encode_key(key),
            value=self._result_codec.encode(event, self.out_topic),
            on_delivery=delivery_report,
        )
        self.producer.poll(0)
        logger.info("[%s] -> %s (%s)", self.agent_name, task.claim_id, event.status)

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.request_stop)

        self.consumer.subscribe([self.in_topic])
        logger.info("[%s] listening on %s -> %s", self.agent_name, self.in_topic, self.out_topic)

        while not self._stop.is_set():
            msg = await loop.run_in_executor(None, self.consumer.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("[%s] consume error: %s", self.agent_name, msg.error())
                continue

            key = decode_key(msg.key())
            try:
                task = self._task_codec.decode(msg.value(), self.in_topic)
            except Exception:
                logger.exception("[%s] bad task payload, skipping", self.agent_name)
                self.consumer.commit(msg, asynchronous=False)
                continue

            await self._handle(key, task)
            self.producer.flush(10)
            self.consumer.commit(msg, asynchronous=False)

        logger.info("[%s] draining producer", self.agent_name)
        self.producer.flush(10)
        self.consumer.close()


def main() -> None:
    asyncio.run(GenericSwarmWorker().start())


if __name__ == "__main__":
    main()
