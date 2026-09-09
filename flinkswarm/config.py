"""Runtime configuration: environment (secrets, endpoints) + the swarm spec YAML."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()


# --------------------------------------------------------------------------- #
# Environment-driven settings (secrets, endpoints)
# --------------------------------------------------------------------------- #
@dataclass
class KafkaSettings:
    bootstrap_servers: str
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "PLAIN"
    sasl_username: str | None = None
    sasl_password: str | None = None

    @classmethod
    def from_env(cls, fallback_bootstrap: str = "") -> KafkaSettings:
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", fallback_bootstrap),
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            sasl_username=os.getenv("KAFKA_SASL_USERNAME"),
            sasl_password=os.getenv("KAFKA_SASL_PASSWORD"),
        )

    def base_config(self) -> dict:
        if not self.bootstrap_servers:
            raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is not set")
        cfg: dict = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
        }
        if self.sasl_username and self.sasl_password:
            cfg.update(
                {
                    "sasl.mechanisms": self.sasl_mechanism,
                    "sasl.username": self.sasl_username,
                    "sasl.password": self.sasl_password,
                }
            )
        return cfg


@dataclass
class LLMSettings:
    base_url: str = "http://localhost:10240/v1"
    model: str = "mlx-community/Qwen2.5-7B-Instruct-4bit"
    api_key: str = "not-needed"
    temperature: float = 0.2
    max_tool_iters: int = 6

    @classmethod
    def from_env(cls) -> LLMSettings:
        return cls(
            base_url=os.getenv("MLX_BASE_URL", cls.base_url),
            model=os.getenv("MLX_MODEL", cls.model),
            api_key=os.getenv("MLX_API_KEY", cls.api_key),
            temperature=float(os.getenv("LLM_TEMPERATURE", cls.temperature)),
            max_tool_iters=int(os.getenv("LLM_MAX_TOOL_ITERS", cls.max_tool_iters)),
        )


@dataclass
class SchemaRegistrySettings:
    url: str | None = None
    api_key: str | None = None
    api_secret: str | None = None

    @classmethod
    def from_env(cls) -> SchemaRegistrySettings:
        return cls(
            url=os.getenv("SCHEMA_REGISTRY_URL"),
            api_key=os.getenv("SCHEMA_REGISTRY_API_KEY"),
            api_secret=os.getenv("SCHEMA_REGISTRY_API_SECRET"),
        )

    @property
    def enabled(self) -> bool:
        return bool(self.url)

    def client_config(self) -> dict:
        cfg: dict = {"url": self.url}
        if self.api_key and self.api_secret:
            cfg["basic.auth.user.info"] = f"{self.api_key}:{self.api_secret}"
        return cfg


# --------------------------------------------------------------------------- #
# Swarm spec (agent-spec.yaml) — shape mirrors the future Go operator's CRD
# --------------------------------------------------------------------------- #
@dataclass
class Topics:
    tasks: str = "agent.tasks.dispatched"
    results: str = "agent.results.completed"
    synthesis: str = "agent.synthesis.ready"
    decisions: str = "agent.decisions.final"


@dataclass
class WorkerSpec:
    name: str
    replicas: int = 1
    image: str = ""
    tools: list[str] = field(default_factory=list)
    instructions: str = ""


@dataclass
class OrchestratorSpec:
    name: str = "Orchestrator"
    image: str = ""
    instructions: str = ""


@dataclass
class SwarmSpec:
    name: str
    barrier_key: str
    timeout_seconds: int
    barrier_uid: str
    fallback_bootstrap: str
    topics: Topics
    workers: list[WorkerSpec]
    orchestrator: OrchestratorSpec

    @classmethod
    def load(cls, path: str | os.PathLike | None = None) -> SwarmSpec:
        path = cls._resolve_path(path)
        doc = yaml.safe_load(path.read_text())
        spec = doc["spec"]

        kafka = spec.get("kafka", {})
        topics = Topics(**{**Topics().__dict__, **kafka.get("topics", {})})
        flink = spec.get("flink", {})
        orch = spec.get("orchestrator", {})

        return cls(
            name=doc["metadata"]["name"],
            barrier_key=flink.get("barrierKey", "claim_id"),
            timeout_seconds=int(flink.get("timeoutSeconds", 30)),
            barrier_uid=flink.get("barrierUid", "flinkswarm-barrier-v1"),
            fallback_bootstrap=kafka.get("bootstrapServers", ""),
            topics=topics,
            workers=[
                WorkerSpec(
                    name=w["name"],
                    replicas=int(w.get("replicas", 1)),
                    image=w.get("image", ""),
                    tools=list(w.get("tools", [])),
                    instructions=(w.get("instructions") or "").strip(),
                )
                for w in spec.get("workers", [])
            ],
            orchestrator=OrchestratorSpec(
                name=orch.get("name", "Orchestrator"),
                image=orch.get("image", ""),
                instructions=(orch.get("instructions") or "").strip(),
            ),
        )

    @staticmethod
    def _resolve_path(path: str | os.PathLike | None) -> Path:
        if path:
            return Path(path)
        env = os.getenv("SWARM_SPEC")
        if env:
            return Path(env)
        cwd = Path("agent-spec.yaml")
        if cwd.is_file():
            return cwd
        # fall back to the copy next to the repo root (parent of this package)
        return Path(__file__).resolve().parent.parent / "agent-spec.yaml"

    def worker(self, name: str) -> WorkerSpec:
        for w in self.workers:
            if w.name == name:
                return w
        raise KeyError(f"worker {name!r} not found in spec (have: {[w.name for w in self.workers]})")

    @property
    def expected_agents(self) -> list[str]:
        return [w.name for w in self.workers]
