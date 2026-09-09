"""Minimal tool-calling agent over an OpenAI-compatible endpoint.

Targets a local MLX server (Qwen), e.g. `mlx_lm.server` or `mlx-omni-server`.
No agent framework dependency — just the chat/completions API plus a tool loop.
"""

from __future__ import annotations

import inspect
import json
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from openai import AsyncOpenAI

from .config import LLMSettings

logger = logging.getLogger("flinkswarm.llm")

ToolFn = Callable[..., Any | Awaitable[Any]]

# Set LLM_LOG_PROMPTS=1 to print every request/response to/from the model.
_TRACE = os.getenv("LLM_LOG_PROMPTS", "").lower() in {"1", "true", "yes", "on"}


def _trace_request(agent: str, step: int, messages: list[dict], tools: list | None) -> None:
    if not _TRACE:
        return
    print(f"\n── llm ▸ {agent} ▸ request (step {step}) ──", flush=True)
    for m in messages:
        role = m.get("role", "?")
        if m.get("tool_calls"):
            for tc in m["tool_calls"]:
                fn = tc["function"]["name"] if isinstance(tc, dict) else tc.function.name
                ar = tc["function"]["arguments"] if isinstance(tc, dict) else tc.function.arguments
                print(f"  [{role}→call] {fn}({ar})", flush=True)
        else:
            body = (m.get("content") or "").strip()
            print(f"  [{role}] {body}", flush=True)
    if tools:
        print(f"  tools offered: {', '.join(t['function']['name'] for t in tools)}", flush=True)


def _trace_response(agent: str, msg: Any) -> None:
    if not _TRACE:
        return
    print(f"── llm ▸ {agent} ▸ response ──", flush=True)
    for tc in (msg.tool_calls or []):
        print(f"  → {tc.function.name}({tc.function.arguments})", flush=True)
    if msg.content:
        print(f"  {msg.content.strip()}", flush=True)


@dataclass
class Tool:
    name: str
    description: str
    parameters: dict  # JSON Schema for the arguments object
    fn: ToolFn

    def schema(self) -> dict:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }

    async def invoke(self, args: dict) -> Any:
        result = self.fn(**args)
        if inspect.isawaitable(result):
            result = await result
        return result


@dataclass
class RunResult:
    text: str
    tool_calls: list[str] = field(default_factory=list)


class Agent:
    def __init__(self, name: str, instructions: str, tools: list[Tool], llm: LLMSettings):
        self.name = name
        self.instructions = instructions or f"You are {name}."
        self.tools = {t.name: t for t in tools}
        self.llm = llm
        self._client = AsyncOpenAI(base_url=llm.base_url, api_key=llm.api_key)

    async def run(self, prompt: str, context: dict | None = None) -> RunResult:
        system = self.instructions
        if context:
            system += "\n\nContext:\n" + json.dumps(context, default=str)

        messages: list[dict] = [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ]
        tool_schemas = [t.schema() for t in self.tools.values()] or None
        used: list[str] = []

        for step in range(self.llm.max_tool_iters):
            kwargs: dict[str, Any] = {
                "model": self.llm.model,
                "messages": messages,
                "temperature": self.llm.temperature,
            }
            if tool_schemas:
                kwargs["tools"] = tool_schemas
                kwargs["tool_choice"] = "auto"

            _trace_request(self.name, step, messages, tool_schemas)
            resp = await self._client.chat.completions.create(**kwargs)
            msg = resp.choices[0].message
            _trace_response(self.name, msg)
            calls = msg.tool_calls or []

            if not calls:
                return RunResult(text=(msg.content or "").strip(), tool_calls=used)

            messages.append(msg.model_dump(exclude_none=True))
            for i, tc in enumerate(calls):
                fn_name = tc.function.name
                call_id = tc.id or f"call_{step}_{i}"
                try:
                    args = json.loads(tc.function.arguments or "{}")
                except json.JSONDecodeError:
                    args = {}
                used.append(fn_name)

                tool = self.tools.get(fn_name)
                if tool is None:
                    output: Any = {"error": f"unknown tool: {fn_name}"}
                else:
                    try:
                        output = await tool.invoke(args)
                    except Exception as exc:  # surface tool errors back to the model
                        logger.exception("tool %s failed", fn_name)
                        output = {"error": f"{type(exc).__name__}: {exc}"}

                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": json.dumps(output, default=str),
                    }
                )

        # Out of tool iterations — ask for a final answer with no tools.
        wrap = messages + [{"role": "user", "content": "Give your final answer now."}]
        _trace_request(self.name, self.llm.max_tool_iters, wrap, None)
        final = await self._client.chat.completions.create(
            model=self.llm.model,
            messages=wrap,
            temperature=self.llm.temperature,
        )
        _trace_response(self.name, final.choices[0].message)
        return RunResult(text=(final.choices[0].message.content or "").strip(), tool_calls=used)
