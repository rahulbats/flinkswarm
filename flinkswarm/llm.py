"""Minimal tool-calling agent over an OpenAI-compatible endpoint.

Targets a local MLX server (Qwen), e.g. `mlx_lm.server` or `mlx-omni-server`.
No agent framework dependency — just the chat/completions API plus a tool loop.
"""

from __future__ import annotations

import inspect
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from openai import AsyncOpenAI

from .config import LLMSettings

logger = logging.getLogger("flinkswarm.llm")

ToolFn = Callable[..., Any | Awaitable[Any]]


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

            resp = await self._client.chat.completions.create(**kwargs)
            msg = resp.choices[0].message
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
        final = await self._client.chat.completions.create(
            model=self.llm.model,
            messages=messages + [{"role": "user", "content": "Give your final answer now."}],
            temperature=self.llm.temperature,
        )
        return RunResult(text=(final.choices[0].message.content or "").strip(), tool_calls=used)
