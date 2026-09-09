from __future__ import annotations

from ..llm import Tool, ToolFn

TOOL_REGISTRY: dict[str, Tool] = {}


def register(name: str, description: str, parameters: dict):
    """Decorator: add a function to TOOL_REGISTRY as a callable tool.

    `parameters` is a JSON Schema object describing the arguments.
    """

    def decorator(fn: ToolFn) -> ToolFn:
        if name in TOOL_REGISTRY:
            raise ValueError(f"duplicate tool registration: {name}")
        TOOL_REGISTRY[name] = Tool(name=name, description=description, parameters=parameters, fn=fn)
        return fn

    return decorator


def load_builtin_tools() -> None:
    """Import the built-in tool modules so their @register calls run."""
    from . import claim_tools, policy_tools  # noqa: F401
