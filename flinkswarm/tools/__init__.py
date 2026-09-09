"""Central tool registry. Workers resolve tool names from `agent-spec.yaml`
against TOOL_REGISTRY at startup.
"""

from .registry import TOOL_REGISTRY, load_builtin_tools, register

load_builtin_tools()

__all__ = ["TOOL_REGISTRY", "load_builtin_tools", "register"]
