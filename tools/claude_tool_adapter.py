"""
OpenAI-style tool schema -> Claude tool schema adapter
-------------------------------------------------------
The tool registries (triage_tools.py, model_eval_tools.py) contain business
logic — SLA math, schema-drift diagnosis, briefing/report drafting — that
does not know or care which model calls it. What differs between providers
is only the shape of the tool-calling protocol:

  OpenAI-style (what an NVIDIA NIM deployment speaks):
    tool schema:       {"type": "function", "function": {name, description, parameters}}
    assistant message:  message["tool_calls"] = [{"id", "function": {name, arguments}}]
    tool reply:         {"role": "tool", "tool_call_id": ..., "content": ...}

  Claude (Anthropic Messages API):
    tool schema:        {"name", "description", "input_schema"}
    assistant content:  content blocks of type "tool_use" ({"id", "name", "input"})
    tool reply:         a user message whose content is a list of
                        {"type": "tool_result", "tool_use_id", "content"} blocks

This module is the one place that translates between them, so the tool
registries stay provider-agnostic and only this adapter — plus the agent loop
that calls it — is Claude-specific.
"""

from typing import Any, Dict, List


def to_claude_tools(openai_schemas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert an OpenAI-style `tools` list into Claude's `input_schema` shape."""
    claude_tools = []
    for schema in openai_schemas:
        fn = schema["function"]
        claude_tools.append({
            "name":         fn["name"],
            "description":  fn["description"],
            "input_schema": fn["parameters"],
        })
    return claude_tools


def extract_tool_uses(content_blocks) -> List[Dict[str, Any]]:
    """
    Pull every tool_use block out of a Claude response's content list.
    Returns [{"id", "name", "input"}, ...] — empty if the model produced no
    tool calls (it is done, or replying in prose instead).
    """
    return [
        {"id": block.id, "name": block.name, "input": block.input}
        for block in content_blocks
        if block.type == "tool_use"
    ]


def extract_text(content_blocks) -> str:
    """Concatenate every text block in a Claude response — the model's prose."""
    return "".join(block.text for block in content_blocks if block.type == "text")


def tool_result_block(tool_use_id: str, content: str, is_error: bool = False) -> Dict[str, Any]:
    """Build one `tool_result` content block for the next user turn."""
    block: Dict[str, Any] = {
        "type":        "tool_result",
        "tool_use_id": tool_use_id,
        "content":     content,
    }
    if is_error:
        block["is_error"] = True
    return block
