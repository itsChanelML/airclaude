"""
AirClaudeOperator — Airflow 3 compatible
------------------------------------------
Custom Airflow operator that runs the AirClaude agent loop inside a task,
pointed at the Claude Developer Platform (direct API, Bedrock, or Vertex —
see claude_env.py) instead of NVIDIA NIM.

The agent reasons over a goal, calls typed tools from a registry, and returns
a structured result to XCom. ESCALATE fails the task with a typed diagnosis
rather than a stack trace.

Works with any tool registry exposing TOOL_REGISTRY, TOOL_SCHEMAS,
AgentStatus, AgentResult, and (optionally) trim_for_history:
  - triage_tools      — NYC 311 triage
  - model_eval_tools  — model migration eval

Airflow 3 notes:
  - apply_defaults was removed in Airflow 3; BaseOperator handles defaults.
  - Data and tool paths resolve through claude_env.repo_root() so the DAG
    works whether it runs from the repo or from a copy in AIRFLOW_HOME.

Python 3.9 compatible.
"""

import importlib
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator

# Repo root is importable regardless of where Airflow loaded this plugin from.
_PLUGIN_DIR = Path(__file__).resolve().parent
for _candidate in (_PLUGIN_DIR.parent, *_PLUGIN_DIR.parents):
    if (_candidate / "claude_env.py").exists():
        if str(_candidate) not in sys.path:
            sys.path.insert(0, str(_candidate))
        break

from claude_env import get_model, get_client_and_error, get_provider, repo_root  # noqa: E402

_TOOLS_ADAPTER_DIR = None
for _candidate in (_PLUGIN_DIR.parent, *_PLUGIN_DIR.parents):
    if (_candidate / "tools" / "claude_tool_adapter.py").exists():
        _TOOLS_ADAPTER_DIR = str(_candidate / "tools")
        break
if _TOOLS_ADAPTER_DIR and _TOOLS_ADAPTER_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_ADAPTER_DIR)

from claude_tool_adapter import (  # noqa: E402
    to_claude_tools, extract_tool_uses, extract_text, tool_result_block,
)

# ── ANSI colors ────────────────────────────────────────────────────────────────
TEAL   = "\033[38;5;43m"
PURPLE = "\033[38;5;135m"
AMBER  = "\033[38;5;214m"
GREEN  = "\033[38;5;82m"
RED    = "\033[38;5;196m"
GRAY   = "\033[38;5;245m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

DEFAULT_MODEL = get_model()
MAX_TOKENS    = 1024

# Tool results whose payload is worth streaming into the task log — this is
# what makes the Airflow UI a viable demo surface.
STREAMED_PAYLOADS = {
    "draft_supervisor_briefing": ("briefing", "BRIEFING CONTENT (ready to send)"),
    "draft_migration_report":    ("report",   "MIGRATION REPORT"),
}

# Per-registry system prompts. Hardcoding one prompt would mean the eval DAG
# gets told to call draft_supervisor_briefing — a tool it does not have.
SYSTEM_PROMPTS = {
    "triage_tools": (
        "You are AirClaude — an autonomous operations agent for NYC 311 service "
        "requests. You triage overnight data, find SLA breaches, detect spikes, "
        "draft supervisor briefings, and surface only what requires human "
        "attention.\n\n"
        "Rules:\n"
        "1. Call validate_schema first. If it ESCALATEs, stop.\n"
        "2. Call check_sla_breaches to find overdue cases. Its result tells you "
        "which agencies have breaches and who each supervisor is — use those "
        "exact names, do not invent them.\n"
        "3. Call detect_complaint_spike to find overnight anomalies.\n"
        "4. query_requests answers counting questions about the feed (group by "
        "complaint_type, borough, district, agency, status, or supervisor, with "
        "filters such as only_breaches). Use it for anything the other tools do "
        "not directly answer.\n"
        "5. Call draft_supervisor_briefing once per agency that has breaches, "
        "passing that agency's real supervisor name and the same file_path. "
        "Start with the highest breach counts.\n"
        "6. Call generate_summary last, passing file_path and the supervisors "
        "you briefed.\n"
        "7. One tool call per turn. No prose between calls. If a tool returns "
        "RETRY, read the message, fix the arguments, and call it again."
    ),
    "model_eval_tools": (
        "You are AirClaude — an autonomous model evaluation agent. You compare "
        "two models on production eval data and produce a go/no-go migration "
        "recommendation backed by evidence.\n\n"
        "Rules:\n"
        "1. Call validate_schema first. If it ESCALATEs, stop.\n"
        "2. Call score_comparison, then detect_regression, then cost_analysis. "
        "Each takes file_path.\n"
        "3. Call draft_migration_report with the two model names and file_path. "
        "It re-reads the eval file itself — do not echo earlier tool payloads "
        "back to it.\n"
        "4. Call generate_summary last.\n"
        "5. One tool call per turn. No prose between calls. If a tool returns "
        "RETRY, read the message, fix the arguments, and call it again."
    ),
}

GENERIC_SYSTEM_PROMPT = (
    "You are AirClaude — an autonomous pipeline agent. Accomplish the goal by "
    "calling the tools available to you, one per turn, with no prose between "
    "calls. If a tool returns RETRY, read the message, fix your arguments, and "
    "call it again. Stop when the goal is met."
)


class AirClaudeOperator(BaseOperator):
    """
    Airflow operator that delegates task execution to a AirClaude agent.

    Parameters
    ----------
    goal : str
        Natural language description of what the agent must accomplish.
    context : dict
        Context payload passed to the agent (file paths, required fields, etc).
    tools_module : str
        Tool registry module to import — "triage_tools" or "model_eval_tools".
    system_prompt : str, optional
        Override the per-registry default system prompt.
    model : str
        Claude model id.
    api_key_env : str
        Env var holding the Anthropic API key (direct-provider mode only).
        Loaded from .env if present.
    max_iterations : int
        Agent turn limit before the task gives up.
    """

    template_fields = ("goal", "context")
    ui_color        = "#D97757"

    def __init__(
        self,
        goal:            str,
        context:         Optional[Dict[str, Any]] = None,
        tools_module:    str = "triage_tools",
        system_prompt:   Optional[str] = None,
        model:           Optional[str] = None,
        api_key_env:     str = "ANTHROPIC_API_KEY",
        max_iterations:  int = 14,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.goal           = goal
        self.context        = context if context is not None else {}
        self.tools_module   = tools_module
        self.system_prompt  = system_prompt
        self.model          = model or get_model()
        self.api_key_env    = api_key_env
        self.max_iterations = max_iterations

    # ── Main ───────────────────────────────────────────────────────────────────

    def execute(self, context: Any) -> dict:
        self._banner("AirClaude Agent — Starting")
        self._log_teal(f"Goal       : {self.goal[:120]}…")
        self._log_teal(f"Model      : {self.model}  ({get_provider()})")
        self._log_teal(f"Tools      : {self.tools_module}")
        self._log_divider()

        client, client_error = get_client_and_error(self.api_key_env)
        if client_error:
            raise ValueError(f"[AirClaude] {client_error}")

        mod = self._load_tools()
        tool_registry = mod.TOOL_REGISTRY
        claude_tools  = to_claude_tools(mod.TOOL_SCHEMAS)
        AgentStatus   = mod.AgentStatus
        trim          = getattr(mod, "trim_for_history", lambda name, payload: payload)

        self._log_gray(f"Tools loaded: {list(tool_registry.keys())}")
        self._log_divider()

        messages = [
            {"role": "user", "content": self._user_message()},
        ]
        system_prompt = self._resolve_system_prompt()

        final_result  = None
        called: List[str] = []

        for iteration in range(1, self.max_iterations + 1):
            self._log_purple(f"[Iteration {iteration}] Calling AirClaude…")

            response = self._call_claude(client, system_prompt, messages, claude_tools)
            if response is None:
                self._log_amber("Claude call failed — retrying in 2s…")
                time.sleep(2)
                continue

            messages.append({"role": "assistant", "content": [b.model_dump() for b in response.content]})
            tool_uses = extract_tool_uses(response.content)

            if not tool_uses:
                thought = extract_text(response.content)
                self._log_green("Agent completed reasoning — no further tool calls.")
                if thought:
                    self._log_gray(f"Final thought: {thought[:300]}")
                self._log_divider()
                break

            tool_result_blocks = []

            for tc in tool_uses:
                tool_name = tc["name"]
                args      = tc["input"] or {}

                self._log_teal(f"→ Calling tool  : {BOLD}{tool_name}{RESET}{TEAL}")
                self._log_gray( f"  Input snippet : {json.dumps(args)[:180]}")

                if tool_name not in tool_registry:
                    # Hand the mistake back as RETRY so the agent can recover
                    # instead of the task dying on a hallucinated tool name.
                    payload = json.dumps({
                        "status":  "RETRY",
                        "message": (
                            f"No such tool: {tool_name}. Available tools: "
                            f"{', '.join(tool_registry.keys())}."
                        ),
                    })
                    self._log_amber(f"Unknown tool: {tool_name} — returning RETRY.")
                    tool_result_blocks.append(tool_result_block(tc["id"], payload, is_error=True))
                    self._log_divider()
                    continue

                tool_result = self._invoke_tool(tool_registry[tool_name], args, mod)
                result_json = tool_result.model_dump_json()

                self._log_status(tool_result.status.value)
                self._log_gray(f"  Message       : {tool_result.message[:200]}")

                self._stream_payload(tool_name, tool_result)

                if tool_result.status == AgentStatus.ESCALATE:
                    self._log_red(f"\nESCALATE — {tool_name} could not complete.")
                    self._log_red(f"Reason: {tool_result.message}")
                    self._log_divider()
                    raise RuntimeError(
                        f"[AirClaude ESCALATE] {tool_name}: {tool_result.message}"
                    )

                if tool_result.status == AgentStatus.SUCCESS:
                    called.append(tool_name)
                    final_result = tool_result

                # Trim before adding to history — untrimmed payloads blow the
                # context window and make the agent stall mid-pipeline.
                tool_result_blocks.append(tool_result_block(tc["id"], trim(tool_name, result_json)))
                self._log_divider()

            messages.append({"role": "user", "content": tool_result_blocks})

            if "generate_summary" in called:
                break

        if final_result is None:
            raise RuntimeError(
                "[AirClaude] Agent loop ended without a successful tool result."
            )

        self._banner("AirClaude Agent — Complete")
        self._log_green(f"Status  : {final_result.status.value}")
        self._log_green(f"Summary : {final_result.message[:400]}")
        self._log_gray(f"Tool calls executed: {' → '.join(called)}")
        self._log_divider()

        return final_result.model_dump()

    # ── Tool registry loading ──────────────────────────────────────────────────

    def _load_tools(self):
        tools_path = str(repo_root() / "tools")
        if tools_path not in sys.path:
            sys.path.insert(0, tools_path)
        try:
            return importlib.import_module(self.tools_module)
        except ImportError as e:
            raise ImportError(
                f"[AirClaude] Could not import tools module '{self.tools_module}' "
                f"from {tools_path}. Available: "
                f"{sorted(p.stem for p in Path(tools_path).glob('*_tools.py'))}. "
                f"Original error: {e}"
            )

    # ── Claude call ────────────────────────────────────────────────────────────

    def _call_claude(self, client, system_prompt, messages, claude_tools):
        # Escalating timeouts — a hosted model can be slow under load.
        for attempt, timeout in enumerate((60, 90, 120), 1):
            try:
                return client.messages.create(
                    model=self.model,
                    max_tokens=MAX_TOKENS,
                    system=system_prompt,
                    messages=messages,
                    tools=claude_tools,
                    tool_choice={"type": "auto"},
                    timeout=timeout,
                )
            except Exception as e:
                if attempt < 3:
                    self._log_amber(
                        f"Claude call failed (attempt {attempt}/3): {e} — retrying…"
                    )
                    time.sleep(3)
                else:
                    self._log_red(f"Claude API error: {e}")
                    return None

    # ── Tool invocation ────────────────────────────────────────────────────────

    def _invoke_tool(self, tool_fn, args, mod):
        import inspect
        params = list(inspect.signature(tool_fn).parameters.values())
        if params:
            input_model = params[0].annotation
            try:
                return tool_fn(input_model(**args))
            except Exception as e:
                # RETRY, not failure — the agent gets the validation error back
                # and can correct its arguments.
                return mod.AgentResult(
                    status=mod.AgentStatus.RETRY,
                    message=f"Tool input error: {e}",
                    tool=getattr(tool_fn, "__name__", "unknown"),
                )
        return tool_fn(args)

    # ── Prompts ────────────────────────────────────────────────────────────────

    def _resolve_system_prompt(self) -> str:
        if self.system_prompt:
            return self.system_prompt
        return SYSTEM_PROMPTS.get(self.tools_module, GENERIC_SYSTEM_PROMPT)

    def _user_message(self) -> str:
        return (
            f"Goal: {self.goal}\n\n"
            f"Context:\n{json.dumps(self.context, indent=2, default=str)}\n\n"
            "Begin. Start with schema validation."
        )

    # ── Logging ────────────────────────────────────────────────────────────────

    def _stream_payload(self, tool_name, tool_result):
        spec = STREAMED_PAYLOADS.get(tool_name)
        if not spec:
            return
        key, label = spec
        body = tool_result.data.get(key)
        if not body:
            return
        self._log_divider()
        self._log_teal(f"  {label}:")
        for line in body.split("\n"):
            self._log_gray(f"  {line}")
        self._log_divider()

    def _banner(self, text):
        w = 62
        self.log.info(f"{BOLD}{TEAL}{'─'*w}{RESET}")
        self.log.info(f"{BOLD}{TEAL}  {text}{RESET}")
        self.log.info(f"{BOLD}{TEAL}{'─'*w}{RESET}")

    def _log_divider(self): self.log.info(f"{GRAY}{'·'*52}{RESET}")
    def _log_teal(self,   m): self.log.info(f"{TEAL}{m}{RESET}")
    def _log_purple(self, m): self.log.info(f"{PURPLE}{m}{RESET}")
    def _log_green(self,  m): self.log.info(f"{GREEN}{m}{RESET}")
    def _log_amber(self,  m): self.log.info(f"{AMBER}{m}{RESET}")
    def _log_red(self,    m): self.log.info(f"{RED}{BOLD}{m}{RESET}")
    def _log_gray(self,   m): self.log.info(f"{GRAY}{m}{RESET}")

    def _log_status(self, s):
        c = GREEN if s == "SUCCESS" else (AMBER if s == "RETRY" else RED)
        self.log.info(f"{c}{BOLD}  Status        : {s}{RESET}")
