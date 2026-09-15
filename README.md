# AirClaude 🦾
### A self-healing pipeline agent, ported to the Claude Developer Platform

> Same tool-calling agent, same typed contracts, same demo data — pointed at Claude instead of NVIDIA NIM.

---

## What this is

[An earlier version of this project](https://github.com/itsChanelML/airclaw) was a self-healing pipeline agent I built for DevFest DC: Apache Airflow for orchestration, a small hand-rolled tool-calling loop for reasoning, and a `SUCCESS / RETRY / ESCALATE` contract so failures surface as typed diagnoses instead of stack traces.

**AirClaude is the same architecture, rebuilt against the [Claude Developer Platform](https://docs.claude.com/en/docs/agents-and-tools/claude-developer-platform)** — the Anthropic Messages API, with tool use, running through the direct API, AWS Bedrock, or Google Vertex AI as a config flag rather than a rewrite.

The point of building it twice: the agent *architecture* — typed tool contracts, idempotent tools, structured escalation, context-window trimming — turned out to be entirely provider-agnostic. Only the wire protocol changes. This repo is that seam, made explicit.

Two pipelines, unchanged from the original:

**Pipeline 1 — NYC 311 Triage Agent**
Finds every open request that's breached its SLA window, detects overnight complaint spikes, and drafts a ready-to-send supervisor briefing per agency — the 30–60 minutes of manual triage a city agency supervisor does every morning.

**Pipeline 2 — Model Migration Eval Agent**
Compares two models on production eval data — quality by category, regressions, refusal rate, cost and latency — and writes a go/no-go migration report. The analysis a Sr Engineer spends 2–3 days on manually.

---

## What actually changed vs. the original

| | Original | AirClaude (this repo) |
|---|---|---|
| Model provider | NVIDIA NIM (`nemotron-3-super-120b-a12b`) | Claude, via direct API / Bedrock / Vertex |
| Tool-calling protocol | OpenAI-style (`tool_calls`, `role: tool`) | Anthropic Messages API (`tool_use` / `tool_result` content blocks) |
| System prompt | A `role: system` message | A top-level `system` request parameter |
| Business logic (`tools/triage_tools.py`, `tools/model_eval_tools.py`, `tools/schema_diff.py`) | — | **Copied verbatim.** SLA math, spike detection, schema-drift diagnosis, briefing/report drafting don't know or care which model calls them. |
| Orchestration (Airflow DAGs, `rebase_data.py`, macOS fork-safety shim) | — | **Copied verbatim**, operator swapped underneath. |
| New: provider abstraction (`claude_env.py`) | — | One config value (`CLAUDE_PROVIDER`) selects `anthropic.Anthropic()`, `AnthropicBedrock()`, or `AnthropicVertex()` — same `.messages.create()` call either way. |
| New: protocol adapter (`tools/claude_tool_adapter.py`) | — | Converts the OpenAI-style `TOOL_SCHEMAS` list each registry already exports into Claude's `input_schema` shape, so the tool registries never had to be touched. |

Everything in the table's left column that isn't "—" is the part of the system that had to change to move providers. Everything else — roughly two-thirds of the original codebase by file count — ported with zero edits.

---

## Architecture

```
[Airflow DAG]  →  [AirClaudeOperator]  →  [Tool Registry]
      ↕                     ↕                     ↕
  Schedule            Reasons + Acts         Python callables
  Contract        anthropic.messages.create   Typed schemas
  Monitor            (direct/Bedrock/Vertex)  RETRY/ESCALATE/SUCCESS
```

---

## Project structure

```
airclaude/
├── dags/
│   ├── airclaude_demo.py            # 311 triage DAG
│   └── model_eval_demo.py            # Model eval DAG
├── data/                             # Sample data for both demo pipelines
├── plugins/
│   └── airclaude_operator.py        # Airflow operator — Claude Messages API loop
├── tools/
│   ├── triage_tools.py               # 311 tool registry
│   ├── model_eval_tools.py           # Model eval tool registry — copied verbatim
│   ├── schema_diff.py                # Shared schema-drift diagnosis — copied verbatim
│   └── claude_tool_adapter.py        # NEW — OpenAI-schema -> Claude-schema adapter
├── compat/
│   └── setproctitle.py               # macOS fork-safety shim — copied verbatim
├── claude_env.py                     # NEW — .env loading, provider selection, client factory
├── rebase_data.py                    # Demo data rebasing — copied verbatim
├── preflight.py                      # Pre-run check (deps, credentials, model reachability, data)
├── run_demo.py                       # 311 standalone runner — no Airflow needed
├── run_model_eval.py                 # Model eval standalone runner — no Airflow needed
├── run_airflow.sh                    # One-command Airflow 3 startup
├── requirements.txt
├── .env.example
└── README.md
```

---

## Quick start

### 1. Install

```bash
cd airclaude
pip3 install -r requirements.txt
```

### 2. Configure a provider

```bash
cp .env.example .env
```

**Direct API (fastest to try):**
```bash
# In .env:
CLAUDE_PROVIDER=direct
ANTHROPIC_API_KEY=sk-ant-...   # https://console.anthropic.com/settings/keys
```

**AWS Bedrock:**
```bash
# In .env:
CLAUDE_PROVIDER=bedrock
AWS_REGION=us-east-1
# Auth via the standard AWS credential chain — `aws configure`, an assumed
# role, or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY.
pip3 install "anthropic[bedrock]"
```

**Google Vertex AI:**
```bash
# In .env:
CLAUDE_PROVIDER=vertex
GOOGLE_CLOUD_PROJECT=your-gcp-project
GOOGLE_CLOUD_REGION=us-east5
gcloud auth application-default login
pip3 install "anthropic[vertex]"
```

The agent loop, tools, and demo data are identical across all three — only `claude_env.get_client_and_error()` changes.

### 3. Check you're ready

```bash
python3 preflight.py
```

Verifies dependencies, that the selected provider's credentials work, that the model is actually reachable with a live tool call, the data files, the rebasing invariants, tool-registry parity, and the OpenAI→Claude schema conversion.

### 4. Run the 311 triage demo

```bash
python3 run_demo.py                          # happy path
python3 run_demo.py --break                  # schema drift -> ESCALATE beat
python3 run_demo.py --goal "Which agency has the most overdue requests in Brooklyn?"
```

### 5. Run the model eval demo

```bash
python3 run_model_eval.py               # happy path
python3 run_model_eval.py --break       # ESCALATE beat
```

### 6. Run with Airflow (optional)

```bash
./run_airflow.sh --check   # parse both DAGs and exit
./run_airflow.sh           # start Airflow, UI on http://localhost:8080

airflow dags trigger airclaude_demo
airflow dags trigger airclaude_model_eval_demo
```

---

## Why this is the project I built for the Anthropic Lead Technical Instructor role

The tool registries, contracts, and Airflow orchestration in this repo already existed — I didn't invent a toy example to learn Claude tool use on. I took a system I'd already put in front of a live conference audience and asked a narrower question: **how much of "an agent" is actually provider-specific?**

The answer, concretely, is `claude_env.py` (client selection) and `tools/claude_tool_adapter.py` (protocol translation) — everything else was portable. That's the same claim the role's preferred qualifications make about the Claude Developer Platform (direct API, Bedrock, Vertex, Foundry as interchangeable deployment surfaces for the same agent), demonstrated in code rather than asserted in a resume bullet.

---

## Built on

- **Anthropic Messages API** — tool use, direct API / Bedrock / Vertex
- **Apache Airflow** — orchestration, scheduling, observability
- **Pydantic** — typed tool schemas
- Business logic and orchestration ported unchanged from [an earlier version of this project](https://github.com/itsChanelML/airclaw)

---

## About

Built by **Chanel Power** — Senior ML Engineer, Startup Advisor and Founder of [Mentor Me Collective](https://mentormecollective.org)

- GitHub: [@itsChanelML](https://github.com/itsChanelML)
- LinkedIn: [Chanel Power](https://linkedin.com/in/powerc1)
- Community: [mentormecollective.org](https://mentormecollective.org)
