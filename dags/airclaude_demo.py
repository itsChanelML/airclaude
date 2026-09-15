"""
AirClaude Demo DAG — NYC 311 Productivity Agent
---------------------------------------------------
Same pipeline as AirClaw's airclaw_demo DAG, running on the Claude Developer
Platform instead of NVIDIA NIM. Reviews overnight 311 requests, finds SLA
breaches, detects complaint spikes, and drafts supervisor briefings.

Three tasks:

  [1] ingest_overnight  →  [2] airclaude_agent  →  [3] dispatch_briefings

Task 1: Loads the upstream 311 feed, packages overnight context.
Task 2: AirClaudeOperator — agent finds breaches, detects spikes,
        drafts supervisor briefings. Returns structured output.
Task 3: Receives agent output and "dispatches" briefings
        (prints them — in production this would be email/Slack).

Demo commands:
    Happy path:    cp data/nyc_311_clean.csv  data/nyc_311_upstream.csv
    Failure beat:  cp data/nyc_311_broken.csv data/nyc_311_upstream.csv
    Then:          airflow dags trigger airclaude_demo
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

# ── Path bootstrap ─────────────────────────────────────────────────────────────
# Airflow parses DAG files in isolation, so make the repo importable regardless
# of whether this file is being read from the repo itself or from a copy in
# AIRFLOW_HOME/dags. Set AIRCLAUDE_HOME to override.
import sys as _sys
from pathlib import Path as _Path

def _bootstrap_paths():
    import os as _os
    candidates = []
    explicit = _os.environ.get("AIRCLAUDE_HOME")
    if explicit:
        candidates.append(_Path(explicit).expanduser())
    here = _Path(__file__).resolve()
    candidates.extend([here.parent.parent, *here.parents])
    for base in candidates:
        if (base / "claude_env.py").exists() or (base / "tools").is_dir():
            for path in (base, base / "plugins", base / "tools"):
                if path.is_dir() and str(path) not in _sys.path:
                    _sys.path.insert(0, str(path))
            return

_bootstrap_paths()

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from claude_env import data_file
from rebase_data import describe_shift, rebase_csv
from airclaude_operator import AirClaudeOperator

# ── Config ─────────────────────────────────────────────────────────────────────

UPSTREAM_FILE = str(data_file("nyc_311_upstream.csv"))
CLEAN_FILE    = str(data_file("nyc_311_clean.csv"))

REQUIRED_FIELDS = [
    "unique_key", "created_date", "complaint_type", "borough", "district",
    "agency", "supervisor", "status", "closed_date", "hours_open",
    "sla_hours", "sla_breach",
]

AGENT_GOAL = (
    "You are the morning operations agent for NYC 311. It is 6am. "
    "Your job is to do the triage work that a supervisor would otherwise spend "
    "30-60 minutes doing manually. "
    "Step 1: Validate the schema — stop immediately if it has drifted. "
    "Step 2: Find all open requests that have breached their SLA window. "
    "Step 3: Detect any complaint types that spiked overnight vs the baseline. "
    "Step 4: For each agency with SLA breaches, draft a ready-to-send morning "
    "briefing addressed to that agency's supervisor — include specific case IDs, "
    "districts, and recommended actions. "
    "Step 5: Generate a one-paragraph duty manager summary of everything found. "
    "Be decisive. Surface only what requires human attention. "
    "The supervisor's first action of the day should be approving your work, "
    "not building it."
)

# ── DAG ────────────────────────────────────────────────────────────────────────

default_args = {
    "owner":            "chanel",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="airclaude_demo",
    description="AirClaude — NYC 311 Productivity Agent (SLA monitoring + supervisor briefings)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    # Production intent is 6am daily: schedule="0 6 * * *".
    # None here on purpose. With a cron schedule and catchup=False, Airflow
    # still queues the most recent interval the moment the scheduler starts —
    # so simply booting the server fired a live agent run (and real API calls)
    # that nobody asked for. Demo runs are triggered by hand.
    schedule=None,
    catchup=False,
    tags=["airclaude", "claude", "311", "productivity", "demo"],
) as dag:

    # ── Task 1: Ingest ─────────────────────────────────────────────────────────

    def ingest_overnight(**kwargs):
        """
        Load the upstream 311 feed and summarize what came in overnight.
        Pushes context to XCom so the agent knows what it's working with.
        """
        import csv, shutil
        path = Path(UPSTREAM_FILE)

        if not path.exists():
            shutil.copy(CLEAN_FILE, path)
            print("[ingest] Initialized upstream file from nyc_311_clean.csv")

        # A real ingest pulls a fresh feed; this rebases the sample's timestamps
        # onto now so the SLA and spike windows below mean something. Done in
        # place so a manually swapped broken file stays broken for the demo.
        shift = rebase_csv(path, path)
        print(f"[ingest] Dates rebased {describe_shift(shift)} — newest request is now")

        with open(path, newline="") as f:
            rows = list(csv.DictReader(f))

        now       = datetime.now()
        overnight = now - timedelta(hours=24)

        overnight_rows  = [r for r in rows if r.get("created_date","") >= overnight.strftime("%Y-%m-%dT%H:%M:%S")]
        breach_count    = sum(1 for r in rows if r.get("sla_breach","") == "YES")
        agencies        = list({r.get("agency","") for r in rows if r.get("agency","")})

        context = {
            "file_path":        str(path.resolve()),
            "required_fields":  REQUIRED_FIELDS,
            "total_rows":       len(rows),
            "overnight_count":  len(overnight_rows),
            "known_breaches":   breach_count,
            "agencies":         agencies,
            "as_of":            now.strftime("%B %d, %Y at %I:%M %p"),
            "source":           "data.cityofnewyork.us/resource/erm2-nwe9",
            "description":      "NYC 311 overnight triage — SLA monitoring and supervisor briefing pipeline",
        }

        print(f"[ingest] Total requests  : {len(rows)}")
        print(f"[ingest] Overnight (24h) : {len(overnight_rows)}")
        print(f"[ingest] SLA breaches    : {breach_count}")
        print(f"[ingest] Agencies        : {agencies}")
        print(f"[ingest] Context packaged — handing off to AirClaude agent.")

        kwargs["ti"].xcom_push(key="ingest_context", value=context)
        return context

    task_ingest = PythonOperator(
        task_id="ingest_overnight",
        python_callable=ingest_overnight,
    )

    # ── Task 2: AirClaude Agent ───────────────────────────────────────────────

    task_agent = AirClaudeOperator(
        task_id="airclaude_agent",
        goal=AGENT_GOAL,
        context={
            "file_path":       UPSTREAM_FILE,
            "required_fields": REQUIRED_FIELDS,
            "description":     "NYC 311 overnight triage — SLA monitoring and supervisor briefing pipeline",
            "as_of":           datetime.now().strftime("%B %d, %Y at %I:%M %p"),
        },
        tools_module="airclaw_tools",
        api_key_env="ANTHROPIC_API_KEY",
        max_iterations=14,
    )

    # ── Task 3: Dispatch Briefings ─────────────────────────────────────────────

    def dispatch_briefings(**kwargs):
        """
        Receive the agent's structured output and dispatch the briefings.
        For demo: prints each briefing with a clean separator.
        In production: send via email API, post to Slack, write to ops dashboard.
        """
        ti     = kwargs["ti"]
        result = ti.xcom_pull(task_ids="airclaude_agent")

        TEAL  = "\033[38;5;43m"
        GREEN = "\033[38;5;82m"
        BOLD  = "\033[1m"
        RESET = "\033[0m"
        LINE  = "═" * 62

        print(f"\n{BOLD}{TEAL}{LINE}{RESET}")
        print(f"{BOLD}{TEAL}  AirClaude — Briefings Ready for Dispatch{RESET}")
        print(f"{BOLD}{TEAL}{LINE}{RESET}\n")

        if not result:
            print("[dispatch] No result from agent — check task 2 logs.")
            return

        data = result.get("data", {})

        summary = result.get("message", "")
        if summary:
            print(f"{TEAL}DUTY MANAGER SUMMARY:{RESET}")
            print(f"  {summary}\n")

        briefings_sent = data.get("briefings_sent", [])
        all_breaches   = data.get("total_breaches", 0)
        all_spikes     = data.get("total_spikes", 0)

        print(f"{GREEN}✓ SLA breaches surfaced : {all_breaches}{RESET}")
        print(f"{GREEN}✓ Complaint spikes      : {all_spikes}{RESET}")
        print(f"{GREEN}✓ Briefings dispatched  : {len(briefings_sent)}{RESET}")

        print(f"\n{TEAL}XCom payload (structured output):{RESET}")
        print(json.dumps(data, indent=2)[:800])

        print(f"\n{BOLD}{TEAL}{LINE}{RESET}")
        print(f"{TEAL}  Pipeline complete. Supervisors have been briefed.{RESET}")
        print(f"{TEAL}  No manual triage required this morning.{RESET}")
        print(f"{BOLD}{TEAL}{LINE}{RESET}\n")

        return result

    task_dispatch = PythonOperator(
        task_id="dispatch_briefings",
        python_callable=dispatch_briefings,
    )

    # ── Wire ───────────────────────────────────────────────────────────────────
    task_ingest >> task_agent >> task_dispatch
