#!/usr/bin/env bash
#
# AirClaude — one-command Airflow 3 startup
# --------------------------------------------------------------------
# Same script as AirClaw's run_airflow.sh, pointed at this repo's dags/ and
# plugins/ directories, so there is nothing to copy and nothing to keep in
# sync. Then starts the API server and scheduler together.
#
#   ./run_airflow.sh          # start Airflow, UI on http://localhost:8080
#   ./run_airflow.sh --check  # parse DAGs and exit (no server)
#
# Requires ANTHROPIC_API_KEY in .env (direct provider) — or AWS/GCP
# credentials configured, for the Bedrock/Vertex providers — for the agent
# tasks to run.
# --------------------------------------------------------------------

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export AIRCLAUDE_HOME="$REPO"
export AIRFLOW_HOME="${AIRFLOW_HOME:-$REPO/airflow_home}"
export AIRFLOW__CORE__DAGS_FOLDER="$REPO/dags"
export AIRFLOW__CORE__PLUGINS_FOLDER="$REPO/plugins"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
# Console noise suppression — same rationale as AirClaw's run_airflow.sh.
export PYTHONWARNINGS="${PYTHONWARNINGS:-ignore}"
export AIRFLOW__LOGGING__LOGGING_LEVEL="${AIRFLOW__LOGGING__LOGGING_LEVEL:-WARNING}"
export AIRFLOW__LOGGING__CELERY_LOGGING_LEVEL=WARNING
export AIRFLOW__TRACES__OTEL_ON=False

# macOS fork safety — same fix as AirClaw. See compat/setproctitle.py.
export no_proxy="${no_proxy:-*}"
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export PYTHONPATH="$REPO/compat${PYTHONPATH:+:$PYTHONPATH}"
# Belt-and-braces: both DAGs are schedule=None, but keep new DAGs paused on
# creation so nothing can ever run without an explicit trigger.
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True

mkdir -p "$AIRFLOW_HOME"

# `airflow standalone` does not reap its gunicorn children on Ctrl-C — they get
# reparented to init and keep holding 8793 (worker log server) and 8794
# (triggerer). The next start then floods the console with
# "[ERROR] Address already in use" once a second, forever. Clear them first.
for port in 8793 8794; do
  pids="$(lsof -nP -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  if [ -n "$pids" ]; then
    echo "→ Port $port held by stale process(es): $pids — clearing."
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null || true
    sleep 1
    pids="$(lsof -nP -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
    # shellcheck disable=SC2086
    [ -n "$pids" ] && kill -9 $pids 2>/dev/null || true
  fi
done

echo "AIRFLOW_HOME : $AIRFLOW_HOME"
echo "DAGs         : $AIRFLOW__CORE__DAGS_FOLDER"
echo "Plugins      : $AIRFLOW__CORE__PLUGINS_FOLDER"
echo

echo "→ Migrating metadata database…"
airflow db migrate >/dev/null 2>&1
echo "  done."

echo "→ Parsing DAGs…"
airflow dags reserialize >/dev/null 2>&1
if [ -n "$(airflow dags list-import-errors 2>/dev/null | grep -v 'No data found' | grep -v '^$' || true)" ]; then
  echo "  DAG import errors:"
  airflow dags list-import-errors
  exit 1
fi
airflow dags list 2>/dev/null | grep -E "airclaude_demo|airclaude_model_eval_demo" || true
echo "  both DAGs parsed cleanly."

if [ "${1:-}" = "--check" ]; then
  echo
  echo "Check complete. Run without --check to start the server."
  exit 0
fi

airflow dags unpause airclaude_demo            >/dev/null 2>&1 || true
airflow dags unpause airclaude_model_eval_demo >/dev/null 2>&1 || true

PWFILE="$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
echo
echo "→ Starting Airflow. UI: http://localhost:8080"
echo "  Login: admin"
if [ -f "$PWFILE" ]; then
  PW="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1])).get("admin",""))' "$PWFILE" 2>/dev/null || true)"
  if [ -n "$PW" ]; then
    echo "  Password: $PW"
  else
    echo "  Password: see $PWFILE"
  fi
else
  echo "  Password: generated on first start — see $PWFILE"
fi
echo "  Trigger from the UI, or in another terminal:"
echo "    airflow dags trigger airclaude_demo"
echo "    airflow dags trigger airclaude_model_eval_demo"
echo "  Ctrl-C to stop."
echo

exec airflow standalone
