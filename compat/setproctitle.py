"""
No-op stand-in for the `setproctitle` C extension, used only by run_airflow.sh.

Why this exists
---------------
Gunicorn (which Airflow's scheduler and triggerer each run as a log server on
8793 / 8794) relabels its forked worker processes with setproctitle. On macOS
the library's Darwin path calls CFBundleGetFunctionPointerForName, which reaches
into CoreFoundation's os_log machinery — and that is not fork-safe. Every forked
worker takes SIGSEGV immediately and gunicorn respawns it forever, producing
thousands of

    [ERROR] Worker (pid:N) was sent SIGSEGV!

lines while Airflow itself is perfectly healthy. Crash report confirms it:

    _setproctitle...so  darwin_set_process_title
      -> CFBundleGetFunctionPointerForName
        -> CoreFoundation -> _os_log_preferences_refresh   SIGSEGV

setproctitle is purely cosmetic — it changes what `ps` shows and nothing else.
Both gunicorn and apache-airflow-core tolerate its absence. Shadowing it with
these no-ops removes the crash without uninstalling a real dependency.

Scope: run_airflow.sh puts this directory first on PYTHONPATH, so only the
Airflow processes that script launches see it. Everything else on the machine
keeps the real library.
"""


def setproctitle(title):  # noqa: D103
    return None


def getproctitle():  # noqa: D103
    return ""


def setthreadtitle(title):  # noqa: D103
    return None


def getthreadtitle():  # noqa: D103
    return ""
