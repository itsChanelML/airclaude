"""
AirClaude Environment Loader
-----------------------------
Single place that loads .env, resolves which Claude provider to talk to, and
builds the right SDK client for it. Used by run_demo.py, run_model_eval.py,
and the AirClaudeOperator so all three behave identically.

Three ways to reach Claude, same code path:

  CLAUDE_PROVIDER=direct   (default) -> anthropic.Anthropic()        — api.anthropic.com
  CLAUDE_PROVIDER=bedrock            -> anthropic.AnthropicBedrock() — AWS Bedrock
  CLAUDE_PROVIDER=vertex             -> anthropic.AnthropicVertex()  — Google Vertex AI

This is the same tool-calling agent loop pointed at the Claude Developer
Platform instead of NVIDIA NIM — the provider is a config value, not an
architecture decision.

Python 3.9 compatible.
"""

import os
from pathlib import Path

ROOT     = Path(__file__).parent
ENV_FILE = ROOT / ".env"

PLACEHOLDER = "your_anthropic_api_key_here"

# claude-sonnet-5 is the current general-purpose model as of this project's
# writing. Override with CLAUDE_MODEL in .env — claude-haiku-4-5-20251001 is a
# faster/cheaper option for the same tool-calling loop.
DEFAULT_MODEL = "claude-sonnet-5"

VALID_PROVIDERS = ("direct", "bedrock", "vertex")


def load_env() -> None:
    """Load .env into os.environ if python-dotenv is available. Never raises."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    if ENV_FILE.exists():
        load_dotenv(ENV_FILE)


def get_model() -> str:
    """Resolve the Claude model id — CLAUDE_MODEL wins, otherwise the default."""
    load_env()
    return (os.environ.get("CLAUDE_MODEL") or "").strip() or DEFAULT_MODEL


def get_provider() -> str:
    """Resolve which backend serves the Claude Developer Platform API."""
    load_env()
    provider = (os.environ.get("CLAUDE_PROVIDER") or "direct").strip().lower()
    if provider not in VALID_PROVIDERS:
        raise ValueError(
            f"CLAUDE_PROVIDER must be one of {VALID_PROVIDERS}, got '{provider}'."
        )
    return provider


def get_api_key(env_var: str = "ANTHROPIC_API_KEY"):
    """
    Return (api_key, error_message) for the direct-API provider.

    On success: (key, None). On failure: (None, human-readable reason).
    Bedrock and Vertex authenticate through their own SDKs (AWS/GCP credential
    chains) rather than this key, so callers only check this for
    CLAUDE_PROVIDER=direct.
    """
    load_env()
    key = (os.environ.get(env_var) or "").strip()

    if not key:
        return None, (
            f"{env_var} is not set.\n"
            f"  Fix: add it to {ENV_FILE} as {env_var}=sk-ant-...\n"
            f"  Or:  export {env_var}=sk-ant-...\n"
            f"  Get a key at https://console.anthropic.com/settings/keys"
        )

    if key == PLACEHOLDER or key.startswith("your_"):
        return None, (
            f"{env_var} is still the placeholder value from .env.example.\n"
            f"  Fix: open {ENV_FILE} and replace it with your real key.\n"
            f"  Get a key at https://console.anthropic.com/settings/keys"
        )

    if not key.startswith("sk-ant-"):
        return None, (
            f"{env_var} does not look like an Anthropic API key "
            f"(expected it to start with 'sk-ant-', got '{key[:7]}…').\n"
            f"  Check you copied the whole key from https://console.anthropic.com/settings/keys"
        )

    return key, None


def get_client_and_error(env_var: str = "ANTHROPIC_API_KEY"):
    """
    Return (client, error_message) — an SDK client wired to whichever provider
    CLAUDE_PROVIDER selects, or a human-readable reason it could not be built.

    Same client interface (`.messages.create(...)`) regardless of provider —
    that portability is the point of this file.
    """
    provider = get_provider()

    try:
        import anthropic
    except ImportError:
        return None, (
            "The 'anthropic' package is not installed.\n"
            "  Fix: pip3 install -r requirements.txt"
        )

    if provider == "direct":
        key, error = get_api_key(env_var)
        if error:
            return None, error
        return anthropic.Anthropic(api_key=key), None

    if provider == "bedrock":
        # Auth flows through the standard AWS credential chain (env vars,
        # ~/.aws/credentials, or an instance/role profile) — nothing Claude-
        # specific to configure beyond AWS_REGION.
        region = os.environ.get("AWS_REGION", "").strip()
        if not region:
            return None, (
                "CLAUDE_PROVIDER=bedrock requires AWS_REGION.\n"
                "  Fix: add AWS_REGION=us-east-1 (or your region) to .env, and "
                "make sure AWS credentials are configured (aws configure / "
                "AWS_ACCESS_KEY_ID+AWS_SECRET_ACCESS_KEY / an assumed role)."
            )
        try:
            return anthropic.AnthropicBedrock(aws_region=region), None
        except Exception as e:
            return None, f"Could not build AnthropicBedrock client: {e}"

    if provider == "vertex":
        # Auth flows through Application Default Credentials
        # (gcloud auth application-default login, or a service account key).
        project = os.environ.get("GOOGLE_CLOUD_PROJECT", "").strip()
        region  = os.environ.get("GOOGLE_CLOUD_REGION", "us-east5").strip()
        if not project:
            return None, (
                "CLAUDE_PROVIDER=vertex requires GOOGLE_CLOUD_PROJECT.\n"
                "  Fix: add GOOGLE_CLOUD_PROJECT=your-gcp-project to .env, and "
                "run `gcloud auth application-default login`."
            )
        try:
            return anthropic.AnthropicVertex(project_id=project, region=region), None
        except Exception as e:
            return None, f"Could not build AnthropicVertex client: {e}"

    return None, f"Unknown CLAUDE_PROVIDER: {provider}"


# ── Path resolution ────────────────────────────────────────────────────────────
# Explicit AIRCLAUDE_HOME wins, otherwise walk up looking for the repo layout,
# otherwise fall back to this file's directory.

def repo_root() -> Path:
    """Locate the AirClaude repo root regardless of how the caller was invoked."""
    explicit = os.environ.get("AIRCLAUDE_HOME")
    if explicit:
        candidate = Path(explicit).expanduser().resolve()
        if candidate.exists():
            return candidate

    for base in (Path(__file__).resolve().parent, Path.cwd().resolve()):
        for candidate in (base, *base.parents):
            if (candidate / "data").is_dir() and (candidate / "tools").is_dir():
                return candidate

    return ROOT.resolve()


def data_file(name: str) -> Path:
    """Absolute path to a file in the repo's data/ directory."""
    return repo_root() / "data" / name


def tools_dir() -> Path:
    """Absolute path to the repo's tools/ directory (for sys.path insertion)."""
    return repo_root() / "tools"
