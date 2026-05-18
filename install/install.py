"""End-to-end install orchestrator for foreman.

Runs each install step in order and prints clear progress. Idempotent — safe to
re-run after partial failures.

Steps:
  1. bootstrap.py     — UC catalog, schema, Lakebase, UC database catalog
                        registration, Vector Search endpoint, secret scope.
  2. preflight.py     — Defensive cleanup of any unhealthy `foreman-<target>`
                        app left over from a prior failed deploy.
  3. grant_race.py    — `databricks bundle deploy` racing Lakebase role creation.
  4. bundle run schema_init — Alembic migrations + Delta tables + synced tables
                              + Vector Search indexes.
  5. bundle run foreman_app — start the FastAPI app.

Prerequisites — see install/README.md:
  - Authenticated `databricks` CLI (`databricks auth login --profile DEFAULT`).
  - An enterprise workspace with: Vector Search STORAGE_OPTIMIZED endpoints,
    app quota > 1, metastore storage root. Smaller workspaces can downgrade
    the VS endpoint via `FOREMAN_VS_ENDPOINT_TYPE=STANDARD`.

Usage:
    databricks auth login --profile DEFAULT
    python install/install.py
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def info(msg: str) -> None:
    print(f"\n=== {msg} ===\n", flush=True)


def run(cmd: list[str], **kwargs) -> None:
    print(f"$ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True, cwd=str(PROJECT_ROOT), **kwargs)


def ensure_build_tooling() -> None:
    """Install `build` if missing — needed to produce the foreman wheel."""
    try:
        import build  # noqa: F401
        return
    except ImportError:
        pass
    info("installing `build` (needed to produce the foreman wheel artifact)")
    run([sys.executable, "-m", "pip", "install", "--quiet", "build", "wheel"])


def build_wheel() -> None:
    """Produce dist/foreman-*.whl. DAB's `artifacts.foreman_wheel` block packages
    whatever wheel it finds in ./dist/ — building it here (via the running venv
    interpreter) sidesteps cross-platform shell PATH issues with DAB's own
    `build:` command."""
    info("building foreman wheel")
    # Clean stale wheels so DAB doesn't accidentally upload an old build.
    dist = PROJECT_ROOT / "dist"
    if dist.exists():
        for old in dist.glob("foreman-*.whl"):
            old.unlink()
    run([sys.executable, "-m", "build", "--wheel"])


def main() -> int:
    if shutil.which("databricks") is None:
        print(
            "ERROR: databricks CLI not found on PATH. Install: "
            "https://docs.databricks.com/dev-tools/cli/install.html",
            file=sys.stderr,
        )
        return 1

    # Force UTF-8 in child Python processes so Unicode in log lines doesn't blow
    # up the default cp1252 stdout on Windows.
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

    ensure_build_tooling()
    build_wheel()

    info("Step 1/5 — bootstrap (UC catalog, Lakebase, UC db catalog, Vector Search, secrets)")
    run([sys.executable, "install/bootstrap.py"])

    info("Step 2/5 — preflight (defensive cleanup of unhealthy app)")
    run([sys.executable, "install/preflight.py"])

    info("Step 3/5 — bundle deploy with grant race (UC schema, app, jobs, wheel)")
    run([sys.executable, "install/grant_race.py"])

    info("Step 4/5 — bundle run schema_init (migrations + Delta tables + VS indexes)")
    run(["databricks", "bundle", "run", "schema_init"])

    info("Step 5/5 — bundle run foreman_app (start the app)")
    run(["databricks", "bundle", "run", "foreman_app"])

    print("\n*** install complete ***")
    print("Next: retrieve the bootstrap admin JWT from the foreman secret scope:")
    print("  databricks secrets get-secret --scope foreman --key bootstrap_admin_token")
    return 0


if __name__ == "__main__":
    sys.exit(main())
