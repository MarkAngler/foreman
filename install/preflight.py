"""Pre-deploy defensive checks.

Runs after install/bootstrap.py but before `databricks bundle deploy`. On a
healthy workspace this is a no-op; its job is to recover from partial-deploy
carnage left by a previous failed run.

  - If the `foreman-<target>` app exists in an unhealthy state (ERROR /
    UNAVAILABLE / FAILED / STOPPED / DELETING), delete it so the next bundle
    deploy starts from a clean slot. Healthy apps pass through.

Idempotent.
"""

from __future__ import annotations

import argparse
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

HEALTHY_STATES = {"ACTIVE", "RUNNING", "STARTING"}
UNHEALTHY_STATES = {"ERROR", "UNAVAILABLE", "FAILED", "STOPPED", "DELETING"}


def info(msg: str) -> None:
    print(f"[preflight] {msg}", flush=True)


def delete_orphan_app(w: WorkspaceClient, app_name: str) -> None:
    try:
        app = w.apps.get(name=app_name)
    except Exception as e:
        if "does not exist" in str(e) or "is deleted" in str(e) or isinstance(e, NotFound):
            info(f"no app '{app_name}' present — clean slate")
            return
        raise

    state = str(app.compute_status.state) if app.compute_status else "UNKNOWN"
    # state may render as "ComputeState.ACTIVE" — match on the suffix.
    state_token = state.rsplit(".", 1)[-1].upper()

    if state_token in HEALTHY_STATES:
        info(f"app '{app_name}' is healthy (state={state_token}) — leaving in place")
        return

    if state_token not in UNHEALTHY_STATES:
        info(f"app '{app_name}' in unknown state={state_token} — leaving in place")
        return

    info(f"app '{app_name}' is unhealthy (state={state_token}), deleting to allow fresh deploy")
    try:
        w.apps.delete(name=app_name)
    except Exception as e:
        info(f"delete returned: {e} — continuing")

    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            w.apps.get(name=app_name)
            info(f"  waiting for '{app_name}' deletion to complete (15s)")
            time.sleep(15)
        except Exception:
            info(f"'{app_name}' fully deleted")
            return
    raise RuntimeError(f"orphan app '{app_name}' did not delete within 5 minutes")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="dev", help="bundle target (default: dev)")
    args = parser.parse_args()

    app_name = f"foreman-{args.target}"

    w = WorkspaceClient()
    info(f"connected to {w.config.host} as {w.current_user.me().user_name}")

    delete_orphan_app(w, app_name)

    info("preflight complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
