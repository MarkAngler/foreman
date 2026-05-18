"""Bundle deploy with concurrent Lakebase role creation.

The Databricks Apps + Lakebase binding (resources/app.yml → database resource
with `permission: CAN_CONNECT_AND_CREATE`) requires a Postgres role in the
Lakebase whose name matches the app's auto-allocated service principal UUID.
The Databricks platform creates this role lazily — empirically several seconds
to minutes after the app is created — but terraform's grant retry budget is
shorter. The grant fails with `Role <SP UUID> not found`, the app enters ERROR
state, and `databricks bundle deploy` fails.

This script wins the race by running the bundle deploy concurrently with a
watcher that:
  1. Polls the Apps API at 100ms intervals for the foreman-<target> app.
  2. Captures the service_principal_client_id the instant it's populated.
  3. Creates the matching Postgres role in foreman-lakebase via the SDK.

By the time the platform's grant attempt runs, the role exists and the grant
succeeds. If a prior attempt already created the role, ResourceConflict on
create is logged and ignored (idempotent).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceConflict
from databricks.sdk.service.database import (
    DatabaseInstanceRole,
    DatabaseInstanceRoleIdentityType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def info(msg: str) -> None:
    print(f"[grant_race] {msg}", flush=True)


def watcher(
    w: WorkspaceClient,
    app_name: str,
    lakebase_instance: str,
    stop: threading.Event,
    poll_interval: float = 0.1,
    timeout: float = 600.0,
) -> None:
    info(f"watching '{app_name}' for SP allocation (poll every {int(poll_interval * 1000)}ms)")
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline and not stop.is_set():
        try:
            app = w.apps.get(name=app_name)
            sp = app.service_principal_client_id
            if sp:
                info(f"SP detected: {sp}")
                try:
                    w.database.create_database_instance_role(
                        instance_name=lakebase_instance,
                        database_instance_role=DatabaseInstanceRole(
                            name=sp,
                            identity_type=DatabaseInstanceRoleIdentityType.SERVICE_PRINCIPAL,
                        ),
                    )
                    info(f"role '{sp}' created in '{lakebase_instance}'")
                except ResourceConflict:
                    info(f"role '{sp}' already exists in '{lakebase_instance}' (OK)")
                return
        except Exception as e:
            msg = str(e)
            if "does not exist" not in msg and "is deleted" not in msg and "NOT_FOUND" not in msg:
                info(f"poll error (will retry): {e}")
        time.sleep(poll_interval)

    info("watcher exiting (timeout or deploy already finished without SP detection)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="dev")
    parser.add_argument(
        "--lakebase-instance",
        default=os.environ.get("FOREMAN_LAKEBASE_INSTANCE", "foreman-lakebase"),
    )
    args = parser.parse_args()

    app_name = f"foreman-{args.target}"
    w = WorkspaceClient()
    info(f"connected to {w.config.host}")
    info(f"target app: {app_name}, lakebase: {args.lakebase_instance}")

    stop = threading.Event()
    t = threading.Thread(
        target=watcher,
        args=(w, app_name, args.lakebase_instance, stop),
        daemon=True,
    )
    t.start()
    time.sleep(0.3)

    info("starting `databricks bundle deploy`")
    proc = subprocess.run(["databricks", "bundle", "deploy"], cwd=str(PROJECT_ROOT))

    stop.set()
    t.join(timeout=2.0)

    info(f"bundle deploy exited with code {proc.returncode}")
    return proc.returncode


if __name__ == "__main__":
    sys.exit(main())
