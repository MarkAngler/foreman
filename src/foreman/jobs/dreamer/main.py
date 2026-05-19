"""Dreamer worker stub. Phase-2 agent D will implement."""

from __future__ import annotations

import sys

from foreman.jobs._bootstrap import apply_env_overrides

apply_env_overrides()


def main() -> int:
    print("[dreamer] stub heartbeat — no-op", flush=True)
    return 0


if __name__ == "__main__":
    # Databricks spark_python_task exec wrapper treats SystemExit (even with
    # code 0) as task failure. Only raise on non-zero.
    _rc = main()
    if _rc != 0:
        sys.exit(_rc)
