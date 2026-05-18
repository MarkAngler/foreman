"""Job-entry bootstrap.

Serverless Spark python tasks have no DAB-level env-vars field; the only way
to flow per-target config into the process is via spark_python_task.parameters
(positional CLI args). Each job calls apply_env_overrides() before importing
foreman.lib so Pydantic Settings picks the values up via env_prefix=FOREMAN_.
"""

from __future__ import annotations

import os
import sys


def apply_env_overrides(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    for arg in args:
        if "=" not in arg or arg.startswith("-"):
            continue
        key, value = arg.split("=", 1)
        os.environ.setdefault(key, value)
