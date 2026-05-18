"""
Pre-deploy bootstrap for foreman.

Creates infrastructure that DABs cannot declare:
  - Unity Catalog catalog + schema
  - Lakebase Provisioned instance and logical database
  - Lakebase registration as a UC database catalog (binds Apps -> Lakebase)
  - Vector Search endpoint
  - Secret scope, JWT signing key, bootstrap admin token

Idempotent: re-running is a no-op when resources already exist.

Usage:
    databricks auth login --profile DEFAULT
    python install/bootstrap.py
"""

from __future__ import annotations

import os
import secrets as _secrets
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.database import (
    CustomTag,
    DatabaseCatalog,
    DatabaseInstance,
    DatabaseInstanceState,
)

CATALOG = os.environ.get("FOREMAN_CATALOG", "foreman")
SCHEMA = os.environ.get("FOREMAN_SCHEMA", "memory")
LAKEBASE_INSTANCE = os.environ.get("FOREMAN_LAKEBASE_INSTANCE", "foreman-lakebase")
LAKEBASE_DATABASE = os.environ.get("FOREMAN_LAKEBASE_DATABASE", "memory")
LAKEBASE_CATALOG_NAME = os.environ.get("FOREMAN_LAKEBASE_CATALOG_NAME", "foreman_lakebase")
VS_ENDPOINT = os.environ.get("FOREMAN_VS_ENDPOINT", "foreman-vs")
SECRET_SCOPE = os.environ.get("FOREMAN_JWT_SECRET_SCOPE", "foreman")
LAKEBASE_CAPACITY = os.environ.get("FOREMAN_LAKEBASE_CAPACITY", "CU_1")
VS_ENDPOINT_TYPE = os.environ.get("FOREMAN_VS_ENDPOINT_TYPE", "STANDARD")

PROJECT_TAG_KEY = "project"
PROJECT_TAG_VALUE = "foreman"


def info(msg: str) -> None:
    print(f"[bootstrap] {msg}", flush=True)


def _discover_storage_root(w: WorkspaceClient, catalog_name: str) -> str | None:
    """Return a workspace-managed storage URL the catalog can use as its MANAGED LOCATION.

    On workspaces where the metastore has no storage root and Default Storage is
    enabled, `w.catalogs.create()` without an explicit `storage_root` fails. The
    workaround the UI uses is to point at one of the workspace's pre-registered
    Unity Catalog managed external locations. We prefer the one that contains
    the workspace id (workspace-specific managed storage); else fall back to the
    metastore-default managed location.
    """
    explicit = os.environ.get("FOREMAN_CATALOG_STORAGE_ROOT")
    if explicit:
        return explicit.rstrip("/") + f"/{catalog_name}"

    workspace_id = str(w.config.workspace_id or "")
    managed = []
    workspace_specific = []
    for el in w.external_locations.list():
        url = (el.url or "").rstrip("/")
        if not url:
            continue
        if "unity-catalog" in url or "unitycatalog" in url:
            managed.append(url)
            if workspace_id and workspace_id in url:
                workspace_specific.append(url)

    chosen = workspace_specific[0] if workspace_specific else (managed[0] if managed else None)
    if chosen is None:
        return None
    return f"{chosen}/{catalog_name}"


def ensure_catalog(w: WorkspaceClient, name: str) -> None:
    try:
        w.catalogs.get(name)
        info(f"catalog '{name}' already exists")
        return
    except Exception:
        pass

    info(f"creating catalog '{name}'")
    try:
        w.catalogs.create(name=name, comment="foreman (honcho rebuild) — managed by install/bootstrap.py")
        return
    except Exception as e:
        if "Metastore storage root URL does not exist" not in str(e):
            raise
        info("  metastore lacks a storage root; discovering a workspace-managed location to use as MANAGED LOCATION")

    storage_root = _discover_storage_root(w, name)
    if storage_root is None:
        raise RuntimeError(
            f"could not auto-discover a managed storage location for catalog '{name}'. "
            "Set FOREMAN_CATALOG_STORAGE_ROOT to an abfss://, s3://, or gs:// URL the workspace "
            "has UC permissions on, or create the catalog manually via the UI."
        )

    info(f"  using storage_root={storage_root}")
    w.catalogs.create(
        name=name,
        comment="foreman (honcho rebuild) — managed by install/bootstrap.py",
        storage_root=storage_root,
    )


def ensure_schema(w: WorkspaceClient, catalog: str, schema: str) -> None:
    full = f"{catalog}.{schema}"
    try:
        w.schemas.get(full_name=full)
        info(f"schema '{full}' already exists")
    except Exception:
        info(f"creating schema '{full}'")
        w.schemas.create(name=schema, catalog_name=catalog, comment="foreman OLTP-mirror Delta tables")


def ensure_lakebase(w: WorkspaceClient, name: str, capacity: str) -> DatabaseInstance:
    project_tag = CustomTag(key=PROJECT_TAG_KEY, value=PROJECT_TAG_VALUE)
    try:
        existing = w.database.get_database_instance(name=name)
        info(f"lakebase '{name}' already exists (state={existing.state})")
        instance = existing
    except Exception:
        info(f"creating lakebase '{name}' (capacity={capacity}) — this can take ~5 minutes")
        instance = w.database.create_database_instance(
            DatabaseInstance(
                name=name,
                capacity=capacity,
                stopped=False,
                custom_tags=[project_tag],
            )
        )

    # Wait for the instance to be AVAILABLE
    deadline = time.time() + 1200
    while time.time() < deadline:
        instance = w.database.get_database_instance(name=name)
        if instance.state == DatabaseInstanceState.AVAILABLE:
            info(f"lakebase '{name}' is AVAILABLE at {instance.read_write_dns}")
            _ensure_lakebase_project_tag(w, instance, project_tag)
            return instance
        info(f"  lakebase state={instance.state}, sleeping 15s")
        time.sleep(15)
    raise RuntimeError(f"lakebase '{name}' did not become AVAILABLE within 20m")


def _ensure_lakebase_project_tag(
    w: WorkspaceClient, instance: DatabaseInstance, project_tag: CustomTag
) -> None:
    current = list(instance.custom_tags or [])
    if any(t.key == project_tag.key and t.value == project_tag.value for t in current):
        return
    merged = [t for t in current if t.key != project_tag.key] + [project_tag]
    info(f"  applying {project_tag.key}={project_tag.value} tag to lakebase '{instance.name}'")
    w.database.update_database_instance(
        name=instance.name,
        database_instance=DatabaseInstance(name=instance.name, custom_tags=merged),
        update_mask="custom_tags",
    )


def ensure_lakebase_uc_catalog(
    w: WorkspaceClient, name: str, instance: str, database: str
) -> None:
    try:
        w.database.get_database_catalog(name=name)
        info(f"UC database catalog '{name}' already exists (-> {instance}/{database})")
        return
    except Exception as e:
        if not ("does not exist" in str(e) or "NOT_FOUND" in str(e) or isinstance(e, NotFound)):
            raise

    info(f"registering Lakebase as UC database catalog '{name}' (-> {instance}/{database})")
    w.database.create_database_catalog(
        DatabaseCatalog(
            name=name,
            database_instance_name=instance,
            database_name=database,
            create_database_if_not_exists=True,
        )
    )


def ensure_vector_search_endpoint(w: WorkspaceClient, name: str, endpoint_type: str) -> None:
    from databricks.sdk.service.vectorsearch import CustomTag as VsCustomTag
    from databricks.sdk.service.vectorsearch import EndpointType

    try:
        ep = w.vector_search_endpoints.get_endpoint(endpoint_name=name)
        info(f"vector search endpoint '{name}' already exists (state={ep.endpoint_status.state if ep.endpoint_status else 'UNKNOWN'})")
    except Exception:
        info(f"creating vector search endpoint '{name}' (type={endpoint_type}) — this can take ~10 minutes")
        w.vector_search_endpoints.create_endpoint(
            name=name,
            endpoint_type=EndpointType[endpoint_type],
        )

    # Wait for endpoint to be ONLINE
    deadline = time.time() + 1800
    while time.time() < deadline:
        ep = w.vector_search_endpoints.get_endpoint(endpoint_name=name)
        state = ep.endpoint_status.state if ep.endpoint_status else None
        if state and "ONLINE" in str(state):
            info(f"vector search endpoint '{name}' is ONLINE")
            _ensure_vs_project_tag(w, name, ep, VsCustomTag)
            return
        info(f"  vs endpoint state={state}, sleeping 30s")
        time.sleep(30)
    raise RuntimeError(f"vector search endpoint '{name}' did not become ONLINE within 30m")


def _ensure_vs_project_tag(w: WorkspaceClient, name: str, ep, tag_cls) -> None:
    current = list(ep.custom_tags or [])
    if any(t.key == PROJECT_TAG_KEY and t.value == PROJECT_TAG_VALUE for t in current):
        return
    merged = [t for t in current if t.key != PROJECT_TAG_KEY] + [
        tag_cls(key=PROJECT_TAG_KEY, value=PROJECT_TAG_VALUE)
    ]
    info(f"  applying {PROJECT_TAG_KEY}={PROJECT_TAG_VALUE} tag to vs endpoint '{name}'")
    try:
        w.vector_search_endpoints.update_endpoint_custom_tags(
            endpoint_name=name, custom_tags=merged
        )
    except NotFound as e:
        if "Custom tags are not supported" in str(e):
            info(f"  custom tags unsupported on this VS tier — skipping")
            return
        raise


def ensure_secret_scope(w: WorkspaceClient, scope: str) -> None:
    scopes = {s.name for s in w.secrets.list_scopes()}
    if scope in scopes:
        info(f"secret scope '{scope}' already exists")
        return
    info(f"creating secret scope '{scope}'")
    w.secrets.create_scope(scope=scope)


def ensure_secret(w: WorkspaceClient, scope: str, key: str, value: str) -> None:
    """Create a secret only if it doesn't already exist (preserves existing values)."""
    try:
        existing = list(w.secrets.list_secrets(scope=scope))
        if any(s.key == key for s in existing):
            info(f"secret {scope}/{key} already exists — leaving value untouched")
            return
    except Exception:
        pass
    info(f"writing secret {scope}/{key}")
    w.secrets.put_secret(scope=scope, key=key, string_value=value)


def main() -> int:
    w = WorkspaceClient()
    info(f"connected to {w.config.host} as {w.current_user.me().user_name}")

    ensure_catalog(w, CATALOG)
    ensure_schema(w, CATALOG, SCHEMA)

    ensure_lakebase(w, LAKEBASE_INSTANCE, LAKEBASE_CAPACITY)
    # UC catalog registration creates the logical Postgres database as a side
    # effect (create_database_if_not_exists=True) — no need to connect directly
    # via psycopg, which works around Lakebase being private-link-only.
    ensure_lakebase_uc_catalog(w, LAKEBASE_CATALOG_NAME, LAKEBASE_INSTANCE, LAKEBASE_DATABASE)

    ensure_vector_search_endpoint(w, VS_ENDPOINT, VS_ENDPOINT_TYPE)

    ensure_secret_scope(w, SECRET_SCOPE)
    # JWT signing key — 64 random bytes, base64-ish. Generated once, reused forever.
    ensure_secret(w, SECRET_SCOPE, "jwt_signing_key", _secrets.token_urlsafe(48))
    # Bootstrap admin token — surfaced via app/keys endpoint at first request.
    ensure_secret(w, SECRET_SCOPE, "bootstrap_admin_token", _secrets.token_urlsafe(32))

    info("bootstrap complete. next: `databricks bundle deploy && databricks bundle run schema_init`")
    return 0


if __name__ == "__main__":
    sys.exit(main())
