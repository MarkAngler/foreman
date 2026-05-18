# foreman install

`python install/install.py` runs the full install. See it for the canonical step list.

## Prerequisites

1. **Authenticated `databricks` CLI** — OAuth or PAT both work, but OAuth is recommended:
   ```bash
   databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile DEFAULT
   ```
   The Python SDK and CLI both honor the `DEFAULT` profile in `~/.databrickscfg`. If you have stale `DATABRICKS_HOST` / `DATABRICKS_TOKEN` env vars set, unset them so the profile takes over.

2. **Vector Search endpoint type**: defaults to `STANDARD` — required for the `messages_idx` DIRECT_ACCESS index (storage-optimized endpoints don't support DIRECT_ACCESS). Override only if you don't need sub-1s message-recall freshness:
   ```bash
   FOREMAN_VS_ENDPOINT_TYPE=STORAGE_OPTIMIZED python install/install.py
   ```
   On STORAGE_OPTIMIZED, `messages_idx` creation fails and the dialectic agent's message-recall semantic search is disabled.

## What runs

| Step | Script | What it does |
|------|--------|--------------|
| 1 | `bootstrap.py` | Creates UC catalog + schema, Lakebase instance + logical DB, registers Lakebase as a UC database catalog, creates the Vector Search endpoint, secret scope, signing key, bootstrap admin token. |
| 2 | `preflight.py` | Defensive only: deletes the `foreman-<target>` app if it's stuck in an unhealthy state (ERROR / UNAVAILABLE / FAILED / STOPPED / DELETING) from a prior failed deploy. Healthy apps pass through. |
| 3 | `grant_race.py` | Runs `databricks bundle deploy` while concurrently watching for the app's auto-allocated SP UUID — creates the matching Postgres role in Lakebase the instant the SP exists, winning the race against terraform's grant attempt. |
| 4 | `databricks bundle run schema_init` | Alembic migrations, Delta tables, Delta→Lakebase synced tables (`documents_mirror`, `webhook_results_mirror`), VS indexes. |
| 5 | `databricks bundle run foreman_app` | Starts the FastAPI app. |

## Phase-3 cleanup status

The original cleanup list from the free-tier-style install session is now closed. Tracking here for posterity:

| Sev | Item | Status |
|---|---|---|
| 🔴 | `messages_idx` DIRECT_ACCESS skipped | ✅ Resolved — VS endpoint defaults to STANDARD (which supports DIRECT_ACCESS; the original cleanup note had the tier relationship inverted); schema_init no longer wraps `ensure_messages_index` in best-effort. |
| 🔴 | App quota of 1 on workspace | ✅ Resolved — preflight cleanup is now defensive (only deletes unhealthy apps). |
| 🟡 | `messages_delta` Lakebase→Delta sync | ✅ Obsolete — the synced table was vestigial (no job or app read from it); deriver/summarizer/dreamer all read Lakebase via JDBC. Removed. |
| 🟡 | Default Storage catalog UI prereq | ✅ Resolved — bootstrap.py's `w.catalogs.create(...)` works without manual UI on enterprise. |
| 🟡 | Lakebase UC registration | ✅ Resolved — folded into `bootstrap.py:ensure_lakebase_uc_catalog`. |
| 🟡 | Apps↔Lakebase SP role race | Kept — platform race, not a tier issue. `grant_race.py` handles it. |
| 🟢 | `sys.path.insert` / `__file__` fallback in 4 job mains | ✅ Resolved — `databricks.yml` builds the `foreman` wheel via the `artifacts.foreman_wheel` block; jobs install it as a normal dependency, so `from foreman.lib import ...` resolves natively. |
| 🟢 | `sys.exit(0)` reported as failure | Kept — schema_init's entry guard still only raises on non-zero rc; applies to any new `spark_python_task` entry point. |
| 🟢 | `client: "2"` hardcoded | ✅ Resolved — exposed as `${var.serverless_client_version}` (default `"2"`). |
| 🟢 | Typed SDK class migration | ✅ Done. |

Webhook results integration (PHASE3.md step 5a/5b) — both done. `schema_init` now creates `webhook_results_mirror` as a Delta→Lakebase synced table, and the FastAPI app's `webhook_reconciler` reads from it to mark `queue_items.processed`.

## Operational runbooks

### A. Upgrading to STORAGE_OPTIMIZED VS endpoint (larger Delta-Sync workloads)

STORAGE_OPTIMIZED endpoints don't support DIRECT_ACCESS indexes — switching to one disables `messages_idx`. Only do this if you don't need real-time message-recall semantic search and you want higher Delta-Sync capacity:

```bash
databricks vector-search-endpoints delete-endpoint foreman-vs
FOREMAN_VS_ENDPOINT_TYPE=STORAGE_OPTIMIZED python install/bootstrap.py
# Either accept messages_idx will fail in schema_init, or refactor it to DELTA_SYNC.
```

### B. Multi-target deploys (dev + prod side-by-side)

```bash
databricks bundle deploy --target prod
databricks bundle run --target prod schema_init
databricks bundle run --target prod foreman_app
```

Preflight is target-scoped (operates on `foreman-<target>`), so deploying `prod` won't disturb `dev`. The `prod` target overrides `catalog: foreman_prod` so namespaces are isolated.

### C. Re-running `schema_init` when UC discovery lagged

The Delta→Lakebase synced tables (`documents_mirror`, `webhook_results_mirror`) depend on the source Delta tables being visible in UC, which is async (minute-scale). If schema_init logs `WARNING: skipping ... sync`, wait ~2 min and re-run:

```bash
databricks bundle run schema_init
```

Re-runs short-circuit cleanly on already-existing resources.

## Troubleshooting

### `failed to resolve host 'ep-*.database.*.azuredatabricks.net'`
Your install host can't resolve the Lakebase private-link hostname. The instance is provisioned correctly but `ensure_logical_database` connects to it via Postgres protocol, which requires DNS. Run install from a host on the corporate network (or via VPN). WSL2 inherits Windows DNS but Azure Private Link CNAMEs may not resolve from inside WSL even when they do from Windows — try running `install.py` from a Windows PowerShell against the same venv, or use a VM joined to the corp domain.

### `Metastore storage root URL does not exist. Default Storage is enabled`
On Azure workspaces with Default Storage enabled but no metastore storage root, bootstrap auto-discovers a workspace-managed external location (looks for one containing `unity-catalog` and the workspace id) and creates the catalog with that as its MANAGED LOCATION. If discovery fails, set `FOREMAN_CATALOG_STORAGE_ROOT=abfss://...` to a location the workspace has UC permissions on and re-run install.

### `Invalid access token` from bootstrap
Your env-var PAT (`DATABRICKS_TOKEN`) is expired/revoked. Either rotate it or `unset` it and `databricks auth login --profile DEFAULT` to use OAuth.

### `STORAGE_OPTIMIZED endpoint type is not supported`
Your account is on a tier that doesn't support storage-optimized VS endpoints. Run `FOREMAN_VS_ENDPOINT_TYPE=STANDARD python install/install.py`.

### `Metastore storage root URL does not exist`
Your workspace metastore lacks a storage root. Ask an account admin to assign one, or pre-create the `foreman` catalog via the UI with `Storage Location: Default Storage`, then re-run install.

### App stuck in ERROR with `Role <UUID> not found in instance foreman-lakebase`
The grant race lost. Workaround:
1. `databricks apps delete foreman-<target>` (frees the slot).
2. Wait ~2 min for `DELETING` to complete: `databricks apps list`.
3. Re-run `python install/install.py` — preflight will detect the unhealthy state and clean up, then the deploy re-runs.

If the race loses repeatedly, you can manually pre-create the role after the app appears in the Apps API:
```bash
databricks database create-database-instance-role foreman-lakebase <SP_UUID> --identity-type SERVICE_PRINCIPAL
```
where `<SP_UUID>` is the `service_principal_client_id` from `databricks apps get foreman-<target> --output json`.

### `InvalidParameterValue: Instance name is not unique` (re-installing after deletion)
Lakebase reserves deleted instance names for ~7 days (the default `retention_window_in_days`). Even after `delete-database-instance` returns success and `get-database-instance` 404s, `create-database-instance` with the same name fails. Two workarounds:

1. **Wait it out** — re-run install after ~7 days from the original deletion.
2. **Use a different name for this install only** — keeps the repo defaults intact for other customers:
   ```bash
   FOREMAN_LAKEBASE_INSTANCE=foreman-db \
   BUNDLE_VAR_lakebase_instance_name=foreman-db \
   python install/install.py
   ```
   Both env vars are required: `FOREMAN_LAKEBASE_INSTANCE` is read by `install/bootstrap.py` + `install/grant_race.py`; `BUNDLE_VAR_lakebase_instance_name` overrides the DAB variable referenced by `resources/app.yml` and `resources/jobs.yml` at deploy time. Once the original name unlocks, delete the alias instance and re-run install with no env vars to restore the canonical name.

### `NotFound: Custom tags are not supported in Vector Search`
Your workspace's Vector Search tier doesn't support the `update_endpoint_custom_tags` API. `install/bootstrap.py:_ensure_vs_project_tag` catches this case and logs `custom tags unsupported on this VS tier — skipping`. The install continues — only the `project=foreman` billing tag on the VS endpoint is skipped (Lakebase and jobs still receive the tag normally).

## Variables

All defaults match the bundle's `databricks.yml`. Override via env vars before running `install.py`:

| Env var | Default |
|---------|---------|
| `FOREMAN_CATALOG` | `foreman` |
| `FOREMAN_SCHEMA` | `memory` |
| `FOREMAN_LAKEBASE_INSTANCE` | `foreman-lakebase` |
| `FOREMAN_LAKEBASE_DATABASE` | `memory` |
| `FOREMAN_LAKEBASE_CATALOG_NAME` | `foreman_lakebase` |
| `FOREMAN_VS_ENDPOINT` | `foreman-vs` |
| `FOREMAN_VS_ENDPOINT_TYPE` | `STANDARD` |
| `FOREMAN_JWT_SECRET_SCOPE` | `foreman` |
| `FOREMAN_LAKEBASE_CAPACITY` | `CU_1` |
| `FOREMAN_CATALOG_STORAGE_ROOT` | _(auto-discovered)_ — explicit `abfss://` / `s3://` / `gs://` URL to use as the catalog's MANAGED LOCATION when the metastore has no storage root. |
