# foreman MCP server

A stdio [Model Context Protocol](https://modelcontextprotocol.io) server that exposes the foreman REST API as tools for Claude Code and other MCP clients. Runs on the client machine and talks to your deployed foreman over HTTPS.

## Prerequisites

- A deployed foreman bundle (an app named `foreman-<target>`, default target `dev`). See `../../../README.md` for install.
- Python 3.11+ and the foreman venv with deps installed (`pip install -r requirements.txt`).
- The Databricks CLI authenticated to the workspace hosting foreman. The simplest path:

  ```bash
  databricks auth login --profile DEFAULT
  ```

  If you've never used `databricks-sdk` on this machine, this triggers an OAuth U2M browser flow on first use.

## Add to Claude Code

The repo's [.mcp.json](../../../.mcp.json) already wires the server. Launch Claude Code from the project root; the `foreman` MCP server appears in `/mcp` output. On first tool call the credential resolver runs:

1. Resolves the deployed app URL via `databricks-sdk` (`apps.get foreman-<target>`).
2. Reads the foreman JWT signing key from secret scope `foreman/jwt_signing_key`.
3. Mints a short-TTL admin JWT in-process per request.

No manual token copying. No `.env` file required.

## Tools (10)

| Tool | Purpose |
|---|---|
| `foreman_chat` | Invoke the dialectic agent for a peer; returns buffered SSE result with full text. |
| `foreman_send_messages` | Append a batch of messages to a session (embedded + indexed inline). |
| `foreman_search` | Hybrid semantic + lexical search over messages in a workspace. |
| `foreman_session_context` | Prompt-ready window: recent messages + optional rolling summary. |
| `foreman_list_workspaces` / `foreman_create_workspace` | Workspace lifecycle. |
| `foreman_list_peers` / `foreman_create_peer` | Peer lifecycle (scoped to a workspace). |
| `foreman_list_sessions` / `foreman_create_session` | Session lifecycle. |

## Environment variables (all optional)

| Var | Default | Purpose |
|---|---|---|
| `FOREMAN_BASE_URL` | resolved via SDK | Override the deployed app URL. |
| `FOREMAN_TOKEN` | minted per-request | Use a static bearer token (bypasses signing-key lookup). Useful for CI. |
| `FOREMAN_TARGET` | `dev` | Bundle target used to resolve the app name (`foreman-<target>`). |
| `FOREMAN_DEFAULT_WORKSPACE` | unset | Workspace name tools fall back to when `workspace_name` is omitted. |
| `FOREMAN_SECRET_SCOPE` | `foreman` | Override secret scope name. |
| `FOREMAN_JWT_SECRET_KEY` | `jwt_signing_key` | Override secret key holding the signing key. |

## Trust model

The default credential path requires the caller to have read access to the `foreman` secret scope on the Databricks workspace. Anyone with that access can mint admin JWTs and gain full foreman access — the **same** trust boundary as the FastAPI app itself. The MCP server adds no new attack surface beyond what the existing secret scope grant exposes.

Per-Databricks-user → per-foreman-workspace binding (so different users get different scoped JWTs) is a planned follow-up; it requires both a user-mapping table in foreman and a `/auth/exchange` endpoint that reads Databricks Apps proxy auth headers.

## Standalone usage

You can run the server outside Claude Code (e.g. with [`mcp dev`](https://github.com/modelcontextprotocol/python-sdk#development-tools)):

```bash
PYTHONPATH=src .venv/bin/python -m foreman.mcp                # stdio
PYTHONPATH=src .venv/bin/python -m foreman.mcp --list-tools   # smoke test
```

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `CredentialsError: could not initialize WorkspaceClient` | No Databricks auth configured. Run `databricks auth login` or set `FOREMAN_TOKEN` + `FOREMAN_BASE_URL`. |
| `CredentialsError: could not fetch Databricks app` | `foreman-<target>` is not deployed in the configured workspace. Run `databricks bundle deploy` or set `FOREMAN_TARGET`. |
| `CredentialsError: could not read secret foreman/jwt_signing_key` | Your Databricks user lacks `READ` on the foreman secret scope. Either grant access or set `FOREMAN_TOKEN`. |
| `foreman ... returned 401: invalid token` | Static `FOREMAN_TOKEN` is expired or signed with the wrong key. Drop it to fall back to in-process minting. |
| `foreman ... returned 403: token not scoped to this workspace` | `FOREMAN_TOKEN` is workspace-scoped to a different workspace than the tool call targets. |
