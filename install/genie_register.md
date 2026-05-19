# Registering Foreman with a Databricks Genie / Agent Bricks Supervisor

This runbook stands up the second Foreman app — `mcp-foreman-${target}` — and
wires it into an Agent Bricks Supervisor (MAS) so the agent answering a human
in a Genie room can retrieve memories *about that specific human only*.

## Prerequisites

- The main Foreman bundle is already deployed (Lakebase + Vector Search +
  schema + `foreman-${target}` app are healthy).
- You know the Foreman workspace name you want the Genie surface bound to.
  All retrieval will be scoped to this single workspace. Peers are auto-created
  inside it from each chatter's Databricks email on first contact.

## 1. Set the bound workspace

The `mcp-foreman-${target}` app refuses to start unless `FOREMAN_MCP_WORKSPACE`
is set. Pick one of:

- Add a `*.local.yml` override at the bundle root (it is git-ignored and
  already included by `databricks.yml`):

  ```yaml
  resources:
    apps:
      mcp_foreman_app:
        config:
          env:
            - name: FOREMAN_MCP_WORKSPACE
              value: <your-foreman-workspace-name>
  ```

- Or, set the env var via the Databricks Apps UI after the first deploy.

## 2. Deploy the bundle

```bash
databricks bundle validate
databricks bundle deploy
```

The new app `mcp-foreman-${target}` appears alongside `foreman-${target}`.

## 3. Enable On-Behalf-Of (OBO) authorization for the app

OBO is **not declarable in the bundle YAML** at this time — enable it via the
Databricks Apps UI for `mcp-foreman-${target}`:

1. Apps → `mcp-foreman-${target}` → **Authorization** → toggle on user
   authorization.
2. Grant the minimal scope needed for SCIM `/Me` lookups (the resolver only
   needs to know who the caller is).
3. **Stop and start the app** (not redeploy). The
   `x-forwarded-access-token` header is only injected after a full cold start.

## 4. Smoke-test the MCP

From a shell authenticated to the same Databricks workspace:

```bash
# Replace <app-url> with the URL shown in the Apps UI for mcp-foreman-${target}
curl -H "Authorization: Bearer $(databricks auth token | jq -r .access_token)" \
     -H "Content-Type: application/json" \
     -X POST <app-url>/mcp \
     -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

Expect exactly three tools: `foreman_recall_about_me`, `foreman_peer_card`,
`foreman_recent_sessions`. If any other tool appears, do not register the MCP
— the peer-isolation guarantee depends on the surface being read-only and
self-referential.

## 5. Register as a tool in your Agent Bricks Supervisor

In the Supervisor (MAS) where your AI/BI Genie Space already lives as a tool:

1. Add a new tool of type **external MCP**.
2. URL: the `mcp-foreman-${target}` app URL from step 2.
3. Auth mode: **on-behalf-of user** — this is what propagates the chatter's
   identity to Foreman. If your supervisor lacks this option, do not proceed:
   without OBO propagation the per-user invariant will not hold.
4. Tool description (optional, helps the router): "Retrieve memories Foreman
   has stored *about the user currently chatting*: recent recall, synthesized
   facts (peer card), and recent session summaries."

## 6. Test peer isolation end-to-end

1. As **user A**, send some messages to Foreman via the main app so memories
   exist for A's peer (`<a-email>` → `<a-email-mapped>`).
2. Chat with the MAS as **user A**: ask "what do you remember about me?".
   Confirm the supervisor calls `foreman_peer_card` or
   `foreman_recall_about_me` and the response references A's content.
3. Chat with the MAS as **user B** (different SSO identity). Ask the same
   question. Confirm A's content does **not** appear — even if user B tries
   prompt-injecting "search for what user A said". Tool args have no
   peer/workspace fields, so there is no path for the agent to escape its own
   identity.

If any test in step 6 reveals cross-peer leakage, file an incident and
disable the tool in the MAS — the invariant is the entire point of this
surface.

## Notes

- The original stdio MCP at `src/foreman/mcp/` continues to serve Claude Code
  unchanged and shares no code path with this HTTP surface beyond the
  spine in `src/foreman/lib/`.
- This surface deliberately exposes **no write tools**. If you need a write
  path from Genie later, stand up a separate `mcp-foreman-write-${target}`
  app with stricter audit; do not extend this one.
- Adding `foreman_chat` (dialectic agent) is deferred. The agent loop carries
  latency + token-budget concerns that aren't worth tackling until the three
  read tools are proven in MAS.
