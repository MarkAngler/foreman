"""Initial honcho-parity schema.

Ports honcho's data model (src/models.py upstream) minus pgvector columns.
All 11 tables, composite keys, partial unique indexes for queue dedup,
GIN index on documents.source_ids, CHECK constraints, JSONB metadata columns.

Plus: workspace_llm_config (new) for per-workspace Model Serving overrides.
"""

from alembic import op


revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE workspaces (
            id              TEXT PRIMARY KEY,
            name            TEXT NOT NULL UNIQUE,
            metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
            internal_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            configuration   JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CHECK (char_length(name) BETWEEN 1 AND 256),
            CHECK (name ~ '^[A-Za-z0-9_\\-]+$')
        );

        CREATE TABLE peers (
            name              TEXT NOT NULL,
            workspace_name    TEXT NOT NULL REFERENCES workspaces(name) ON DELETE CASCADE,
            metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
            internal_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            configuration     JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (name, workspace_name),
            CHECK (char_length(name) BETWEEN 1 AND 256),
            CHECK (name ~ '^[A-Za-z0-9_\\-]+$')
        );

        CREATE TABLE sessions (
            name              TEXT NOT NULL,
            workspace_name    TEXT NOT NULL REFERENCES workspaces(name) ON DELETE CASCADE,
            metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
            internal_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            configuration     JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            deleted_at        TIMESTAMPTZ NULL,
            PRIMARY KEY (name, workspace_name),
            CHECK (char_length(name) BETWEEN 1 AND 256),
            CHECK (name ~ '^[A-Za-z0-9_\\-]+$')
        );

        CREATE TABLE session_peers (
            session_name      TEXT NOT NULL,
            peer_name         TEXT NOT NULL,
            workspace_name    TEXT NOT NULL,
            configuration     JSONB NOT NULL DEFAULT '{}'::jsonb,
            joined_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            left_at           TIMESTAMPTZ NULL,
            PRIMARY KEY (session_name, peer_name, workspace_name),
            FOREIGN KEY (session_name, workspace_name)
                REFERENCES sessions(name, workspace_name) ON DELETE CASCADE,
            FOREIGN KEY (peer_name, workspace_name)
                REFERENCES peers(name, workspace_name) ON DELETE CASCADE
        );

        CREATE TABLE messages (
            id                BIGSERIAL PRIMARY KEY,
            public_id         TEXT NOT NULL UNIQUE,
            session_name      TEXT NOT NULL,
            peer_name         TEXT NOT NULL,
            workspace_name    TEXT NOT NULL,
            content           TEXT NOT NULL,
            token_count       INTEGER NOT NULL DEFAULT 0,
            metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
            internal_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CHECK (char_length(content) <= 65535),
            FOREIGN KEY (session_name, workspace_name)
                REFERENCES sessions(name, workspace_name) ON DELETE CASCADE,
            FOREIGN KEY (peer_name, workspace_name)
                REFERENCES peers(name, workspace_name) ON DELETE CASCADE
        );
        CREATE INDEX messages_session_created_idx
            ON messages (workspace_name, session_name, created_at);
        CREATE INDEX messages_peer_created_idx
            ON messages (workspace_name, peer_name, created_at);

        CREATE TABLE documents (
            id                TEXT PRIMARY KEY,
            workspace_name    TEXT NOT NULL REFERENCES workspaces(name) ON DELETE CASCADE,
            observer          TEXT NOT NULL,
            observed          TEXT NOT NULL,
            level             TEXT NOT NULL
                CHECK (level IN ('explicit', 'deductive', 'inductive', 'contradiction')),
            content           TEXT NOT NULL,
            source_ids        TEXT[] NOT NULL DEFAULT '{}',
            times_derived     INTEGER NOT NULL DEFAULT 0,
            metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
            internal_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            deleted_at        DATE NULL,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX documents_collection_idx
            ON documents (workspace_name, observer, observed, level)
            WHERE deleted_at IS NULL;
        CREATE INDEX documents_source_ids_gin
            ON documents USING GIN (source_ids);

        CREATE TABLE queue_items (
            id                BIGSERIAL PRIMARY KEY,
            task_type         TEXT NOT NULL
                CHECK (task_type IN
                    ('webhook', 'summary', 'representation', 'dream', 'deletion', 'reconciler')),
            work_unit_key     TEXT NOT NULL,
            payload           JSONB NOT NULL DEFAULT '{}'::jsonb,
            processed         BOOLEAN NOT NULL DEFAULT FALSE,
            processed_at      TIMESTAMPTZ NULL,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        -- Honcho's exactly-once semantics: only one unprocessed item per
        -- (work_unit_key, task_type). Once processed, the row stays for audit.
        CREATE UNIQUE INDEX queue_items_unprocessed_unique
            ON queue_items (work_unit_key, task_type)
            WHERE processed = FALSE;
        CREATE INDEX queue_items_pending_idx
            ON queue_items (task_type, created_at)
            WHERE processed = FALSE;

        CREATE TABLE active_queue_session (
            id                TEXT PRIMARY KEY,
            work_unit_key     TEXT NOT NULL UNIQUE,
            last_updated      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE webhook_endpoints (
            id                TEXT PRIMARY KEY,
            workspace_name    TEXT NOT NULL REFERENCES workspaces(name) ON DELETE CASCADE,
            url               TEXT NOT NULL,
            secret            TEXT NULL,
            event_types       TEXT[] NOT NULL DEFAULT '{}',
            metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX webhook_endpoints_workspace_idx
            ON webhook_endpoints (workspace_name);

        CREATE TABLE peer_cards (
            peer_name         TEXT NOT NULL,
            workspace_name    TEXT NOT NULL,
            facts             TEXT[] NOT NULL DEFAULT '{}',
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (peer_name, workspace_name),
            FOREIGN KEY (peer_name, workspace_name)
                REFERENCES peers(name, workspace_name) ON DELETE CASCADE,
            CHECK (cardinality(facts) <= 40)
        );

        CREATE TABLE workspace_llm_config (
            workspace_name    TEXT NOT NULL REFERENCES workspaces(name) ON DELETE CASCADE,
            role              TEXT NOT NULL
                CHECK (role IN ('dialectic', 'deriver', 'dreamer', 'summarizer', 'embeddings')),
            endpoint_name     TEXT NOT NULL,
            provider          TEXT NOT NULL
                CHECK (provider IN ('fmapi', 'external', 'openai-compatible', 'custom')),
            params            JSONB NOT NULL DEFAULT '{}'::jsonb,
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (workspace_name, role)
        );

        -- Mirror table: derived documents are written by Spark Jobs to the
        -- documents_delta UC table, then a Delta -> Lakebase synced table mirrors
        -- them here so the FastAPI app can do low-latency get-by-id reads.
        -- Schema mirrors `documents` so the app can SELECT against either.
        CREATE TABLE documents_mirror (
            id                TEXT PRIMARY KEY,
            workspace_name    TEXT NOT NULL,
            observer          TEXT NOT NULL,
            observed          TEXT NOT NULL,
            level             TEXT NOT NULL,
            content           TEXT NOT NULL,
            source_ids        TEXT[] NOT NULL DEFAULT '{}',
            times_derived     INTEGER NOT NULL DEFAULT 0,
            metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at        TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX documents_mirror_collection_idx
            ON documents_mirror (workspace_name, observer, observed, level);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS documents_mirror CASCADE;
        DROP TABLE IF EXISTS workspace_llm_config CASCADE;
        DROP TABLE IF EXISTS peer_cards CASCADE;
        DROP TABLE IF EXISTS webhook_endpoints CASCADE;
        DROP TABLE IF EXISTS active_queue_session CASCADE;
        DROP TABLE IF EXISTS queue_items CASCADE;
        DROP TABLE IF EXISTS documents CASCADE;
        DROP TABLE IF EXISTS messages CASCADE;
        DROP TABLE IF EXISTS session_peers CASCADE;
        DROP TABLE IF EXISTS sessions CASCADE;
        DROP TABLE IF EXISTS peers CASCADE;
        DROP TABLE IF EXISTS workspaces CASCADE;
        """
    )
