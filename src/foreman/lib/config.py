"""Runtime configuration. Resolved from environment, falling back to bundle defaults."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FOREMAN_", env_file=".env", extra="ignore")

    catalog: str = "foreman"
    schema_: str = Field("memory", alias="FOREMAN_SCHEMA")

    lakebase_instance: str = "foreman-lakebase"
    lakebase_database: str = "memory"
    lakebase_username: str | None = None  # resolved from current user / service principal if None

    vs_endpoint: str = "foreman-vs"

    jwt_secret_scope: str = "foreman"
    jwt_secret_key: str = "jwt_signing_key"  # secret key name within the scope

    # Default Model Serving endpoints, per role. Overridden per-workspace via
    # workspace_llm_config table (see lib/llm_dispatch.py).
    llm_default_dialectic: str = "databricks-meta-llama-3-3-70b-instruct"
    llm_default_deriver: str = "databricks-meta-llama-3-3-70b-instruct"
    llm_default_summarizer: str = "databricks-meta-llama-3-3-70b-instruct"
    llm_default_dreamer: str = "databricks-meta-llama-3-3-70b-instruct"
    llm_default_embeddings: str = "databricks-gte-large-en"

    # Vector index dimension — must match the embeddings endpoint above.
    embedding_dim: int = 1024

    @property
    def messages_index(self) -> str:
        return f"{self.catalog}.{self.schema_}.messages_idx"

    @property
    def documents_index(self) -> str:
        return f"{self.catalog}.{self.schema_}.documents_idx"

    @property
    def documents_delta(self) -> str:
        return f"{self.catalog}.{self.schema_}.documents_delta"

    @property
    def session_summaries_delta(self) -> str:
        return f"{self.catalog}.{self.schema_}.session_summaries_delta"

    def default_endpoint_for(self, role: str) -> str:
        return {
            "dialectic": self.llm_default_dialectic,
            "deriver": self.llm_default_deriver,
            "summarizer": self.llm_default_summarizer,
            "dreamer": self.llm_default_dreamer,
            "embeddings": self.llm_default_embeddings,
        }[role]


@lru_cache(maxsize=1)
def settings() -> Settings:
    return Settings()
