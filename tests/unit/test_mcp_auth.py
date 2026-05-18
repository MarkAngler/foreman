"""Unit tests for foreman.mcp.auth credential resolution.

No Databricks calls — WorkspaceClient and env are mocked. Verifies the
first-match-wins resolution order and clear error messages on failure.
"""

from __future__ import annotations

import base64
from unittest.mock import MagicMock, patch

import jwt
import pytest

from foreman.mcp import auth as mcp_auth


@pytest.fixture(autouse=True)
def _clear_cache_and_env(monkeypatch):
    mcp_auth.resolve_credentials.cache_clear()
    for var in (
        "FOREMAN_BASE_URL",
        "FOREMAN_TOKEN",
        "FOREMAN_TARGET",
        "FOREMAN_SECRET_SCOPE",
        "FOREMAN_JWT_SECRET_KEY",
    ):
        monkeypatch.delenv(var, raising=False)
    yield
    mcp_auth.resolve_credentials.cache_clear()


def test_env_vars_bypass_databricks_sdk(monkeypatch):
    monkeypatch.setenv("FOREMAN_BASE_URL", "https://foreman.example.com/")
    monkeypatch.setenv("FOREMAN_TOKEN", "static-token-xyz")

    with patch.object(mcp_auth, "_workspace_client") as wc:
        creds = mcp_auth.resolve_credentials()

    assert creds.base_url == "https://foreman.example.com"
    assert creds.bearer_token() == "static-token-xyz"
    wc.assert_not_called()


def test_fetches_app_url_and_mints_jwt_when_no_env(monkeypatch):
    signing_key = "test-signing-key-bytes"
    fake_w = MagicMock()
    fake_w.apps.get.return_value = MagicMock(url="https://foreman-dev.databricks.app")
    fake_w.secrets.get_secret.return_value = MagicMock(
        value=base64.b64encode(signing_key.encode()).decode()
    )

    with patch.object(mcp_auth, "_workspace_client", return_value=fake_w):
        creds = mcp_auth.resolve_credentials()

    assert creds.base_url == "https://foreman-dev.databricks.app"
    token = creds.bearer_token()
    decoded = jwt.decode(token, signing_key, algorithms=["HS256"])
    assert decoded["ad"] is True
    fake_w.apps.get.assert_called_once_with(name="foreman-dev")
    fake_w.secrets.get_secret.assert_called_once_with(scope="foreman", key="jwt_signing_key")


def test_target_env_var_changes_app_name(monkeypatch):
    monkeypatch.setenv("FOREMAN_TARGET", "prod")
    fake_w = MagicMock()
    fake_w.apps.get.return_value = MagicMock(url="https://foreman-prod.databricks.app")
    fake_w.secrets.get_secret.return_value = MagicMock(value=base64.b64encode(b"key").decode())

    with patch.object(mcp_auth, "_workspace_client", return_value=fake_w):
        mcp_auth.resolve_credentials()

    fake_w.apps.get.assert_called_once_with(name="foreman-prod")


def test_env_token_with_resolved_url(monkeypatch):
    monkeypatch.setenv("FOREMAN_TOKEN", "ci-token")
    fake_w = MagicMock()
    fake_w.apps.get.return_value = MagicMock(url="https://foreman-dev.databricks.app")

    with patch.object(mcp_auth, "_workspace_client", return_value=fake_w):
        creds = mcp_auth.resolve_credentials()

    assert creds.bearer_token() == "ci-token"
    fake_w.secrets.get_secret.assert_not_called()


def test_each_bearer_token_is_freshly_minted(monkeypatch):
    fake_w = MagicMock()
    fake_w.apps.get.return_value = MagicMock(url="https://x")
    fake_w.secrets.get_secret.return_value = MagicMock(value=base64.b64encode(b"k").decode())
    with patch.object(mcp_auth, "_workspace_client", return_value=fake_w):
        creds = mcp_auth.resolve_credentials()

    t1 = creds.bearer_token()
    # JWTs include `t` (issued-at, seconds) — minting again immediately produces
    # the same token, but the provider is callable each time, not memoized.
    assert callable(creds._provider)
    assert creds._provider() == t1


def test_missing_app_url_raises(monkeypatch):
    fake_w = MagicMock()
    fake_w.apps.get.return_value = MagicMock(url=None)
    with patch.object(mcp_auth, "_workspace_client", return_value=fake_w):
        with pytest.raises(mcp_auth.CredentialsError, match="no url"):
            mcp_auth.resolve_credentials()


def test_empty_signing_key_secret_raises(monkeypatch):
    fake_w = MagicMock()
    fake_w.apps.get.return_value = MagicMock(url="https://x")
    fake_w.secrets.get_secret.return_value = MagicMock(value="")
    with patch.object(mcp_auth, "_workspace_client", return_value=fake_w):
        with pytest.raises(mcp_auth.CredentialsError, match="empty"):
            mcp_auth.resolve_credentials()


def test_secret_scope_overridable(monkeypatch):
    monkeypatch.setenv("FOREMAN_SECRET_SCOPE", "other-scope")
    monkeypatch.setenv("FOREMAN_JWT_SECRET_KEY", "other-key")
    fake_w = MagicMock()
    fake_w.apps.get.return_value = MagicMock(url="https://x")
    fake_w.secrets.get_secret.return_value = MagicMock(value=base64.b64encode(b"k").decode())
    with patch.object(mcp_auth, "_workspace_client", return_value=fake_w):
        mcp_auth.resolve_credentials()
    fake_w.secrets.get_secret.assert_called_once_with(scope="other-scope", key="other-key")
