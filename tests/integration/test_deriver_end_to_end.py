"""End-to-end deriver test: messages -> facts -> documents.

Spark and Lakebase are mocked here because spinning up a real Spark session
inside CI is impractical. The flow exercised is the full orchestration:
fetch messages -> resolve session_peers -> per-(observed) LLM calls ->
parse JSON -> fan out across observers -> upsert into documents_delta ->
trigger Vector Search sync -> advance the watermark.

Run with:
    PYTHONPATH=src .venv/Scripts/python.exe -m pytest tests/integration/ -m integration
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from types import ModuleType
from unittest.mock import patch

import pytest

from foreman.jobs.deriver import main as deriver_main

pytestmark = pytest.mark.integration


def _install_fake_pyspark(spark_instance) -> None:
    """Register a stub `pyspark.sql` (+ `pyspark.sql.types`) for the deriver's
    lazy `from pyspark.sql import ...` calls. PySpark is provisioned by the
    Databricks runtime, not the dev venv, so we shim it here.
    """
    pyspark_mod = ModuleType("pyspark")
    pyspark_sql_mod = ModuleType("pyspark.sql")
    pyspark_sql_types_mod = ModuleType("pyspark.sql.types")

    class _FakeBuilder:
        @staticmethod
        def getOrCreate():
            return spark_instance

    class _FakeSparkSession:
        builder = _FakeBuilder()

    def _fake_row(**kwargs):
        return _Row(kwargs)

    # Type stubs - the deriver passes these to createDataFrame, our FakeSpark ignores schema.
    class _StubType:
        def __init__(self, *args, **kwargs):
            pass

    pyspark_sql_mod.SparkSession = _FakeSparkSession  # type: ignore[attr-defined]
    pyspark_sql_mod.Row = _fake_row  # type: ignore[attr-defined]
    pyspark_sql_mod.DataFrame = object  # type: ignore[attr-defined]
    for name in (
        "ArrayType",
        "IntegerType",
        "StringType",
        "StructField",
        "StructType",
        "TimestampType",
    ):
        setattr(pyspark_sql_types_mod, name, _StubType)

    pyspark_mod.sql = pyspark_sql_mod  # type: ignore[attr-defined]

    sys.modules["pyspark"] = pyspark_mod
    sys.modules["pyspark.sql"] = pyspark_sql_mod
    sys.modules["pyspark.sql.types"] = pyspark_sql_types_mod


@pytest.fixture(autouse=True)
def _cleanup_pyspark_stub():
    yield
    sys.modules.pop("pyspark", None)
    sys.modules.pop("pyspark.sql", None)
    sys.modules.pop("pyspark.sql.types", None)


class _Row(dict):
    """Mimic Spark's Row: subscript and attribute access, plus `.asDict()`."""

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError as e:
            raise AttributeError(item) from e

    def asDict(self):
        return dict(self)


def _row(**kwargs) -> _Row:
    return _Row(kwargs)


class FakeDataFrame:
    """Stand-in for a Spark DataFrame: holds rows and replays them on collect()."""

    def __init__(self, rows):
        self._rows = list(rows)

    def collect(self):
        return self._rows

    def limit(self, n: int) -> "FakeDataFrame":
        return FakeDataFrame(self._rows[:n])

    def createOrReplaceTempView(self, _name):
        return None


class FakeSpark:
    """A minimal SparkSession harness that records sql() calls and stub-returns DataFrames."""

    def __init__(self):
        self.sql_calls: list[str] = []
        self._collected_dfs: list[FakeDataFrame] = []
        self.created_dfs: list[FakeDataFrame] = []
        self._scripted_sql: dict[str, FakeDataFrame] = {}

    def script_sql(self, predicate_substring: str, df: FakeDataFrame) -> None:
        self._scripted_sql[predicate_substring] = df

    def sql(self, query: str):
        self.sql_calls.append(query)
        for needle, df in self._scripted_sql.items():
            if needle in query:
                return df
        # MERGE / CREATE TABLE statements return an empty result.
        return FakeDataFrame([])

    def createDataFrame(self, rows, schema=None):  # noqa: ARG002 - matches Spark signature
        df = FakeDataFrame(list(rows))
        self.created_dfs.append(df)
        return df


def test_full_pipeline_writes_documents_and_advances_watermark():
    spark = FakeSpark()
    spark.script_sql(
        "FROM " + deriver_main.state_table(),
        FakeDataFrame([_row(value="100")]),
    )

    messages = [
        _row(
            id=101,
            public_id="pub-101",
            session_name="s1",
            peer_name="alice",
            workspace_name="ws",
            content="I just got a dog named Rover",
            created_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        ),
        _row(
            id=102,
            public_id="pub-102",
            session_name="s1",
            peer_name="bob",
            workspace_name="ws",
            content="What kind of dog?",
            created_at=datetime(2026, 1, 1, 12, 0, 1, tzinfo=timezone.utc),
        ),
        _row(
            id=103,
            public_id="pub-103",
            session_name="s1",
            peer_name="alice",
            workspace_name="ws",
            content="A golden retriever puppy",
            created_at=datetime(2026, 1, 1, 12, 0, 2, tzinfo=timezone.utc),
        ),
    ]

    session_peers_rows = [
        _row(workspace_name="ws", session_name="s1", peer_name="alice"),
        _row(workspace_name="ws", session_name="s1", peer_name="bob"),
    ]

    def fake_read_query(_spark, query: str):
        if "FROM messages" in query:
            return FakeDataFrame(messages)
        if "FROM session_peers" in query:
            return FakeDataFrame(session_peers_rows)
        raise AssertionError(f"unexpected JDBC query: {query}")

    chat_calls: list[dict] = []

    def fake_chat_sync(*, workspace_name, role, messages):
        chat_calls.append({"workspace": workspace_name, "role": role})
        first_content = messages[0]["content"]
        if "alice" in first_content and "puppy" in first_content:
            response = (
                '{"explicit": ['
                '{"content": "alice has a dog named Rover"},'
                '{"content": "alice\'s dog is a golden retriever puppy"}'
                "]}"
            )
        else:
            response = '{"explicit": [{"content": "bob asked about alice\'s dog"}]}'
        return {"choices": [{"message": {"content": response}}]}

    upsert_calls: list[FakeDataFrame] = []

    def fake_upsert(_spark, df):
        upsert_calls.append(df)

    sync_called = {"count": 0}

    def fake_sync():
        sync_called["count"] += 1

    _install_fake_pyspark(spark)
    with patch.object(deriver_main, "init_tracing"), \
         patch.object(deriver_main.delta_write, "ensure_documents_delta"), \
         patch.object(deriver_main.lakebase_read, "read_query", side_effect=fake_read_query), \
         patch.object(deriver_main.llm_dispatch, "chat_sync", side_effect=fake_chat_sync), \
         patch.object(deriver_main.delta_write, "upsert_documents", side_effect=fake_upsert), \
         patch.object(deriver_main.vector_search, "trigger_documents_sync", side_effect=fake_sync):
        rc = deriver_main.main()

    assert rc == 0

    # Two observed peers (alice, bob) -> two LLM calls.
    assert len(chat_calls) == 2
    assert {c["role"] for c in chat_calls} == {"deriver"}

    # One MERGE call into documents_delta.
    assert len(upsert_calls) == 1
    drafts = upsert_calls[0]._rows  # FakeDataFrame uses _rows for createDataFrame
    # 2 alice facts + 1 bob fact = 3 facts; each fanned across 2 observers = 6 docs.
    assert len(drafts) == 6
    assert {(d["observer"], d["observed"]) for d in (r.asDict() for r in drafts)} == {
        ("alice", "alice"),
        ("bob", "alice"),
        ("alice", "bob"),
        ("bob", "bob"),
    }
    # IDs must be unique and 24 chars.
    ids = {r.asDict()["id"] for r in drafts}
    assert len(ids) == 6
    for doc_id in ids:
        assert len(doc_id) == 24

    # Vector Search sync was kicked.
    assert sync_called["count"] == 1

    # Watermark advanced to the highest message id.
    merge_sqls = [s for s in spark.sql_calls if "MERGE INTO" in s and "deriver_state" in s]
    assert merge_sqls, "expected a MERGE into deriver_state to update the watermark"
    written_watermarks = [r.asDict()["value"] for r in spark.created_dfs[-1]._rows]
    assert "103" in written_watermarks


def test_no_messages_skips_llm_and_does_not_advance_watermark():
    spark = FakeSpark()
    spark.script_sql(
        "FROM " + deriver_main.state_table(),
        FakeDataFrame([_row(value="999")]),
    )

    def fake_read_query(_spark, _query):
        return FakeDataFrame([])

    chat_calls = 0

    def fake_chat_sync(**_kwargs):
        nonlocal chat_calls
        chat_calls += 1
        return {}

    _install_fake_pyspark(spark)
    with patch.object(deriver_main, "init_tracing"), \
         patch.object(deriver_main.delta_write, "ensure_documents_delta"), \
         patch.object(deriver_main.lakebase_read, "read_query", side_effect=fake_read_query), \
         patch.object(deriver_main.llm_dispatch, "chat_sync", side_effect=fake_chat_sync), \
         patch.object(deriver_main.delta_write, "upsert_documents") as mock_upsert, \
         patch.object(deriver_main.vector_search, "trigger_documents_sync") as mock_sync:
        rc = deriver_main.main()

    assert rc == 0
    assert chat_calls == 0
    mock_upsert.assert_not_called()
    mock_sync.assert_not_called()
    # Watermark unchanged: no MERGE INTO deriver_state should have happened.
    assert not any("MERGE INTO" in s and "deriver_state" in s for s in spark.sql_calls)
