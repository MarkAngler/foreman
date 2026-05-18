"""Unit tests for the foreman.lib.tracing.trace wrapper.

Regression coverage for FOR-001 — `mlflow.trace` on the deployed runtime strips
Annotated/Depends metadata from FastAPI handlers, which makes the dependency
injector misclassify every Depends() as a required query parameter. The
wrapper restores `__signature__`, `__annotations__`, and `__wrapped__` on the
returned callable so FastAPI's `inspect.signature()` introspection works.
"""

from __future__ import annotations

import inspect
from typing import Annotated

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from foreman.lib.tracing import trace


def _dep() -> str:
    return "x"


def test_signature_preserved_on_simple_function():
    def fn(a: int, b: str = "hi") -> str:
        return f"{a}{b}"

    sig_before = inspect.signature(fn)
    wrapped = trace(fn)
    assert inspect.signature(wrapped) == sig_before


def test_annotated_depends_preserved():
    async def handler(
        path_id: str,
        dep_value: Annotated[str, Depends(_dep)],
    ) -> dict:
        return {"id": path_id, "dep": dep_value}

    sig_before = inspect.signature(handler)
    wrapped = trace(handler)
    sig_after = inspect.signature(wrapped)

    # The strict signature equality is the contract FastAPI relies on:
    # if it survives, Annotated[..., Depends(...)] is also intact end-to-end
    # (verified by test_fastapi_route_with_trace_resolves_depends).
    assert sig_after == sig_before


def test_wrapped_attribute_points_to_original():
    def fn() -> int:
        return 1

    wrapped = trace(fn)
    assert getattr(wrapped, "__wrapped__", None) is fn


def test_fastapi_route_with_trace_resolves_depends():
    # Regression: if @trace drops the signature, FastAPI raises this Depends()
    # as a required query param and the endpoint returns 422.
    app = FastAPI()

    @app.get("/probe")
    @trace
    async def probe(dep_value: Annotated[str, Depends(_dep)]) -> dict:
        return {"dep": dep_value}

    client = TestClient(app)
    resp = client.get("/probe")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"dep": "x"}
