"""MLflow Tracing setup. Imported once at app and job startup."""

from __future__ import annotations

import asyncio
import functools
import inspect
import os
import types
from typing import Any, Callable

import mlflow


def init_tracing(experiment_name: str = "/Shared/foreman") -> None:
    """Configure MLflow to log traces against a Databricks experiment.

    Idempotent — safe to call multiple times. Pulls workspace from
    DATABRICKS_HOST / configured profile.
    """
    if os.environ.get("MLFLOW_TRACKING_URI") is None:
        mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(experiment_name)
    try:
        mlflow.openai.autolog()
    except Exception:
        pass
    try:
        mlflow.langchain.autolog()
    except Exception:
        pass


def _rehome_globals(wrapper: Callable[..., Any], target: Callable[..., Any]) -> Callable[..., Any]:
    # FastAPI's get_typed_signature resolves stringified annotations using
    # `callable.__globals__` directly (it does NOT walk __wrapped__). With
    # `from __future__ import annotations` active in every router, annotations
    # like `Annotated[auth.Scope, Depends(auth.current_scope)]` are strings
    # that only resolve in the router module's namespace. functools.wraps
    # copies __module__ but a function's __globals__ is read-only — fixed at
    # creation. Rebuild the function with types.FunctionType using the
    # original function's __globals__ so name resolution succeeds.
    if not isinstance(wrapper, types.FunctionType):
        return wrapper
    rehomed = types.FunctionType(
        wrapper.__code__,
        target.__globals__,
        wrapper.__name__,
        wrapper.__defaults__,
        wrapper.__closure__,
    )
    rehomed.__kwdefaults__ = wrapper.__kwdefaults__
    return rehomed


def _wrap(fn: Callable[..., Any], name: str | None = None) -> Callable[..., Any]:
    # Implement tracing via mlflow.start_span inside a functools.wraps wrapper
    # rather than mlflow.trace. On the Databricks Apps runtime mlflow.trace
    # returns a callable whose signature introspection drops the original
    # Annotated[..., Depends(...)] metadata — FastAPI then misreads every
    # Depends() as a required query param (422 on every route, see
    # docs/deployment-defects.html FOR-001). Span destination/format is
    # identical to mlflow.trace: spans land in the configured Databricks-
    # hosted MLflow experiment.
    span_name = name or fn.__qualname__
    # Bind mlflow into the closure so the wrapper doesn't depend on its own
    # __globals__ for the lookup. After _rehome_globals swaps __globals__ to
    # the router module's namespace, plain `mlflow` would otherwise NameError.
    _mlflow = mlflow

    if asyncio.iscoroutinefunction(fn):

        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            with _mlflow.start_span(name=span_name):
                return await fn(*args, **kwargs)

        wrapper: Callable[..., Any] = functools.wraps(fn)(_rehome_globals(async_wrapper, fn))
        return wrapper

    if inspect.isasyncgenfunction(fn):

        async def asyncgen_wrapper(*args: Any, **kwargs: Any):
            with _mlflow.start_span(name=span_name):
                async for item in fn(*args, **kwargs):
                    yield item

        return functools.wraps(fn)(_rehome_globals(asyncgen_wrapper, fn))

    if inspect.isgeneratorfunction(fn):

        def gen_wrapper(*args: Any, **kwargs: Any):
            with _mlflow.start_span(name=span_name):
                yield from fn(*args, **kwargs)

        return functools.wraps(fn)(_rehome_globals(gen_wrapper, fn))

    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        with _mlflow.start_span(name=span_name):
            return fn(*args, **kwargs)

    return functools.wraps(fn)(_rehome_globals(sync_wrapper, fn))


def trace(*args: Any, **kwargs: Any) -> Any:
    # Supports both shapes mlflow.trace supports:
    #   @trace                          — decorator, single positional callable
    #   @trace(name="...", **mlflow_kw) — decorator factory; only `name` is
    #                                     honored (start_span takes name +
    #                                     attributes only — keep this surface
    #                                     minimal).
    if len(args) == 1 and not kwargs and callable(args[0]):
        return _wrap(args[0])
    name = kwargs.get("name")

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        return _wrap(fn, name=name)

    return decorator
