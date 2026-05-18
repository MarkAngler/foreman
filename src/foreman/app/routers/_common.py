"""Shared helpers for foreman REST routers.

Keep this module thin — only things every router uses. Anything specific to a
single router stays in that router file.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException, status
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError

from foreman.app import schemas


def hydrate(row: Any) -> dict[str, Any]:
    """SQLAlchemy Row/RowMapping -> dict that Pydantic from_attributes can ingest."""
    if row is None:
        return {}
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    if isinstance(row, dict):
        return row
    raise TypeError(f"cannot hydrate row of type {type(row)!r}")


def not_found(detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


def conflict(detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=detail)


def bad_request(detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


def assert_path_name(value: str, *, kind: str) -> None:
    try:
        schemas.assert_name(value, kind=kind)
    except ValueError as e:
        raise bad_request(str(e)) from e


def integrity_to_conflict(exc: IntegrityError, *, detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=detail)


def validation_to_bad_request(exc: ValidationError) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=exc.errors())
