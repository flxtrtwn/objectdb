"""Database connection."""

from __future__ import annotations

import http
from typing import Any, TypeVar

import httpx

from objectdb.database import DatabaseItem, PydanticObjectId, UnknownEntityError

TIMEOUT = 5

T = TypeVar("T", bound=DatabaseItem)


class DBConnection:
    """Database connection."""

    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint

    async def get(self, model: type[T], identifier: PydanticObjectId | str) -> T:
        """Retrieve entity."""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{self.endpoint}/{model.__name__.lower()}/{identifier}", timeout=TIMEOUT)
                response.raise_for_status()
            except httpx.HTTPError as exc:
                if response.status_code == http.HTTPStatus.NOT_FOUND:  # type: ignore
                    raise UnknownEntityError from exc
                raise
            return model.model_validate(response.json())

    async def find(self, model: type[T], **kwargs: Any) -> list[T]:
        """Find entities."""
        async with httpx.AsyncClient() as client:
            try:
                url = f"{self.endpoint}/{model.__name__.lower()}"

                response = await client.get(url, params=kwargs, timeout=TIMEOUT)
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise ValueError("Unsuccessful database request.") from exc
            return [model.model_validate(element) for element in response.json()]

    async def upsert(self, item: DatabaseItem) -> None:
        """Update data or create if does not exist."""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{self.endpoint}/{type(item).__name__.lower()}", json=item.model_dump(mode="json"), timeout=TIMEOUT
                )
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise ValueError(f"Unsuccessful database request: {exc}") from exc

    async def delete(self, item: DatabaseItem) -> None:
        """Delete entity."""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.delete(
                    f"{self.endpoint}/{type(item).__name__.lower()}/{item.identifier}", timeout=TIMEOUT
                )
                response.raise_for_status()
            except httpx.HTTPError as exc:
                if response.status_code == http.HTTPStatus.NOT_FOUND:  # type: ignore
                    raise UnknownEntityError from exc
                raise
