"""Pytest configuration and fixtures."""

from __future__ import annotations

import asyncio
import types
from typing import Any, AsyncGenerator, Callable, Mapping, Self

import mongomock
import pymongo
import pymongo.collection
import pymongo.cursor
import pymongo.database
import pytest
import pytest_asyncio

from objectdb.backends.dict import DictDatabase
from objectdb.backends.mongodb import MongoDBDatabase


def asyncify(fn: Callable) -> Callable[..., types.CoroutineType[Any, Any, Any]]:
    """Wrap a sync function to make it awaitable."""

    async def wrapper(*args: tuple[Any], **kwargs: dict[str, Any]) -> Any:
        return await asyncio.to_thread(fn, *args, **kwargs)  # type: ignore

    return wrapper


class AsyncCursor:
    """Async iterator for mongomock cursor results."""

    def __init__(self, cursor: pymongo.cursor.Cursor) -> None:
        # Pre-fetch all docs into memory, since mongomock is in-memory anyway
        self._docs: list[pymongo.cursor.Cursor[Any]] = list(cursor)
        self._index = 0

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> pymongo.collection.Cursor[Any]:
        if self._index >= len(self._docs):
            raise StopAsyncIteration
        doc = self._docs[self._index]
        self._index += 1
        return doc


class AsyncMongoMockCollection:
    """Async wrapper around a mongomock Collection."""

    def __init__(self, collection: pymongo.collection.Collection):
        self._collection: pymongo.collection.Collection[Mapping[str, dict[str, Any]]] = collection
        self.find_one = asyncify(collection.find_one)
        self.update_one = asyncify(collection.update_one)
        self.delete_one = asyncify(collection.delete_one)
        self.count_documents = asyncify(collection.count_documents)

    def find(self, *args: tuple[Any], **kwargs: dict[str, Any]) -> AsyncCursor:
        """Return an async cursor wrapper instead of a coroutine."""
        cursor = self._collection.find(*args, **kwargs)
        return AsyncCursor(cursor)


class AsyncMongoMockDatabase:
    """Async wrapper around a mongomock Database."""

    def __init__(self, db: pymongo.database.Database) -> None:
        self._db: pymongo.database.Database[Mapping[str, dict[str, Any]]] = db

    def __getitem__(self, name: str) -> AsyncMongoMockCollection:
        return AsyncMongoMockCollection(self._db[name])

    def list_collection_names(self) -> types.CoroutineType[Any, Any, list[str]]:
        return asyncify(self._db.list_collection_names)()

    def drop_collection(self, name: str) -> types.CoroutineType[Any, Any, None]:
        return asyncify(self._db.drop_collection)(name)


class AsyncMongoMockClient:
    """Async-compatible mongomock.MongoClient that mimics PyMongo Async client."""

    def __init__(self) -> None:
        self._sync_client: mongomock.MongoClient[Mapping[str, dict[str, Any]]] = mongomock.MongoClient()

    def __getitem__(self, name: str) -> AsyncMongoMockDatabase:
        return AsyncMongoMockDatabase(self._sync_client[name])

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> types.NoneType:  # type: ignore
        await self.close()

    async def close(self) -> None:
        """Close the client (no-op for mongomock)."""


@pytest_asyncio.fixture(
    name="db",
    params=[MongoDBDatabase(mongodb_client=AsyncMongoMockClient(), name="test_db"), DictDatabase()],  # type: ignore
)
async def db_fixture(request: pytest.FixtureRequest) -> AsyncGenerator[MongoDBDatabase, None]:
    """Async-compatible mongomock database for PyMongo asyncio tests."""
    db = request.param
    yield db
    await db.purge()
