"""MongoDB Database implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from objectdb.database import Database, DatabaseItem, PydanticObjectId, T, UnknownEntityError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pymongo import AsyncMongoClient
    from pymongo.asynchronous.database import AsyncDatabase


class MongoDBDatabase(Database):
    """MongoDB database implementation."""

    def __init__(self, supported_types: list[type[DatabaseItem]], mongodb_client: AsyncMongoClient, name: str) -> None:
        """Initialize database connection and create database."""
        super().__init__(supported_types)
        self.connection: AsyncMongoClient[Mapping[str, dict[str, Any]]] = mongodb_client
        self.database: AsyncDatabase[Mapping[str, dict[str, Any]]] = self.connection[name]

    async def upsert(self, item: DatabaseItem) -> PydanticObjectId | None:
        """Update data."""
        item_type = type(item)
        upsert_result = await self.database[item_type.__name__].update_one(
            filter={"_id": item.identifier}, update={"$set": item.model_dump(exclude={"identifier"})}, upsert=True
        )
        if upsert_result.matched_count:
            return None
        return PydanticObjectId(upsert_result.upserted_id)

    async def get(self, class_type: type[T], identifier: PydanticObjectId) -> T:
        """Get item."""
        collection = self.database[class_type.__name__]
        if result := await collection.find_one(filter={"_id": identifier}):
            return class_type.model_validate(result)
        raise UnknownEntityError(f"Not found {class_type} with identifier: {identifier}")

    async def delete(self, class_type: type[T], identifier: PydanticObjectId, *, cascade: bool = False) -> None:  # noqa: ARG002
        """Delete item."""
        collection = self.database[class_type.__name__]
        result = await collection.delete_one(filter={"_id": identifier})
        if result.deleted_count == 0:
            for possible_subtype in self.supported_types:
                if issubclass(possible_subtype, class_type):
                    collection = self.database[possible_subtype.__name__]
                    result = await collection.delete_one(filter={"_id": identifier})
                    if result.deleted_count != 0:
                        return
            raise UnknownEntityError(f"Not found {class_type} with identifier: {identifier}")

    async def find(self, class_type: type[T], **kwargs: Any) -> list[T]:
        """Find item."""
        collection = self.database[class_type.__name__]
        results = collection.find(filter=kwargs)
        return [class_type.model_validate(result) async for result in results]

    async def find_inherited(self, class_type: type[T], **kwargs: Any) -> list[T]:
        """Find item among subtypes."""
        validated_results: list[T] = []
        for possible_subtype in self.supported_types:
            if issubclass(possible_subtype, class_type):
                collection = self.database[possible_subtype.__name__]
                results = collection.find(filter=kwargs)
                validated_results.extend([possible_subtype.model_validate(result) async for result in results])
        return validated_results

    async def close(self) -> None:
        """Close client connection."""
        await self.connection.close()

    async def purge(self) -> None:
        """Purge all collections in the database."""
        collection_names = await self.database.list_collection_names()
        for name in collection_names:
            await self.database.drop_collection(name)
