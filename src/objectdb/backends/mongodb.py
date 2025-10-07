"""MongoDB Database implementation."""

from typing import Any, Dict, Mapping, Optional, Type

from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase

from objectdb.database import Database, DatabaseItem, PydanticObjectId, T, UnknownEntityError


class MongoDBDatabase(Database):
    """MongoDB database implementation."""

    def __init__(self, mongodb_client: AsyncMongoClient, name: str) -> None:
        self.connection: AsyncMongoClient[Mapping[str, dict[str, Any]]] = mongodb_client
        self.database: AsyncDatabase[Mapping[str, dict[str, Any]]] = self.connection[name]

    async def update(self, item: DatabaseItem):
        """Update data."""
        item_type = type(item)
        await self.database[item_type.__name__].update_one(
            filter={"_id": item.identifier}, update={"$set": item.model_dump(by_alias=True)}, upsert=True
        )

    async def get(self, class_type: Type[T], identifier: PydanticObjectId) -> T:
        collection = self.database[class_type.__name__]
        if res := await collection.find_one(filter={"_id": identifier}):
            return class_type.model_validate(res)
        raise UnknownEntityError(f"Unknown identifier: {identifier}")

    async def get_all(self, class_type: Type[T]) -> Optional[Dict[str, T]]:
        collection = self.database[class_type.__name__]
        validated_results: dict[PydanticObjectId, T] = {}
        if results := collection.find():
            async for result in results:
                validated_result = class_type.model_validate(result)
                validated_results[validated_result.identifier] = validated_result
            return validated_results
        return None

    async def delete(self, class_type: Type[T], identifier: PydanticObjectId, cascade: bool = False) -> None:
        collection = self.database[class_type.__name__]
        result = await collection.delete_one(filter={"_id": identifier})
        if result.deleted_count != 1:
            raise UnknownEntityError(f"Unknown identifier: {identifier}")

    async def find(self, class_type: Type[T], **kwargs: Any) -> Optional[Dict[PydanticObjectId, T]]:
        collection = self.database[class_type.__name__]
        validated_results: dict[PydanticObjectId, T] = {}
        if results := collection.find(filter=kwargs):
            async for result in results:
                validated_result = class_type.model_validate(result)
                validated_results[validated_result.identifier] = validated_result
            return validated_results
        return None

    async def find_one(self, class_type: Type[T], **kwargs: Any) -> Optional[T]:
        """Find one item matching the criteria."""
        collection = self.database[class_type.__name__]
        if result := await collection.find_one(filter=kwargs):
            return class_type.model_validate(result)
        return None

    async def close(self) -> None:
        """Close client connection."""
        await self.connection.close()

    async def purge(self) -> None:
        """Purge all collections in the database."""
        collection_names = await self.database.list_collection_names()
        for name in collection_names:
            await self.database.drop_collection(name)
