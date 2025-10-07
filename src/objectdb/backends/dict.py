"""Dictionary-based example Database implementation for reference."""

import copy
from typing import Dict, Optional, Type

from objectdb.database import Database, DatabaseError, DatabaseItem, ForeignKey, PydanticObjectId, T, UnknownEntityError


class DictDatabase(Database):
    """Simple Database implementation with dictionary."""

    def __init__(self) -> None:
        self.data: Dict[Type[DatabaseItem], Dict[PydanticObjectId, DatabaseItem]] = {}

    async def update(self, item: DatabaseItem) -> None:
        """Update data."""
        item_type = type(item)
        if item_type not in self.data:
            self.data[item_type] = {}
        self.data[item_type][item.identifier] = copy.deepcopy(item)

    async def get(self, class_type: Type[T], identifier: PydanticObjectId) -> T:
        try:
            return self.data[class_type][identifier]  # type: ignore
        except KeyError as exc:
            raise UnknownEntityError(f"Unknown identifier: {identifier}") from exc

    async def get_all(self, class_type: Type[T]) -> Dict[str, T]:
        try:
            return self.data[class_type]  # type: ignore
        except KeyError as exc:
            raise DatabaseError(f"Unkonwn collection: {class_type}") from exc

    async def delete(self, class_type: Type[T], identifier: PydanticObjectId, cascade: bool = False) -> None:
        try:
            del self.data[class_type][identifier]
        except KeyError as exc:
            raise UnknownEntityError(f"Unknown identifier: {identifier}") from exc
        if cascade:
            for db in self.data:
                for identifier, item in self.data[db].items():
                    for attribute in item.__class__.model_fields:
                        if isinstance(attribute, ForeignKey) and attribute == item.identifier:
                            del self.data[db][identifier]

    async def find(self, class_type: Type[T], **kwargs: str) -> Optional[Dict[PydanticObjectId, T]]:
        try:
            results = []
            for item in self.data[class_type].values():  # type: ignore
                if all(getattr(item, k) == v for k, v in kwargs.items()):
                    results.append(item)  # type: ignore
            return {item.identifier: item for item in results}  # type: ignore
        except KeyError:
            return None

    async def find_one(self, class_type: Type[T], **kwargs: str) -> Optional[T]:
        if results := await self.find(class_type, **kwargs):
            if len(results) > 1:
                raise DatabaseError(f"Multiple entities found for {class_type} with {kwargs}")
            return list(results.values())[0]
        return None

    async def close(self) -> None:
        """Close database connection (no-op for DictDatabase)."""
        pass

    async def purge(self) -> None:
        """Purge all collections in the database."""
        self.data.clear()
