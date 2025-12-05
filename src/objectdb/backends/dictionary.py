"""Dictionary-based example Database implementation for reference."""

from __future__ import annotations

import copy

from objectdb.database import Database, DatabaseError, DatabaseItem, PydanticObjectId, T, UnknownEntityError
from objectdb.foreign_key import ForeignKey


class DictDatabase(Database):
    """Simple Database implementation with dictionary."""

    def __init__(self, supported_types: list[type[DatabaseItem]]) -> None:
        """Initialize empty dictionary."""
        super().__init__(supported_types)
        self.data: dict[type[DatabaseItem], dict[PydanticObjectId, DatabaseItem]] = {}

    async def upsert(self, item: DatabaseItem) -> PydanticObjectId | None:
        """Update data."""
        item_type = type(item)
        return_value = None
        if item_type not in self.data:
            self.data[item_type] = {}
        if item.identifier not in self.data[item_type]:
            return_value = item.identifier
        self.data[item_type][item.identifier] = copy.deepcopy(item)
        return return_value

    async def get(self, class_type: type[T], identifier: PydanticObjectId) -> T:
        """Return item."""
        try:
            return self.data[class_type][identifier]  # type: ignore
        except KeyError as exc:
            raise UnknownEntityError(f"Unknown identifier: {identifier}") from exc

    async def delete(self, class_type: type[T], identifier: PydanticObjectId, *, cascade: bool = False) -> None:
        """Delete item."""
        try:
            del self.data[class_type][identifier]
        except KeyError as exc:
            for possible_subtype in self.supported_types:
                if issubclass(possible_subtype, class_type) and identifier in self.data.get(possible_subtype, {}):
                    del self.data[possible_subtype][identifier]
                    return
            raise UnknownEntityError(f"Unknown identifier: {identifier}") from exc
        if cascade:
            for db in self.supported_types:
                for iter_identifier, item in self.data[db].items():
                    for attribute in item.__class__.model_fields:
                        if isinstance(attribute, ForeignKey) and attribute == item.identifier:
                            del self.data[db][iter_identifier]

    async def find(self, class_type: type[T], **kwargs: str) -> list[T]:
        """Find item."""
        try:
            return [
                item
                for item in self.data[class_type].values()
                if all(getattr(item, k) == v for k, v in kwargs.items())  # type: ignore
            ]
        except KeyError:
            return []

    async def find_inherited(self, class_type: type[T], **kwargs: str) -> list[T]:
        """Find subtype item in all collections."""
        try:
            results: list[T] = []
            for possibly_inherited_class_type in self.supported_types:
                if issubclass(possibly_inherited_class_type, class_type):
                    results.extend(
                        [
                            item
                            for item in self.data[possibly_inherited_class_type].values()
                            if all(getattr(item, k) == v for k, v in kwargs.items())  # type: ignore
                        ]
                    )
        except KeyError:
            return []
        return results

    async def find_one(self, class_type: type[T], **kwargs: str) -> T:
        """Find one item."""
        if results := await self.find(class_type, **kwargs):
            if len(results) > 1:
                raise DatabaseError(f"Multiple entities found for {class_type} with {kwargs}")
            return results[0]
        raise UnknownEntityError

    async def close(self) -> None:
        """Close database connection (no-op for DictDatabase)."""

    async def purge(self) -> None:
        """Purge all collections in the database."""
        self.data.clear()
