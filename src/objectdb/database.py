"""Database abstraction layer."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, Optional, Type, TypeVar

import fastapi
import pydantic
from bson.objectid import ObjectId
from pydantic_core import core_schema

T = TypeVar("T", bound="DatabaseItem")


class ForeignKey(Generic[T]):
    """A reference to another DatabaseItem."""

    def __init__(self, target_type: type[T], identifier: str):
        self.target_type = target_type
        self.identifier = identifier

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, ForeignKey)
            and self.target_type == other.target_type
            and self.identifier == other.identifier
        )

    def __hash__(self) -> int:
        return hash((self.target_type, self.identifier))

    def __repr__(self) -> str:
        return f"ForeignKey({self.target_type.__name__}:{self.identifier})"

    @classmethod
    def __class_getitem__(cls, item: type[T]):
        target_type = item

        class _ForeignKey(cls):  # type: ignore
            __origin__ = cls
            __args__ = (item,)

            @classmethod
            def __get_pydantic_core_schema__(cls, source_type, handler: pydantic.GetCoreSchemaHandler):
                def validator(v):
                    if isinstance(v, ForeignKey):
                        return v
                    if isinstance(v, target_type):
                        return ForeignKey(target_type, v.identifier)
                    if isinstance(v, str):
                        return ForeignKey(target_type, v)
                    raise TypeError(f"Cannot convert {v!r} to ForeignKey[{target_type.__name__}]")

                return core_schema.no_info_after_validator_function(
                    validator,
                    core_schema.union_schema(
                        [
                            core_schema.is_instance_schema(target_type),
                            core_schema.str_schema(),
                            core_schema.is_instance_schema(ForeignKey),
                        ]
                    ),
                )

            @classmethod
            def __get_pydantic_json_schema__(cls, _core_schema, handler):
                return handler(core_schema.str_schema())

        return _ForeignKey


class PyObjectId(ObjectId):
    """Custom ObjectId type for Pydantic."""

    @classmethod
    def __get_pydantic_core_schema__(cls, _source, _handler) -> core_schema.PlainValidatorFunctionSchema:
        return core_schema.no_info_plain_validator_function(cls.validate)

    @classmethod
    def validate(cls, v: Any) -> PyObjectId:
        """Validate and convert to ObjectId."""
        if isinstance(v, ObjectId):
            return cls(v)
        if not ObjectId.is_valid(v):
            raise ValueError(f"Invalid ObjectId: {v}")
        return cls(v)


class DatabaseItem(ABC, pydantic.BaseModel):
    """Base class for database items."""

    model_config = pydantic.ConfigDict(revalidate_instances="always", populate_by_name=True)
    identifier: PyObjectId = pydantic.Field(default_factory=PyObjectId, alias="_id")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DatabaseItem):
            return NotImplemented
        return self.identifier == other.identifier

    def __hash__(self) -> int:
        return hash(self.identifier)


router = fastapi.APIRouter()


class Database(ABC):
    """Database abstraction."""

    @abstractmethod
    async def update(self, item: DatabaseItem) -> None:
        """Update entity."""

    @abstractmethod
    async def get(self, class_type: Type[T], identifier: PyObjectId) -> T:
        """Return entity, raise UnknownEntityError if entity does not exist."""

    @abstractmethod
    async def get_all(self, class_type: Type[T]) -> Dict[str, T]:
        """Return all entities of collection."""

    @abstractmethod
    async def delete(self, class_type: Type[T], identifier: PyObjectId, cascade: bool = False) -> None:
        """Delete entity."""

    @abstractmethod
    async def find(self, class_type: Type[T], **kwargs: str) -> Optional[Dict[PyObjectId, T]]:
        """Return all entities of collection matching the filter criteria."""

    @abstractmethod
    async def find_one(self, class_type: Type[T], **kwargs: str) -> Optional[T]:
        """Return one entitiy of collection matching the filter criteria, raise if multiple exist."""

    @router.get("/{collection}/{identifier}")
    async def get_item(self, class_type: Type[T], identifier: PyObjectId) -> T:
        """Get an item from the database."""
        return await self.get(class_type, identifier)

    @router.post("/{collection}/")
    async def update_item(self, item: DatabaseItem) -> None:
        """Create or update an item in the database."""
        await self.update(item)

    @router.delete("/{collection}/{identifier}")
    async def delete_item(self, class_type: Type[T], identifier: PyObjectId) -> None:
        """Delete an item from the database."""
        await self.delete(class_type, identifier)

    @router.get("/{collection}/")
    async def list_items(self, class_type: Type[T]) -> Dict[str, T]:
        """List all items in a collection."""
        return await self.get_all(class_type)

    @router.get("/{collection}/find/")
    async def find_items(self, class_type: Type[T], **kwargs: str) -> Optional[Dict[PyObjectId, T]]:
        """Find items in a collection matching the filter criteria."""
        return await self.find(class_type, **kwargs)

    @router.get("/{collection}/find_one/")
    async def find_one_item(self, class_type: Type[T], **kwargs: str) -> Optional[T]:
        """Find one item in a collection matching the filter criteria."""
        return await self.find_one(class_type, **kwargs)


class DatabaseError(Exception):
    """Errors related to database operations."""


class UnknownEntityError(DatabaseError):
    """Requested entity does not exist."""
