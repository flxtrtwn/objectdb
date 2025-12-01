"""Database abstraction layer."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, TypeVar

import fastapi
import pydantic
from bson.objectid import ObjectId
from pydantic_core import core_schema

T = TypeVar("T", bound="DatabaseItem")


class PydanticObjectId(ObjectId):
    """Custom ObjectId type for Pydantic v2 compatibility."""

    @classmethod
    def __get_pydantic_core_schema__(
        cls, _source_type: Any, _handler: pydantic.GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """Core schema for pydantic, serialize to string."""
        return core_schema.no_info_after_validator_function(
            cls.validate, core_schema.any_schema(), serialization=core_schema.plain_serializer_function_ser_schema(str)
        )

    @classmethod
    def validate(cls, value: Any) -> PydanticObjectId:
        """Validate PydanticObjectId, accepting strings and ObjectIds."""
        if isinstance(value, ObjectId):
            return cls(value)
        if isinstance(value, str) and ObjectId.is_valid(value):
            return cls(value)
        raise ValueError(f"Invalid ObjectId: {value}")

    def __eq__(self, other: object) -> bool:
        """Act as string."""
        if isinstance(other, str):
            return str(self) == other
        return super().__eq__(other)

    def __hash__(self) -> int:
        """Act as ObjectId."""
        return super().__hash__()

    def __repr__(self) -> str:
        """Pydantic-specific ObjectId."""
        return "Pydantic" + super().__repr__()


class DatabaseItem(ABC, pydantic.BaseModel):
    """Base class for database items."""

    model_config = pydantic.ConfigDict(
        revalidate_instances="always", validate_assignment=True, populate_by_name=True, from_attributes=True
    )

    identifier: PydanticObjectId = pydantic.Field(alias="_id", default_factory=PydanticObjectId)

    def __eq__(self, other: object) -> bool:
        """Compare identifiers."""
        if not isinstance(other, DatabaseItem):
            return NotImplemented
        return self.identifier == other.identifier

    def __hash__(self) -> int:
        """Hash identifier."""
        return hash(self.identifier)


class Database(ABC):
    """Database abstraction."""

    def __init__(self, supported_types: list[type[DatabaseItem]]) -> None:
        """Ensure supported types are known at runtime."""
        self.supported_types = supported_types

    @abstractmethod
    async def upsert(self, item: DatabaseItem) -> PydanticObjectId | None:
        """Update entity if it exists or create it otherwise.

        If a new entity was created, return its identifier.
        """

    @abstractmethod
    async def get(self, class_type: type[T], identifier: PydanticObjectId) -> T:
        """Return entity if it exists or raise UnknownEntityError otherwise."""

    @abstractmethod
    async def delete(self, class_type: type[T], identifier: PydanticObjectId, *, cascade: bool = False) -> None:
        """Delete entity, raise UnknownEntityError if entity does not exist."""

    @abstractmethod
    async def find(self, class_type: type[T], **kwargs: str) -> list[T]:
        """Return all entities of collection matching the filter criteria."""

    @abstractmethod
    async def find_inherited(self, class_type: type[T], **kwargs: str) -> list[T]:
        """Return all entities of collection or inherited collections matching the filter criteria."""

    @abstractmethod
    async def close(self) -> None:
        """Close database connection."""

    @abstractmethod
    async def purge(self) -> None:
        """Purge all collections in the database."""

    def create_api_router(self) -> fastapi.APIRouter:  # noqa: C901
        """Create a FastAPI router for the database."""
        router = fastapi.APIRouter()

        for class_type in self.supported_types:
            class_name = class_type.__name__.lower()

            def create_get_item(cls_name: str, cls_type: type[DatabaseItem]):  # noqa: ANN202
                """Create get_item function."""

                @router.get(f"/{cls_name}/{{identifier}}", response_model=cls_type)
                async def get_item(identifier: PydanticObjectId) -> cls_type:  # type: ignore
                    """Get a single item by ID."""
                    try:
                        return await self.get(cls_type, identifier)
                    except UnknownEntityError as exc:
                        raise fastapi.HTTPException(status_code=404, detail="Item not found") from exc

                return get_item  # type: ignore

            def create_upsert_item(cls_name: str, cls_type: type[DatabaseItem]):  # noqa: ANN202
                """Create upsert_item function."""

                @router.post(f"/{cls_name}")
                async def upsert_item(request: fastapi.Request) -> PydanticObjectId | None:
                    data = await request.json()
                    return await self.upsert(cls_type.model_validate(data))

                return upsert_item

            def create_delete_item(cls_name: str, cls_type: type[DatabaseItem]):  # noqa: ANN202
                """Create delete_item function."""

                @router.delete(f"/{cls_name}/{{identifier}}")
                async def delete_item(identifier: str) -> None:
                    """Delete an item by ID."""
                    try:
                        await self.delete(cls_type, PydanticObjectId(identifier))
                    except UnknownEntityError as exc:
                        raise fastapi.HTTPException(status_code=404, detail="Item not found") from exc

                return delete_item

            def create_find(cls_name: str, cls_type: type[DatabaseItem]):  # noqa: ANN202
                """Create find_item function."""

                @router.get(f"/{cls_name}", response_model=list[cls_type])
                async def find(request: fastapi.Request) -> list[DatabaseItem]:
                    """Find items by criteria."""
                    if request.query_params.get("inherited") == "true":
                        model_params = {key: value for key, value in request.query_params.items() if key != "inherited"}
                        return await self.find_inherited(cls_type, **model_params)
                    return await self.find(cls_type, **request.query_params)

                return find

            create_get_item(class_name, class_type)
            create_upsert_item(class_name, class_type)
            create_delete_item(class_name, class_type)
            create_find(class_name, class_type)

        return router


class DatabaseError(Exception):
    """Errors related to database operations."""


class UnknownEntityError(DatabaseError):
    """Requested entity does not exist."""
