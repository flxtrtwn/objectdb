"""Database abstraction layer."""

from __future__ import annotations

import operator
from abc import ABC, abstractmethod
from functools import reduce
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
    type: str = pydantic.Field(init=False, alias="_type", default_factory=lambda: "DatabaseItem")

    def model_post_init(self, _) -> None:  # pylint: disable=arguments-differ #noqa: ANN001
        """Set _type after initialization."""
        # Automatically assign class name
        object.__setattr__(self, "type", self.__class__.__name__)

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
            cls_name = class_type.__name__.lower()

            def make_get_route(cls_type: type[DatabaseItem], cls_name: str):  # noqa: ANN202
                async def get_item(identifier: PydanticObjectId) -> DatabaseItem:
                    try:
                        return await self.get(cls_type, identifier)
                    except UnknownEntityError as exc:
                        raise fastapi.HTTPException(status_code=404, detail="Item not found") from exc

                return get_item, {  # type: ignore
                    "path": f"/{cls_name}/{{identifier}}",
                    "endpoint": get_item,
                    "response_model": cls_type,
                    "methods": ["GET"],
                }

            def make_upsert_route(cls_type: type[DatabaseItem], cls_name: str):  # noqa: ANN202
                async def upsert_item(request: fastapi.Request) -> PydanticObjectId | None:
                    data = await request.json()
                    return await self.upsert(cls_type.model_validate(data))

                return upsert_item, {"path": f"/{cls_name}", "endpoint": upsert_item, "methods": ["POST"]}  # type: ignore

            def make_delete_route(cls_type: type[DatabaseItem], cls_name: str):  # noqa: ANN202
                async def delete_item(identifier: str) -> None:
                    try:
                        await self.delete(cls_type, PydanticObjectId(identifier))
                    except UnknownEntityError as exc:
                        raise fastapi.HTTPException(status_code=404, detail="Item not found") from exc

                return delete_item, {  # type: ignore
                    "path": f"/{cls_name}/{{identifier}}",
                    "endpoint": delete_item,
                    "methods": ["DELETE"],
                }

            def make_find_route(cls_type: type[DatabaseItem], cls_name: str):  # noqa: ANN202
                async def find_items(request: fastapi.Request) -> list[DatabaseItem]:
                    if request.query_params.get("inherited") == "true":
                        params = {k: v for k, v in request.query_params.items() if k != "inherited"}
                        return await self.find_inherited(cls_type, **params)
                    return await self.find(cls_type, **request.query_params)

                possible_response_models = [
                    possible_subtype
                    for possible_subtype in self.supported_types
                    if issubclass(possible_subtype, cls_type)
                ]

                return find_items, {  # type: ignore
                    "path": f"/{cls_name}",
                    "endpoint": find_items,
                    "response_model": list[reduce(operator.or_, possible_response_models)],
                    "methods": ["GET"],
                }

            for factory in [make_get_route, make_upsert_route, make_delete_route, make_find_route]:  # type: ignore
                _, kwargs = factory(class_type, cls_name)  # type: ignore
                router.add_api_route(**kwargs)

        return router


class DatabaseError(Exception):
    """Errors related to database operations."""


class UnknownEntityError(DatabaseError):
    """Requested entity does not exist."""
