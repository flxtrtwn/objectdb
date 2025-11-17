"""Database abstraction layer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

import pydantic
from pydantic_core import core_schema

if TYPE_CHECKING:
    from objectdb.database import DatabaseItem


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
