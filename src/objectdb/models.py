"""Customer data-handling."""

from __future__ import annotations

import logging
from zoneinfo import ZoneInfo

import pydantic

from objectdb.database import DatabaseItem, PydanticObjectId

logger = logging.getLogger(__name__)


class Product(DatabaseItem):
    """Product information."""

    name: str
    customer_id: PydanticObjectId
    description: str
    price: float

    def __str__(self) -> str:
        """Return product information."""
        return f"A product costing {self.price} with name {self.name} and description: {self.description}"


class Customer(DatabaseItem):
    """Customer profile."""

    mail: str
    type: str
    name: str
    timezone: ZoneInfo = ZoneInfo("UTC")

    @pydantic.field_serializer("timezone")
    def encode_timezone(self, tz: ZoneInfo) -> str:
        """Ensure timezone is a string in DB."""
        return tz.key

    def __str__(self) -> str:
        """Return customer information."""
        return f"A customer with name {self.name}"
