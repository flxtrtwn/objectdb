"""Task scheduling based on database objects."""

from __future__ import annotations

import logging
from datetime import datetime
from enum import Enum
from zoneinfo import ZoneInfo

import pydantic

import objectdb
import objectdb.database

logger = logging.getLogger(__name__)


class Frequency(str, Enum):
    """Temporal frequency."""

    ONCE = "once"
    WEEKLY = "weekly"
    DAILY = "daily"

    def __str__(self) -> str:
        """Return literal value."""
        return self._value_  # pylint: disable=E1101


class Task(objectdb.database.DatabaseItem):
    """Database task item with information about its next execution."""

    frequency: Frequency
    next_run: datetime
    created_at: datetime = datetime.now(tz=ZoneInfo("UTC"))

    @pydantic.field_validator("next_run", "created_at")
    @classmethod
    def localize_datetime(cls, value: datetime) -> datetime:
        """Ensure timezone is attached after database operations."""
        if value.tzinfo is None:
            return value.replace(tzinfo=ZoneInfo("UTC"))
        return value

    async def process(self) -> None:
        """Process task."""
