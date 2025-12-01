"""Mock classes for tests."""

from objectdb.database import DatabaseItem


class User(DatabaseItem):
    """Test user entity."""

    name: str
    email: str


class Administrator(User):
    """Test administrator entity."""

    needs_pw_rotation: bool
