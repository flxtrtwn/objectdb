"""Tests for the MongoDB database implementation."""

import pytest

from objectdb.database import DatabaseItem
from objectdb.backends.mongodb import MongoDBDatabase, UnknownEntityError


class User(DatabaseItem):
    """Test user entity."""

    name: str
    email: str


class TestUpdating:
    """Tests for updating (and inserting) items into the MongoDB database."""

    @pytest.mark.asyncio
    async def test_insert_non_existing(self, db: MongoDBDatabase) -> None:
        """Test inserting and retrieving an item."""
        # GIVEN a user not existing in the database
        user = User(name="Alice", email="alice@example.com")
        with pytest.raises(UnknownEntityError):
            await db.get(User, identifier=user.identifier)
        # WHEN inserting it into the database
        await db.update(user)
        # THEN it can be retrieved by its identifier
        fetched = await db.get(User, identifier=user.identifier)
        assert fetched.name == "Alice"
        assert fetched.identifier == user.identifier

    @pytest.mark.asyncio
    async def test_update_existing(self, db: MongoDBDatabase) -> None:
        """Test updating an existing item."""
        # GIVEN a user in the database
        user = User(name="Bob", email="box@example.com")
        await db.update(user)
        # WHEN updating the user's email
        user.email = "bob@example.com"
        await db.update(user)
        # THEN the change is reflected in the database
        fetched = await db.get(User, identifier=user.identifier)
        assert fetched.email == "bob@example.com"


class TestGetting:
    """Tests for getting items from the MongoDB database."""

    @pytest.mark.asyncio
    async def test_get_unknown(self, db: MongoDBDatabase) -> None:
        """Test retrieving an unknown item raises an error."""
        # GIVEN a user that does not exist in the database
        user = User(name="Dave", email="dave@example.com")
        # WHEN trying to get a user with a random identifier
        with pytest.raises(UnknownEntityError):
            await db.get(User, identifier=user.identifier)


class TestFinding:
    """Tests for finding items in the MongoDB database."""

    @pytest.mark.asyncio
    async def test_find_users(self, db: MongoDBDatabase) -> None:
        """Test finding users by attribute."""
        # GIVEN multiple users in the database
        user1 = User(name="Eve", email="eve@example.com")
        user2 = User(name="Frank", email="frank@example.com")
        await db.update(user1)
        await db.update(user2)
        # WHEN finding users by name
        results = await db.find(User, name="Eve")
        # THEN only the matching user is returned
        assert results == {user1.identifier: user1}

    @pytest.mark.asyncio
    async def test_find_one_user(self, db: MongoDBDatabase) -> None:
        """Test finding a single user by attribute."""
        # GIVEN two users in the database
        user = User(name="Grace", email="grace@example.com")
        User(name="Heidi", email="heidi@example.com")
        await db.update(user)
        # WHEN finding the user by name
        result = await db.find_one(User, name="Grace")
        # THEN the correct user is returned
        assert result == user

    @pytest.mark.asyncio
    async def test_find_one_user_not_found(self, db: MongoDBDatabase) -> None:
        """Test finding a single user that does not exist."""
        # GIVEN no users in the database
        # WHEN finding a user by name that does not exist
        # THEN None is returned
        assert await db.find_one(User, name="NonExistentUser") is None


class TestDeleting:
    """Tests for deleting items from the MongoDB database."""

    @pytest.mark.asyncio
    async def test_delete_existing(self, db: MongoDBDatabase) -> None:
        """Test deleting an item."""
        # GIVEN a user in the database
        user = User(name="Charlie", email="charlie@example.com")
        await db.update(user)
        assert await db.get(User, identifier=user.identifier)
        # WHEN deleting the user
        await db.delete(type(user), user.identifier)
        # THEN the user can no longer be retrieved
        with pytest.raises(UnknownEntityError):
            await db.get(User, identifier=user.identifier)

    @pytest.mark.asyncio
    async def test_delete_unknown(self, db: MongoDBDatabase) -> None:
        """Test deleting an unknown item raises an error."""
        # GIVEN a user that does not exist in the database
        user = User(name="Ivan", email="ivan@example.com")
        # WHEN trying to delete the user
        # THEN an UnknownEntityError is raised
        with pytest.raises(UnknownEntityError):
            await db.delete(User, user.identifier)
