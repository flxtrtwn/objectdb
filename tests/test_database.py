"""Tests for thedatabase implementation."""

import fastapi
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from objectdb.database import Database, DatabaseItem, PydanticObjectId, UnknownEntityError, create_api_router


class User(DatabaseItem):
    """Test user entity."""

    name: str
    email: str


class TestUpdating:
    """Tests for updating (and inserting) items into the database."""

    @pytest.mark.asyncio
    async def test_insert_non_existing(self, db: Database) -> None:
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
    async def test_update_existing(self, db: Database) -> None:
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
    """Tests for getting items from the  database."""

    @pytest.mark.asyncio
    async def test_get_unknown(self, db: Database) -> None:
        """Test retrieving an unknown item raises an error."""
        # GIVEN a user that does not exist in the database
        user = User(name="Dave", email="dave@example.com")
        # WHEN trying to get a user with a random identifier
        with pytest.raises(UnknownEntityError):
            await db.get(User, identifier=user.identifier)


class TestFinding:
    """Tests for finding items in the  database."""

    @pytest.mark.asyncio
    async def test_find_users(self, db: Database) -> None:
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
    async def test_find_one_user(self, db: Database) -> None:
        """Test finding a single user by attribute."""
        # GIVEN two users in the database
        user = User(name="Grace", email="grace@example.com")
        other_user = User(name="Heidi", email="heidi@example.com")
        await db.update(user)
        await db.update(other_user)
        # WHEN finding the user by name
        result = await db.find_one(User, name="Grace")
        # THEN the correct user is returned
        assert result == user

    @pytest.mark.asyncio
    async def test_find_one_user_not_found(self, db: Database) -> None:
        """Test finding a single user that does not exist."""
        # GIVEN no users in the database
        # WHEN finding a user by name that does not exist
        # THEN None is returned
        assert await db.find_one(User, name="NonExistentUser") is None


class TestDeleting:
    """Tests for deleting items from the  database."""

    @pytest.mark.asyncio
    async def test_delete_existing(self, db: Database) -> None:
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
    async def test_delete_unknown(self, db: Database) -> None:
        """Test deleting an unknown item raises an error."""
        # GIVEN a user that does not exist in the database
        user = User(name="Ivan", email="ivan@example.com")
        # WHEN trying to delete the user
        # THEN an UnknownEntityError is raised
        with pytest.raises(UnknownEntityError):
            await db.delete(User, user.identifier)


class TestEndpoints:
    """Tests for the FastAPI endpoints provided by the database."""

    @pytest_asyncio.fixture
    async def client(self, db: Database) -> TestClient:
        """Create a FastAPI app with the database router included."""
        app = fastapi.FastAPI()
        app.include_router(create_api_router(db, [User]))
        return TestClient(app)

    @pytest.mark.asyncio
    async def test_get(self, client: TestClient, db: Database) -> None:
        """Test the get endpoint."""
        # GIVEN a user in the database
        user = User(name="Jack", email="jack@example.com")
        assert isinstance(user.identifier, PydanticObjectId)
        await db.update(user)

        # WHEN requesting the user by ID
        response = client.get(f"/user/{user.identifier}")
        # THEN the correct user is returned
        assert response.status_code == 200
        assert user == User(**response.json())

    @pytest.mark.asyncio
    async def test_get_not_found(self, client: TestClient) -> None:
        """Test getting non-existent user returns 404."""
        # GIVEN no users in the database
        # WHEN requesting a user by a random ID
        response = client.get("/user/507f1f77bcf86cd799439011")
        # THEN a 404 error is returned with item not found detail
        assert response.status_code == 404
        assert response.json()["detail"] == "Item not found"

    @pytest.mark.asyncio
    async def test_create_user(self, client: TestClient, db: Database) -> None:
        """Test creating a new user via POST."""
        # GIVEN a user
        user = User(name="Alice", email="alice@example.com")
        # WHEN creating a new user
        response = client.post("/user/", json=user.model_dump())
        # THEN the user should be in the database
        assert response.status_code == 200
        assert user == await db.get(User, user.identifier)

    @pytest.mark.asyncio
    async def test_update_user(self, client: TestClient, db: Database) -> None:
        """Test updating an existing user via POST."""
        # GIVEN an existing user
        user = User(name="Bob", email="bob@example.com")
        await db.update(user)

        # WHEN updating the user
        user.email = "bob2@example.com"
        response = client.post("/user/", json=user.model_dump())

        # THEN response should reflect changes
        assert response.status_code == 200
        assert (await db.get(User, user.identifier)).email == "bob2@example.com"

    @pytest.mark.asyncio
    async def test_delete_user(self, client: TestClient, db: Database) -> None:
        """Test deleting a user."""
        # GIVEN an existing user
        user = User(name="Carol", email="carol@example.com")
        await db.update(user)

        # WHEN deleting the user
        response = client.delete(f"/user/{user.identifier}")

        # THEN response should be successful
        assert response.status_code == 200

        # AND user should not exist
        get_response = client.get(f"/user/{user.identifier}")
        assert get_response.status_code == 404

    @pytest.mark.asyncio
    async def test_get_all_users(self, client: TestClient, db: Database) -> None:
        """Test getting all users."""
        # GIVEN multiple users in database
        user1 = User(name="Dave", email="dave@example.com")
        user2 = User(name="Eve", email="eve@example.com")
        await db.update(user1)
        await db.update(user2)

        # WHEN getting all users
        response = client.get("/user/")

        # THEN response should include all users
        assert response.status_code == 200
        users = [User.model_validate(user) for user in dict(response.json()).values()]
        assert user1 in users
        assert user2 in users

    @pytest.mark.asyncio
    async def test_find_users(self, client: TestClient, db: Database) -> None:
        """Test finding users by criteria."""
        # GIVEN users in database
        user1 = User(name="Frank", email="frank@example.com")
        user2 = User(name="Grace", email="grace@example.com")
        await db.update(user1)
        await db.update(user2)

        # WHEN searching for specific user
        response = client.get("/user/find/", params={"name": "Frank"})

        # THEN response should include matching user
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        found_user = next(iter(data.values()))
        assert found_user["name"] == "Frank"
        assert found_user["email"] == "frank@example.com"

    @pytest.mark.asyncio
    async def test_find_one_user(self, client: TestClient, db: Database) -> None:
        """Test finding one user by criteria."""
        # GIVEN a user in database
        user = User(name="Helen", email="helen@example.com")
        await db.update(user)

        # WHEN finding the user
        response = client.get("/user/find_one/", params={"name": "Helen"})

        # THEN response should have correct user
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Helen"
        assert data["email"] == "helen@example.com"

    @pytest.mark.asyncio
    async def test_find_one_not_found(self, client: TestClient) -> None:
        """Test find_one returns null for non-existent user."""
        response = client.get("/user/find_one/", params={"name": "NonExistent"})
        assert response.status_code == 200
        assert response.json() is None
