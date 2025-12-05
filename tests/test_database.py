"""Tests for thedatabase implementation."""

import http

import fastapi
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from objectdb.database import Database, PydanticObjectId, UnknownEntityError

from .mocks import Administrator, User

# ruff: noqa: S101


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
        new_identifier = await db.upsert(user)
        # THEN it can be retrieved by its identifier
        assert new_identifier
        fetched = await db.get(User, identifier=new_identifier)
        assert fetched is not None
        assert fetched.name == "Alice"
        assert fetched.identifier == user.identifier

    @pytest.mark.asyncio
    async def test_update_existing(self, db: Database) -> None:
        """Test updating an existing item."""
        # GIVEN a user in the database
        user = User(name="Bob", email="box@example.com")
        await db.upsert(user)
        # WHEN updating the user's email
        user.email = "bob@example.com"
        new_identifier = await db.upsert(user)
        # THEN the change is reflected in the database and no new identifier is returned
        assert new_identifier is None
        fetched = await db.get(User, identifier=user.identifier)
        assert fetched is not None
        assert fetched.email == "bob@example.com"


class TestGetting:
    """Tests for getting items from the  database."""

    @pytest.mark.asyncio
    async def test_get_unknown(self, db: Database) -> None:
        """Test retrieving an unknown item raises an error."""
        # GIVEN a user that does not exist in the database
        user = User(name="Dave", email="dave@example.com")
        # WHEN trying to get a user with a random identifier
        # THEN an UnknownEntityError is raised
        with pytest.raises(UnknownEntityError):
            assert await db.get(User, identifier=user.identifier) is None


class TestFinding:
    """Tests for finding items in the  database."""

    @pytest.mark.asyncio
    async def test_find_users(self, db: Database) -> None:
        """Test finding users by attribute."""
        # GIVEN multiple users in the database
        user1 = User(name="Eve", email="eve@example.com")
        user2 = User(name="Frank", email="frank@example.com")
        await db.upsert(user1)
        await db.upsert(user2)
        # WHEN finding users by name
        results = await db.find(User, name="Eve")
        # THEN only the matching user is returned
        assert results == [user1]

    @pytest.mark.asyncio
    async def test_find_inherited_users(self, db: Database) -> None:
        """Test finding inherited users by attribute."""
        # GIVEN multiple users and administrators in the database
        admin1 = Administrator(name="Peter", email="peter@example.com", needs_pw_rotation=False)
        user1 = User(name="Eve", email="eve@example.com")
        user2 = User(name="Frank", email="frank@example.com")
        await db.upsert(user1)
        await db.upsert(user2)
        await db.upsert(admin1)
        # WHEN finding an item of an inherited class by name
        results = await db.find_inherited(User, name="Peter")
        # THEN only the matching admin is returned
        assert results == [admin1]


class TestDeleting:
    """Tests for deleting items from the  database."""

    @pytest.mark.asyncio
    async def test_delete_existing(self, db: Database) -> None:
        """Test deleting an item."""
        # GIVEN a user in the database
        user = User(name="Charlie", email="charlie@example.com")
        await db.upsert(user)
        assert await db.get(User, identifier=user.identifier)
        # WHEN deleting the user
        await db.delete(type(user), user.identifier)
        # THEN the user can no longer be retrieved
        with pytest.raises(UnknownEntityError):
            assert await db.get(User, identifier=user.identifier)

    @pytest.mark.asyncio
    async def test_delete_inherited(self, db: Database) -> None:
        """Test deleting an item."""
        # GIVEN a user in the database
        user = User(name="Charlie", email="charlie@example.com")
        admin = Administrator(name="Dana", email="dana@example.com", needs_pw_rotation=True)
        await db.upsert(user)
        await db.upsert(admin)
        assert await db.get(Administrator, identifier=admin.identifier)
        # WHEN deleting the admin as user
        await db.delete(User, admin.identifier)
        # THEN the admin can no longer be retrieved
        with pytest.raises(UnknownEntityError):
            assert await db.get(Administrator, identifier=admin.identifier)

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
        app.include_router(db.create_api_router())
        return TestClient(app)

    @pytest.mark.asyncio
    async def test_get(self, client: TestClient, db: Database) -> None:
        """Test the get endpoint."""
        # GIVEN a user in the database
        user = User(name="Jack", email="jack@example.com")
        assert isinstance(user.identifier, PydanticObjectId)
        await db.upsert(user)

        # WHEN requesting the user by ID
        response = client.get(f"/user/{user.identifier}")
        # THEN the correct user is returned
        assert response.status_code == http.HTTPStatus.OK
        assert user == User(**response.json())

    @pytest.mark.asyncio
    async def test_get_not_found(self, client: TestClient) -> None:
        """Test getting non-existent user returns 404."""
        # GIVEN no users in the database
        # WHEN requesting a user by a random ID
        response = client.get("/user/507f1f77bcf86cd799439011")
        # THEN a 404 error is returned
        assert response.status_code == http.HTTPStatus.NOT_FOUND

    @pytest.mark.asyncio
    async def test_create_user(self, client: TestClient, db: Database) -> None:
        """Test creating a new user via POST."""
        # GIVEN a user
        user = User(name="Alice", email="alice@example.com")
        # WHEN creating a new user
        response = client.post("/user", json=user.model_dump())
        # THEN the response should be the new identifier and the user should be in the database
        assert response.status_code == http.HTTPStatus.OK
        assert PydanticObjectId(response.json()) == user.identifier
        assert user == await db.get(User, user.identifier)

    @pytest.mark.asyncio
    async def test_update_user(self, client: TestClient, db: Database) -> None:
        """Test updating an existing user via POST."""
        # GIVEN an existing user
        user = User(name="Bob", email="bob@example.com")
        await db.upsert(user)

        # WHEN updating the user
        user.email = "bob2@example.com"
        response = client.post("/user", json=user.model_dump(mode="json"))

        # THEN response should be null and the database should reflect changes
        assert response.status_code == http.HTTPStatus.OK
        assert response.text == "null"
        fetched = await db.get(User, user.identifier)
        assert fetched is not None
        assert fetched.email == "bob2@example.com"

    @pytest.mark.asyncio
    async def test_delete_user(self, client: TestClient, db: Database) -> None:
        """Test deleting a user."""
        # GIVEN an existing user
        user = User(name="Carol", email="carol@example.com")
        await db.upsert(user)

        # WHEN deleting the user
        response = client.delete(f"/user/{user.identifier}")

        # THEN response should be successful
        assert response.status_code == http.HTTPStatus.OK

        # AND user should not exist
        get_response = client.get(f"/user/{user.identifier}")
        assert get_response.status_code == http.HTTPStatus.NOT_FOUND

    @pytest.mark.asyncio
    async def test_get_all_users(self, client: TestClient, db: Database) -> None:
        """Test getting all users."""
        # GIVEN multiple users in database
        user1 = User(name="Dave", email="dave@example.com")
        user2 = User(name="Eve", email="eve@example.com")
        await db.upsert(user1)
        await db.upsert(user2)

        # WHEN getting all users
        response = client.get("/user")

        # THEN response should include all users
        assert response.status_code == http.HTTPStatus.OK
        users = [User.model_validate(user) for user in list(response.json())]
        assert user1 in users
        assert user2 in users

    @pytest.mark.asyncio
    async def test_find_users(self, client: TestClient, db: Database) -> None:
        """Test finding users by criteria."""
        # GIVEN users in database
        user1 = User(name="Frank", email="frank@example.com")
        user2 = User(name="Grace", email="grace@example.com")
        await db.upsert(user1)
        await db.upsert(user2)

        # WHEN searching for specific user
        response = client.get("/user", params={"name": "Frank"})

        # THEN response should include matching user
        assert response.status_code == http.HTTPStatus.OK
        data = response.json()
        assert len(data) == 1
        found_user = next(iter(data))
        assert found_user["name"] == "Frank"
        assert found_user["email"] == "frank@example.com"

    @pytest.mark.asyncio
    async def test_find_inherited_users(self, client: TestClient, db: Database) -> None:
        """Test finding inherited users by criteria."""
        # GIVEN users in database
        user1 = User(name="Frank", email="frank@example.com")
        user2 = User(name="Grace", email="grace@example.com")
        admin1 = Administrator(name="Peter", email="peter@example.com", needs_pw_rotation=True)
        await db.upsert(user1)
        await db.upsert(user2)
        await db.upsert(admin1)

        # WHEN searching for specific user
        response = client.get("/user", params={"name": "Peter", "inherited": "true"})

        # THEN response should include matching administrator
        assert response.status_code == http.HTTPStatus.OK
        data = response.json()
        assert len(data) == 1
        found_user = next(iter(data))
        assert found_user["name"] == "Peter"
        assert found_user["email"] == "peter@example.com"
        assert found_user["needs_pw_rotation"] is True
        assert found_user["_type"] == "Administrator"

    @pytest.mark.asyncio
    async def test_delete_inherited_users(self, client: TestClient, db: Database) -> None:
        """Test deleting inherited users."""
        # GIVEN users in database
        user1 = User(name="Frank", email="frank@example.com")
        user2 = User(name="Grace", email="grace@example.com")
        admin1 = Administrator(name="Peter", email="peter@example.com", needs_pw_rotation=True)
        await db.upsert(user1)
        await db.upsert(user2)
        await db.upsert(admin1)

        # WHEN deleting an admin via the user endpoint
        response = client.delete(f"/user/{admin1.identifier}")

        # THEN admin should be deleted
        assert response.status_code == http.HTTPStatus.OK
        get_response = client.get(f"/user/{admin1.identifier}")
        assert get_response.status_code == http.HTTPStatus.NOT_FOUND
