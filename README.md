# About

`objectdb` aims to reduce mental load for database integration, especially for developers thinking in "objects".
Users can persist their domain-specific objects by inheriting from `objectdb.database.DatabaseItem`.
Utilizing the power of `pydantic` and `fastapi`, they can then perform validated CRUD operations either directly or via an automatically generated REST API layer, using any database implementation from `objectdb.backends`.

# Example

This is an exemplary `fastapi` app.
If a REST API layer is required, the classes used in production must be known at runtime, for the endpoints to be defined.
Otherwise, the operations that `database.Database` provides can be used directly.

    import fastapi

    from objectdb import database
    from objectdb.backends import dictionary


    class User(database.DatabaseItem):
        """Simple user."""

        name: str


    app = fastapi.FastAPI()
    db = dictionary.DictDatabase()
    app.include_router(database.create_api_router(db, [User]))

## Direct access

You can use the database layer directly in your code.
However, for performance reasons, the implementation uses [`asyncio`](https://docs.python.org/3/library/asyncio.html).
If you embed database calls in your code, you probably should, too.

    users_with_name_joe: list[User] = await db.find(User, {"name": "Joe"})

## REST access

You may run an API server in a containerized environment or public-facing.
When you have a lot of I/O in your code, you might consider using `httpx.AsyncClient`as in this example.
Of course it is fine to just use `requests`.

    from objectdb import database

    API_URL = "http://some-url"

    # The endpoint is /user but you can construct it programmatically, too
    user_endpoint = f"{API_URL}/{User.__name__.lower()}

    users_with_name_joe: list[User] = []

    try:
        response = requests.get(url, params={"name": "Joe"}, timeout=TIMEOUT)
        response.raise_for_status()
    except requests.HTTPError as exc:
        if response.status_code == 404:
            raise database.UnknownEntityError from exc
        raise
    except requests.RequestException as exc:
        raise ValueError("Unsuccessful database request.") from exc
    else:
        users_with_name_joe = [User.model_validate(element) for element in response.json()]
