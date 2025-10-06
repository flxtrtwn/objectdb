"""Tests for database module."""

import pytest

from objectdb.database import DatabaseItem, ForeignKey, UnknownEntityError
from objectdb.backends.dict import DictDatabase


class Customer(DatabaseItem):
    """Sample item for testing."""

    name: str
    city: str


class Product(DatabaseItem):
    """Sample item for testing."""

    name: str
    customer: ForeignKey[Customer]


@pytest.mark.asyncio
async def test_add_item() -> None:
    """Test adding item to database."""
    test_item = Customer(name="name", city="city")
    db = DictDatabase()
    await db.update(test_item)
    assert await db.get(Customer, test_item.identifier)


@pytest.mark.asyncio
async def test_update_item() -> None:
    """Test updating item in database."""
    test_item = Customer(name="name", city="city")
    db = DictDatabase()
    await db.update(test_item)
    item_to_change = await db.get(Customer, test_item.identifier)
    item_to_change.city = "changed_city"
    await db.update(item_to_change)
    updated_item = await db.get(Customer, test_item.identifier)
    assert updated_item.city == "changed_city"


@pytest.mark.asyncio
async def test_delete_item() -> None:
    """Test deleting items."""
    test_item = Customer(name="name", city="city")
    db = DictDatabase()
    await db.update(test_item)
    assert await db.get(type(test_item), test_item.identifier)
    await db.delete(type(test_item), test_item.identifier)
    with pytest.raises(UnknownEntityError):
        await db.get(type(test_item), test_item.identifier)


@pytest.mark.skip(reason="Not implemented yet")
@pytest.mark.asyncio
async def test_cascading_delete() -> None:
    """Test cascading delete of items with foreign keys."""
    customer = Customer(name="name", city="city")
    db = DictDatabase()
    await db.update(customer)
    product = Product(name="product", customer=customer)  # type: ignore
    await db.update(product)
    await db.delete(type(customer), customer.identifier)
    with pytest.raises(UnknownEntityError):
        await db.get(type(product), product.identifier)
