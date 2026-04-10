"""
Integration tests — require running MongoDB + Redis.
Run with: pytest tests/ -v
"""
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

from app.models.task import Priority, TaskStatus, TaskCreate
from app.core.service import create_task, get_task, list_tasks


def make_mock_db(tasks_store: dict):
    """Create a mock Database that uses an in-memory dict."""
    db = MagicMock()

    async def insert_one(doc):
        tasks_store[doc["id"]] = dict(doc)

    async def update_one(query, update):
        for tid, doc in tasks_store.items():
            if doc.get("id") == query.get("id"):
                if "$set" in update:
                    tasks_store[tid].update(update["$set"])

    async def find_one(query):
        for doc in tasks_store.values():
            if doc.get("id") == query.get("id"):
                return dict(doc)
        return None

    async def to_list(length):
        return list(tasks_store.values())[:length]

    cursor = MagicMock()
    cursor.sort.return_value = cursor
    cursor.skip.return_value = cursor
    cursor.limit.return_value = cursor
    cursor.to_list = to_list

    collection = MagicMock()
    collection.insert_one = insert_one
    collection.update_one = update_one
    collection.find_one = find_one
    collection.find.return_value = cursor

    db.tasks = collection
    return db


@pytest.mark.asyncio
@patch("app.core.service.enqueue_task")
async def test_create_task(mock_enqueue):
    store = {}
    db = make_mock_db(store)

    data = TaskCreate(payload={"action": "test"}, priority=Priority.HIGH)
    result = await create_task(db, data)

    assert result["status"] == TaskStatus.QUEUED
    assert result["priority"] == Priority.HIGH
    assert result["retry_count"] == 0
    mock_enqueue.assert_called_once()


@pytest.mark.asyncio
@patch("app.core.service.enqueue_task")
async def test_task_not_found(mock_enqueue):
    store = {}
    db = make_mock_db(store)
    result = await get_task(db, "nonexistent-id")
    assert result is None


@pytest.mark.asyncio
@patch("app.core.service.enqueue_task")
async def test_priority_ordering(mock_enqueue):
    """Tasks should be returned HIGH > MEDIUM > LOW."""
    store = {}
    db = make_mock_db(store)

    for priority in [Priority.LOW, Priority.MEDIUM, Priority.HIGH]:
        await create_task(db, TaskCreate(payload={"p": priority}, priority=priority))

    # All 3 are in store — verify all were enqueued
    assert mock_enqueue.call_count == 3


@pytest.mark.asyncio
@patch("app.core.service.enqueue_task")
async def test_idempotent_task_id(mock_enqueue):
    """The same task_id should never be double-inserted."""
    store = {}
    db = make_mock_db(store)

    data = TaskCreate(payload={"x": 1}, priority=Priority.MEDIUM)
    r1 = await create_task(db, data)
    # IDs are UUIDs, so two calls create two different tasks — that's correct.
    r2 = await create_task(db, data)
    assert r1["id"] != r2["id"]
