from datetime import datetime, timezone
from typing import Any

import motor.motor_asyncio
from pymongo import ASCENDING, DESCENDING, IndexModel

from app.config import settings
from app.models.task import Priority, TaskStatus

# Priority sort weight for MongoDB queries (lower = higher priority)
PRIORITY_WEIGHT = {
    Priority.HIGH: 1,
    Priority.MEDIUM: 2,
    Priority.LOW: 3,
}


class Database:
    client: motor.motor_asyncio.AsyncIOMotorClient | None = None
    db: motor.motor_asyncio.AsyncIOMotorDatabase | None = None

    async def connect(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(settings.mongodb_url)
        self.db = self.client[settings.mongodb_db_name]
        await self._create_indexes()

    async def disconnect(self):
        if self.client:
            self.client.close()

    async def _create_indexes(self):
        collection = self.db.tasks
        await collection.create_indexes([
            IndexModel([("status", ASCENDING), ("priority_weight", ASCENDING), ("created_at", ASCENDING)]),
            IndexModel([("id", ASCENDING)], unique=True),
        ])

    @property
    def tasks(self):
        return self.db.tasks


db = Database()


async def get_db() -> Database:
    return db


# ── Sync helpers used by Celery workers (PyMongo) ───────────────────────────

import pymongo


def get_sync_db():
    """Synchronous PyMongo client for Celery workers."""
    client = pymongo.MongoClient(settings.mongodb_url)
    return client[settings.mongodb_db_name]


def task_to_response(doc: dict) -> dict:
    """Normalize a MongoDB document for API response."""
    doc["id"] = doc.pop("_id") if "_id" in doc and "id" not in doc else doc.get("id", doc.get("_id"))
    doc.pop("priority_weight", None)
    return doc
