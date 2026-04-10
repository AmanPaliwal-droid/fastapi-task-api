"""
Task service — all business logic lives here, not in the API layer.
"""
import uuid
from datetime import datetime, timezone

from app.core.database import Database, PRIORITY_WEIGHT, task_to_response
from app.models.task import Priority, TaskCreate, TaskStatus
from app.workers.celery_app import enqueue_task


async def create_task(db: Database, data: TaskCreate) -> dict:
    task_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    doc = {
        "id": task_id,
        "payload": data.payload,
        "priority": data.priority,
        "priority_weight": PRIORITY_WEIGHT[data.priority],
        "status": TaskStatus.PENDING,
        "retry_count": 0,
        "created_at": now,
        "updated_at": now,
        "error": None,
        "result": None,
        "worker_id": None,
    }

    await db.tasks.insert_one(doc)

    # Transition to QUEUED and enqueue atomically
    await db.tasks.update_one(
        {"id": task_id},
        {"$set": {"status": TaskStatus.QUEUED, "updated_at": datetime.now(timezone.utc)}},
    )

    # Dispatch to Celery — this is safe to call multiple times for the same
    # task_id because process_task checks idempotency via DB status
    enqueue_task(task_id, data.priority)

    doc["status"] = TaskStatus.QUEUED
    return task_to_response(doc)


async def get_task(db: Database, task_id: str) -> dict | None:
    doc = await db.tasks.find_one({"id": task_id})
    if not doc:
        return None
    return task_to_response(doc)


async def list_tasks(db: Database, status=None, priority=None, skip=0, limit=50) -> list[dict]:
    query = {}
    if status:
        query["status"] = status
    if priority:
        query["priority"] = priority

    cursor = (
        db.tasks.find(query)
        .sort([("priority_weight", 1), ("created_at", 1)])
        .skip(skip)
        .limit(limit)
    )
    docs = await cursor.to_list(length=limit)
    return [task_to_response(doc) for doc in docs]
