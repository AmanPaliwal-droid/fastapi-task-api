"""
Celery application and task processor.

Priority enforcement strategy:
  - Three named queues: high, medium, low
  - worker-high consumes from all three (HIGH > MEDIUM > LOW via queue ordering)
  - worker-medium consumes from medium + low
  - worker-low consumes from low only
  - Celery respects queue ordering within a single worker, so HIGH tasks
    are always drained before MEDIUM, MEDIUM before LOW.

Concurrency / race conditions:
  - Redis SETNX lock per task_id with TTL (prevents double processing)
  - Lock is released on success or permanent failure
  - If worker crashes mid-task, TTL auto-expires the lock → task requeued via
    Celery's acks_late=True + task visibility timeout in Redis

Idempotency:
  - task_id is the idempotency key
  - Atomic status transition check before work begins
  - If task is already COMPLETED/FAILED, lock is skipped

At-least-once delivery:
  - acks_late=True means the message is NOT acked until the task function
    returns. If the worker crashes, the message is redelivered.
"""

import random
import time
import logging
from datetime import datetime, timezone

import pymongo
from celery import Celery
from celery.utils.log import get_task_logger

from app.config import settings
from app.models.task import Priority, TaskStatus
from app.core.database import get_sync_db, PRIORITY_WEIGHT

logger = get_task_logger(__name__)

# ── Celery app ────────────────────────────────────────────────────────────────

celery_app = Celery(
    "xavi_tasks",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

celery_app.conf.update(
    # Queue routing
    task_routes={
        "app.workers.celery_app.process_task": {
            # Routing is dynamic — see apply_async calls in service layer
        },
    },
    # Priority via separate queues (not Celery's built-in priority which is broker-side only)
    task_queues={},  # Defined implicitly by queue names in workers

    # Reliability settings
    task_acks_late=True,             # Ack AFTER task completes (at-least-once)
    task_reject_on_worker_lost=True, # Requeue on worker crash
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # Visibility timeout must exceed longest expected task duration
    broker_transport_options={
        "visibility_timeout": 3600,  # 1 hour
    },

    # Prevent memory leaks in long-running workers
    worker_max_tasks_per_child=100,
)

# Redis lock TTL in seconds — must be > max task execution time
LOCK_TTL = settings.task_lock_ttl
LOCK_PREFIX = "task_lock:"


def _acquire_lock(redis_client, task_id: str) -> bool:
    """
    Attempt to acquire a distributed lock for this task_id.
    Uses SET NX EX (atomic) — returns True if lock acquired.
    """
    key = f"{LOCK_PREFIX}{task_id}"
    return redis_client.set(key, "1", nx=True, ex=LOCK_TTL)


def _release_lock(redis_client, task_id: str):
    key = f"{LOCK_PREFIX}{task_id}"
    redis_client.delete(key)


@celery_app.task(
    bind=True,
    name="app.workers.celery_app.process_task",
    max_retries=settings.max_retries,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=240,
    time_limit=300,
)
def process_task(self, task_id: str):
    """
    Core task processor.

    Flow:
      1. Acquire Redis distributed lock (prevents concurrent processing)
      2. Atomic status check + transition to PROCESSING in MongoDB
      3. Simulate work (with ~30% artificial failure)
      4. On success → mark COMPLETED, release lock
      5. On failure → retry up to max_retries with exponential backoff
      6. On exhausted retries → mark FAILED, release lock
    """
    import redis as redis_lib

    r = redis_lib.from_url(settings.redis_url)
    mongo_db = get_sync_db()
    tasks_col = mongo_db.tasks

    now = datetime.now(timezone.utc)

    # ── 1. Idempotency check before acquiring lock ────────────────────────────
    doc = tasks_col.find_one({"id": task_id})
    if not doc:
        logger.error(f"Task {task_id} not found in DB — skipping")
        return

    if doc["status"] in (TaskStatus.COMPLETED, TaskStatus.FAILED):
        logger.info(f"Task {task_id} already in terminal state {doc['status']} — skipping")
        return

    # ── 2. Acquire distributed lock ───────────────────────────────────────────
    if not _acquire_lock(r, task_id):
        logger.warning(f"Task {task_id} is already being processed — another worker holds the lock")
        # Don't retry via Celery — let the lock-holding worker finish
        return

    try:
        # ── 3. Atomic status transition: only proceed if QUEUED or RETRYING ──
        result = tasks_col.find_one_and_update(
            {
                "id": task_id,
                "status": {"$in": [TaskStatus.QUEUED, TaskStatus.RETRYING, TaskStatus.PENDING]},
            },
            {
                "$set": {
                    "status": TaskStatus.PROCESSING,
                    "updated_at": now,
                    "worker_id": self.request.hostname,
                }
            },
            return_document=pymongo.ReturnDocument.AFTER,
        )

        if not result:
            logger.warning(f"Task {task_id} status transition failed — concurrent worker won the race")
            return

        logger.info(f"[{task_id}] Processing started (attempt {self.request.retries + 1}/{settings.max_retries + 1})")

        # ── 4. Simulate work ──────────────────────────────────────────────────
        _simulate_work(task_id)

        # ── 5. Mark COMPLETED ─────────────────────────────────────────────────
        tasks_col.update_one(
            {"id": task_id},
            {
                "$set": {
                    "status": TaskStatus.COMPLETED,
                    "updated_at": datetime.now(timezone.utc),
                    "result": {"message": "Task processed successfully", "processed_at": datetime.now(timezone.utc).isoformat()},
                }
            },
        )
        logger.info(f"[{task_id}] Completed successfully")

    except TaskProcessingError as exc:
        # Controlled failure — retry with exponential backoff
        retry_count = self.request.retries + 1
        logger.warning(f"[{task_id}] Failed (attempt {retry_count}): {exc}")

        if self.request.retries < settings.max_retries:
            # Update to RETRYING before releasing the lock (so next pickup works)
            tasks_col.update_one(
                {"id": task_id},
                {
                    "$set": {
                        "status": TaskStatus.RETRYING,
                        "updated_at": datetime.now(timezone.utc),
                        "error": str(exc),
                    },
                    "$inc": {"retry_count": 1},
                },
            )
            _release_lock(r, task_id)
            # Exponential backoff: 2^retry * 10s (10s, 20s, 40s)
            countdown = (2 ** self.request.retries) * 10
            logger.info(f"[{task_id}] Retrying in {countdown}s (attempt {retry_count + 1}/{settings.max_retries + 1})")
            raise self.retry(exc=exc, countdown=countdown)
        else:
            # Exhausted retries → permanent failure
            tasks_col.update_one(
                {"id": task_id},
                {
                    "$set": {
                        "status": TaskStatus.FAILED,
                        "updated_at": datetime.now(timezone.utc),
                        "error": f"Max retries ({settings.max_retries}) exhausted. Last error: {exc}",
                    },
                    "$inc": {"retry_count": 1},
                },
            )
            logger.error(f"[{task_id}] Permanently failed after {retry_count} attempts")

    except Exception as exc:
        # Unexpected error — mark failed, don't retry
        logger.exception(f"[{task_id}] Unexpected error: {exc}")
        tasks_col.update_one(
            {"id": task_id},
            {
                "$set": {
                    "status": TaskStatus.FAILED,
                    "updated_at": datetime.now(timezone.utc),
                    "error": f"Unexpected error: {exc}",
                }
            },
        )

    finally:
        _release_lock(r, task_id)


class TaskProcessingError(Exception):
    """Raised to simulate a task failure eligible for retry."""


def _simulate_work(task_id: str):
    """
    Simulate real processing work.
    Introduces ~30% random failure rate as required by the spec.
    """
    # Simulate variable processing time (0.5–2s)
    time.sleep(random.uniform(0.5, 2.0))

    if random.random() < settings.failure_simulation_rate:
        raise TaskProcessingError(f"Simulated processing failure for task {task_id}")


# ── Queue name helpers ────────────────────────────────────────────────────────

PRIORITY_QUEUE_MAP = {
    Priority.HIGH: "high",
    Priority.MEDIUM: "medium",
    Priority.LOW: "low",
}


def enqueue_task(task_id: str, priority: Priority):
    """Dispatch a task to the correct priority queue."""
    queue = PRIORITY_QUEUE_MAP[priority]
    process_task.apply_async(
        args=[task_id],
        queue=queue,
        task_id=f"celery-{task_id}",  # deterministic celery task ID for idempotency
    )
    logger.info(f"Enqueued task {task_id} → queue={queue}")
