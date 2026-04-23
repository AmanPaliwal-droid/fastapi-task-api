# Prioritized Async Task Processing System

A production-grade async task processing system built with **FastAPI**, **Celery**, **Redis**, and **MongoDB**. Implements strict priority enforcement, distributed locking, idempotent retries, and at-least-once delivery guarantees.

---

## Architecture Overview

```
┌─────────────┐     POST /tasks      ┌─────────────────┐
│   Client    │ ──────────────────▶  │   FastAPI API   │
│             │ ◀──────────────────  │  (uvicorn x2)   │
└─────────────┘   task_id + status   └────────┬────────┘
                                              │ enqueue_task()
                                              ▼
                                   ┌──────────────────────┐
                                   │        Redis         │
                                   │  ┌────────────────┐  │
                                   │  │  queue: high   │  │
                                   │  │  queue: medium │  │
                                   │  │  queue: low    │  │
                                   │  └────────────────┘  │
                                   │  distributed locks   │
                                   └──────────┬───────────┘
                                              │
                  ┌───────────────────────────┼──────────────────────────┐
                  ▼                           ▼                          ▼
        ┌──────────────────┐      ┌──────────────────┐       ┌─────────────────┐
        │  worker-high     │      │  worker-medium   │       │  worker-low     │
        │  queues:         │      │  queues:         │       │  queues:        │
        │  high,medium,low │      │  medium,low      │       │  low            │
        │  concurrency=4   │      │  concurrency=2   │       │  concurrency=1  │
        └────────┬─────────┘      └────────┬─────────┘       └────────┬────────┘
                 │                         │                           │
                 └─────────────────────────┼───────────────────────────┘
                                           ▼
                                  ┌─────────────────┐
                                  │    MongoDB      │
                                  │  tasks collection│
                                  │  (full lifecycle)│
                                  └─────────────────┘
```

---

## Queue Design & Priority Enforcement

### Strategy: Dedicated Queues per Priority (NOT naive sorting)

Three separate Celery queues — `high`, `medium`, `low` — with workers assigned to consume them in strict priority order:

| Worker         | Consumes queues      | Concurrency | Effect                                      |
|----------------|----------------------|-------------|---------------------------------------------|
| `worker-high`  | high → medium → low  | 4           | Always drains HIGH first                    |
| `worker-medium`| medium → low         | 2           | Handles MEDIUM overflow                     |
| `worker-low`   | low only             | 1           | LOW only gets capacity when HIGH/MED are empty|

**Why this beats naive sorting:** Celery's queue ordering within a single worker is respected at the broker level. When `worker-high` has both `high` and `medium` messages, Redis serves `high` queue first. This is enforced structurally, not by a sort at dequeue time which could be violated under concurrency.

**Why not Celery's built-in `task_priority`?** Celery's priority field is broker-dependent and unreliable with Redis (Redis sorted sets don't support per-item priorities natively without the `redis-py-priority-queue` backend). Separate queues are the idiomatic, reliable solution.

---

## Concurrency & Race Condition Handling

### Problem
With multiple workers and `acks_late=True`, the same task message can be delivered to two workers simultaneously if the visibility timeout is hit.

### Solution: Two-Layer Protection

**Layer 1 — Redis Distributed Lock (SETNX)**
```
SETNX task_lock:<task_id> 1 EX 300
```
- Atomic: only one worker acquires the lock
- TTL=300s: lock auto-expires if the worker crashes (prevents deadlock)
- Second worker finds the lock held → logs and exits cleanly

**Layer 2 — MongoDB Atomic Status Transition**
```python
find_one_and_update(
    {"id": task_id, "status": {"$in": ["QUEUED", "RETRYING", "PENDING"]}},
    {"$set": {"status": "PROCESSING"}},
    return_document=AFTER
)
```
- Even if two workers bypass the lock (extremely rare), only one can win the atomic status update
- The loser receives `None` and exits without doing any work

This double-barrier ensures **no task is ever processed twice simultaneously**.

---

## Retry & Failure Handling

### Simulated Failures
30% of tasks fail artificially via `random.random() < 0.30` inside `_simulate_work()`. This mimics real-world transient errors (network timeouts, downstream service failures).

### Retry Policy
- **Max retries:** 3 (configurable via `MAX_RETRIES` env var)
- **Backoff:** Exponential — `2^attempt * 10s` → 10s, 20s, 40s
- **State machine:**

```
PENDING → QUEUED → PROCESSING → COMPLETED
                       │
                       ▼ (failure, retries remain)
                    RETRYING → PROCESSING → ...
                       │
                       ▼ (retries exhausted)
                    FAILED
```

### Safe Retries (No Duplicates)
- The Redis lock is **released before requeuing** — the next attempt runs as a fresh execution
- Celery's `task_id=f"celery-{task_id}"` makes `apply_async` idempotent: resubmitting the same Celery task ID is a no-op if it's already in the broker
- MongoDB status check at task start ensures a COMPLETED task can never be re-processed

---

## Idempotency

Every task operation is idempotent:

1. **Task submission** — each call creates a new UUID. Clients should store the returned `task_id`.
2. **Worker execution** — process_task checks DB status first. COMPLETED or FAILED tasks are skipped unconditionally.
3. **Celery dispatch** — deterministic `celery-{task_id}` as the Celery task ID prevents duplicate broker entries.
4. **Retry transitions** — MongoDB `find_one_and_update` with status precondition is atomic; concurrent transitions are safe.

---

## Worker Crash Recovery

### How it works

`acks_late=True` + `reject_on_worker_lost=True` means:
- The Redis message is NOT acknowledged until the Celery task function **returns**
- If the worker process dies (OOM kill, crash, SIGKILL), the message is **automatically redelivered** by Redis after the `visibility_timeout` (1 hour)

The Redis lock TTL (300s) ensures:
- If a worker crashes while holding the lock, the lock expires in ≤5 minutes
- The redelivered message will find the lock free and proceed normally

### Manual recovery (edge case)
If a task is stuck in PROCESSING after a crash and the lock has expired, run:
```bash
python scripts/recover.py  # sets PROCESSING tasks older than 10min back to QUEUED
```
*(Recovery script is a good next addition for production.)*

---

## API Reference

### POST `/tasks/`
Submit a new task.
```json
{
  "payload": {"key": "value"},
  "priority": "HIGH" | "MEDIUM" | "LOW"
}
```
Returns: `TaskResponse` with `id`, `status: "QUEUED"`, etc.

### GET `/tasks/{task_id}`
Poll task status and result.

### GET `/tasks/`
List tasks with optional filters.
```
?status=FAILED&priority=HIGH&skip=0&limit=50
```

---

## Running the System

### Prerequisites
- Docker + Docker Compose

### Start everything
```bash
cp .env.example .env
docker-compose up --build
```

Services:
| Service       | URL                        |
|---------------|----------------------------|
| API           | http://localhost:8000      |
| API Docs      | http://localhost:8000/docs |
| Flower (UI)   | http://localhost:5555      |
| MongoDB       | localhost:27017            |
| Redis         | localhost:6379             |

### Seed test data
```bash
pip install httpx
python scripts/seed.py
```

### Run tests
```bash
pip install -r requirements.txt
pytest tests/ -v
```

---

## Trade-offs & Design Decisions

| Decision | Chosen | Alternative | Reason |
|----------|--------|-------------|--------|
| Queue strategy | 3 named queues | Single queue + priority field | Named queues are reliable and deterministic on Redis; priority field is broker-dependent |
| Message broker | Redis | RabbitMQ | Simpler ops; Redis already used for locking; acceptable for this scale |
| Database | MongoDB | PostgreSQL | Flexible schema for arbitrary JSON payloads; motor for async |
| Locking | Redis SETNX | DB-level advisory locks | Sub-millisecond, TTL-based, no deadlocks |
| Ack strategy | `acks_late=True` | Default (ack on receive) | At-least-once guarantee; safe with idempotent workers |
| Retry backoff | Exponential (10s/20s/40s) | Fixed interval | Reduces thundering herd on transient failures |
| Worker crash recovery | Visibility timeout + lock TTL | Manual heartbeat | Zero ops overhead; automatic via Redis |

### Known Limitations
- **At-least-once, not exactly-once.** A network partition during the final DB write could cause a retry of a task that already succeeded. Mitigated by the idempotency check, but not 100% eliminated.
- **Priority is best-effort under saturation.** If all workers are busy with HIGH tasks, MEDIUM tasks wait. This is correct behavior, but sets latency expectations for MEDIUM/LOW under heavy load.
- **Single Redis instance.** A Redis failure loses all queued messages. For production: Redis Sentinel or Cluster + persistence (`appendonly yes` is already set in docker-compose).

---

## Project Structure

```
xavi-task-processor/
├── app/
│   ├── api/
│   │   └── routes.py          # FastAPI endpoints
│   ├── core/
│   │   ├── database.py        # MongoDB async client + sync helpers
│   │   └── service.py         # Business logic
│   ├── models/
│   │   └── task.py            # Pydantic models + enums
│   ├── workers/
│   │   └── celery_app.py      # Celery app, task, locking, retries
│   ├── config.py              # Settings via pydantic-settings
│   └── main.py                # FastAPI app entrypoint
├── tests/
│   └── test_tasks.py
├── scripts/
│   └── seed.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```
