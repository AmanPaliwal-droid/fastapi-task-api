#!/usr/bin/env python3
"""
Seed script: Submit sample tasks across all priorities to test the system.
Usage: python scripts/seed.py
"""
import httpx
import random
import time

BASE_URL = "http://localhost:8000"
PRIORITIES = ["HIGH", "MEDIUM", "LOW"]


def submit_task(payload: dict, priority: str) -> dict:
    r = httpx.post(f"{BASE_URL}/tasks/", json={"payload": payload, "priority": priority})
    r.raise_for_status()
    return r.json()


def main():
    print("Submitting 30 tasks (10 per priority)...")
    task_ids = []

    for priority in PRIORITIES:
        for i in range(10):
            task = submit_task(
                payload={"job": f"{priority.lower()}_task_{i}", "value": random.randint(1, 100)},
                priority=priority,
            )
            task_ids.append(task["id"])
            print(f"  [{priority}] Submitted: {task['id']}")

    print(f"\nSubmitted {len(task_ids)} tasks. Polling status in 5s...")
    time.sleep(5)

    for task_id in task_ids[:5]:
        r = httpx.get(f"{BASE_URL}/tasks/{task_id}")
        t = r.json()
        print(f"  {task_id[:8]}... status={t['status']} retries={t['retry_count']}")

    print("\nDone. Check GET /tasks/ for full list, or Flower at http://localhost:5555")


if __name__ == "__main__":
    main()
