#!/usr/bin/env python3
"""
Recovery script: Re-queue tasks stuck in PROCESSING state.
Run this after a worker crash if tasks are stuck.
Usage: python scripts/recover.py [--older-than-minutes 10]
"""
import sys
import argparse
from datetime import datetime, timezone, timedelta

import pymongo

MONGODB_URL = "mongodb://root:rootpassword@localhost:27017/taskdb?authSource=admin"


def recover(older_than_minutes: int = 10):
    client = pymongo.MongoClient(MONGODB_URL)
    db = client["taskdb"]
    tasks = db.tasks

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=older_than_minutes)

    result = tasks.update_many(
        {
            "status": "PROCESSING",
            "updated_at": {"$lt": cutoff},
        },
        {
            "$set": {
                "status": "QUEUED",
                "updated_at": datetime.now(timezone.utc),
                "error": f"Recovered from stuck PROCESSING state (>{older_than_minutes}min)",
            }
        },
    )

    print(f"Recovered {result.modified_count} stuck task(s) → QUEUED")
    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--older-than-minutes", type=int, default=10)
    args = parser.parse_args()
    recover(args.older_than_minutes)
