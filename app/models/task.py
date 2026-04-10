from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field
import uuid


class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


class TaskCreate(BaseModel):
    payload: dict[str, Any]
    priority: Priority = Priority.MEDIUM


class TaskResponse(BaseModel):
    id: str
    payload: dict[str, Any]
    priority: Priority
    status: TaskStatus
    retry_count: int
    created_at: datetime
    updated_at: datetime
    error: str | None = None
    result: dict[str, Any] | None = None

    model_config = {"from_attributes": True}


class TaskFilter(BaseModel):
    status: TaskStatus | None = None
    priority: Priority | None = None
    skip: int = 0
    limit: int = 50
