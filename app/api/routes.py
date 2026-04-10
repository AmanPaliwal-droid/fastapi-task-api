from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional

from app.core.database import Database, get_db
from app.core.service import create_task, get_task, list_tasks
from app.models.task import TaskCreate, TaskResponse, TaskStatus, Priority

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.post("/", response_model=TaskResponse, status_code=201)
async def submit_task(data: TaskCreate, db: Database = Depends(get_db)):
    """Submit a new task for async processing."""
    return await create_task(db, data)


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task_status(task_id: str, db: Database = Depends(get_db)):
    """Retrieve the current status and result of a task."""
    task = await get_task(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    return task


@router.get("/", response_model=list[TaskResponse])
async def list_all_tasks(
    status: Optional[TaskStatus] = Query(None, description="Filter by status"),
    priority: Optional[Priority] = Query(None, description="Filter by priority"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    db: Database = Depends(get_db),
):
    """List tasks with optional filtering by status and/or priority."""
    return await list_tasks(db, status=status, priority=priority, skip=skip, limit=limit)
