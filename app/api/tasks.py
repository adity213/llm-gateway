"""Task status check endpoint for deferrable queue jobs."""

from fastapi import APIRouter, HTTPException, status
from app.models.schemas import TaskStatusResponse
from app.queuing.queue_manager import queue_manager

router = APIRouter(tags=["Tasks"])


@router.get(
    "/v1/tasks/{task_id}",
    response_model=TaskStatusResponse,
    responses={
        200: {"description": "Task status retrieved"},
        404: {"description": "Task not found"},
    },
)
async def get_task_status(task_id: str):
    """
    Checks the current status of an asynchronously queued deferrable request.
    """
    task = await queue_manager.get_task_status(task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task '{task_id}' not found.",
        )
    return task
