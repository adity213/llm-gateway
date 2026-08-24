"""Redis-backed queue manager with exponential backoff and jitter for deferrable requests."""

import json
import random
import time
import uuid
from typing import Dict, List, Optional
import redis.asyncio as aioredis
from app.config import settings
from app.core.metrics import set_queue_depth_metric
from app.core.redis_client import get_redis_client
from app.models.schemas import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    TaskStatusEnum,
    TaskStatusResponse,
)


class QueueManager:
    """Manages deferrable request queues with exponential backoff, jitter, and status tracking."""

    QUEUE_KEY = "gateway:queue:deferrable"
    TASK_PREFIX = "gateway:task:"

    def __init__(
        self,
        base_backoff: float = settings.QUEUE_BACKOFF_BASE_SECONDS,
        jitter: float = settings.QUEUE_BACKOFF_JITTER_SECONDS,
        max_retries: int = settings.MAX_DEFERRABLE_RETRIES,
        max_timeout_seconds: int = settings.MAX_DEFERRABLE_TIMEOUT_SECONDS,
    ):
        self.base_backoff = base_backoff
        self.jitter = jitter
        self.max_retries = max_retries
        self.max_timeout_seconds = max_timeout_seconds
        self._local_tasks: Dict[str, Dict] = {}

    def calculate_next_retry(self, attempt: int) -> float:
        """Calculates next retry timestamp using exponential backoff with jitter."""
        # 1s, 2s, 4s, 8s, 16s... capped at 60s
        backoff = min(60.0, self.base_backoff * (2 ** (attempt - 1)))
        jitter_offset = random.uniform(-self.jitter, self.jitter)
        delay = max(0.2, backoff + jitter_offset)
        return time.time() + delay

    async def enqueue_request(
        self,
        request: ChatCompletionRequest,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> TaskStatusResponse:
        """Pushes a deferrable request into the priority queue."""
        client = redis_client or await get_redis_client()
        now = time.time()
        task_id = request.idempotency_key or f"task-{uuid.uuid4().hex[:12]}"
        next_retry = now  # immediate first attempt in worker

        task_data = {
            "task_id": task_id,
            "status": TaskStatusEnum.QUEUED.value,
            "created_at": now,
            "updated_at": now,
            "attempts": 0,
            "max_attempts": self.max_retries,
            "max_timeout_seconds": self.max_timeout_seconds,
            "request": request.model_dump(),
            "error": None,
            "result": None,
        }

        task_key = f"{self.TASK_PREFIX}{task_id}"
        pipe = client.pipeline()
        pipe.set(task_key, json.dumps(task_data), ex=86400)
        pipe.zadd(self.QUEUE_KEY, {task_id: next_retry})
        pipe.zcard(self.QUEUE_KEY)
        results = await pipe.execute()

        queue_len = results[2] if len(results) > 2 else 1
        set_queue_depth_metric(queue_len)
        self._local_tasks[task_id] = task_data

        return TaskStatusResponse(
            task_id=task_id,
            status=TaskStatusEnum.QUEUED,
            created_at=now,
            updated_at=now,
            attempts=0,
            max_attempts=self.max_retries,
        )

    async def get_task_status(
        self,
        task_id: str,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> Optional[TaskStatusResponse]:
        """Fetches the current status and result for a queued or completed task."""
        client = redis_client or await get_redis_client()
        task_key = f"{self.TASK_PREFIX}{task_id}"
        raw = await client.get(task_key)

        if not raw:
            local = self._local_tasks.get(task_id)
            if local:
                return TaskStatusResponse(
                    task_id=task_id,
                    status=TaskStatusEnum(local["status"]),
                    created_at=local["created_at"],
                    updated_at=local["updated_at"],
                    attempts=local.get("attempts", 0),
                    max_attempts=local.get("max_attempts", self.max_retries),
                    error=local.get("error"),
                    result=ChatCompletionResponse.model_validate(local["result"]) if local.get("result") else None,
                )
            return None

        try:
            data = json.loads(raw)
            result_obj = None
            if data.get("result"):
                result_obj = ChatCompletionResponse.model_validate(data["result"])

            return TaskStatusResponse(
                task_id=task_id,
                status=TaskStatusEnum(data.get("status", TaskStatusEnum.QUEUED.value)),
                created_at=data.get("created_at", time.time()),
                updated_at=data.get("updated_at", time.time()),
                attempts=data.get("attempts", 0),
                max_attempts=data.get("max_attempts", self.max_retries),
                error=data.get("error"),
                result=result_obj,
            )
        except Exception:
            return None

    async def get_due_tasks(
        self,
        limit: int = 10,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> List[str]:
        """Retrieves task IDs ready for retry (score <= current_time)."""
        client = redis_client or await get_redis_client()
        now = time.time()
        task_ids = await client.zrangebyscore(self.QUEUE_KEY, "-inf", now, start=0, num=limit)
        return task_ids or []

    async def complete_task(
        self,
        task_id: str,
        response: ChatCompletionResponse,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> None:
        """Marks task as successfully completed and removes from queue."""
        client = redis_client or await get_redis_client()
        now = time.time()
        task_key = f"{self.TASK_PREFIX}{task_id}"

        raw = await client.get(task_key)
        data = json.loads(raw) if raw else {"task_id": task_id, "created_at": now, "attempts": 1}
        data["status"] = TaskStatusEnum.COMPLETED.value
        data["updated_at"] = now
        data["result"] = response.model_dump()
        data["error"] = None

        pipe = client.pipeline()
        pipe.set(task_key, json.dumps(data), ex=86400)
        pipe.zrem(self.QUEUE_KEY, task_id)
        pipe.zcard(self.QUEUE_KEY)
        results = await pipe.execute()

        queue_len = results[2] if len(results) > 2 else 0
        set_queue_depth_metric(queue_len)
        self._local_tasks[task_id] = data

    async def reschedule_or_fail_task(
        self,
        task_id: str,
        error_message: str,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> None:
        """Increments attempt count, applies backoff, or permanently fails task if caps exceeded."""
        client = redis_client or await get_redis_client()
        now = time.time()
        task_key = f"{self.TASK_PREFIX}{task_id}"

        raw = await client.get(task_key)
        data = json.loads(raw) if raw else {"task_id": task_id, "created_at": now, "attempts": 0}

        attempts = data.get("attempts", 0) + 1
        data["attempts"] = attempts
        data["updated_at"] = now
        data["error"] = error_message
        created_at = data.get("created_at", now)

        # Check hard caps: attempts cap or time cap
        if attempts >= self.max_retries or (now - created_at) >= self.max_timeout_seconds:
            data["status"] = TaskStatusEnum.FAILED.value
            pipe = client.pipeline()
            pipe.set(task_key, json.dumps(data), ex=86400)
            pipe.zrem(self.QUEUE_KEY, task_id)
            pipe.zcard(self.QUEUE_KEY)
            results = await pipe.execute()
            queue_len = results[2] if len(results) > 2 else 0
            set_queue_depth_metric(queue_len)
        else:
            data["status"] = TaskStatusEnum.QUEUED.value
            next_retry = self.calculate_next_retry(attempts)
            pipe = client.pipeline()
            pipe.set(task_key, json.dumps(data), ex=86400)
            pipe.zadd(self.QUEUE_KEY, {task_id: next_retry})
            pipe.zcard(self.QUEUE_KEY)
            results = await pipe.execute()
            queue_len = results[2] if len(results) > 2 else 0
            set_queue_depth_metric(queue_len)

        self._local_tasks[task_id] = data


queue_manager = QueueManager()
