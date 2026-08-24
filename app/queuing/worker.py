"""Background worker for asynchronous retry of queued deferrable requests."""

import asyncio
import json
import logging
from typing import Optional
import redis.asyncio as aioredis
from app.core.redis_client import get_redis_client
from app.models.schemas import ChatCompletionRequest
from app.queuing.idempotency import idempotency_manager
from app.queuing.queue_manager import queue_manager
from app.resilience.router import router

logger = logging.getLogger("gateway.worker")


class DeferrableQueueWorker:
    """Processes deferrable requests from Redis queue as providers recover."""

    def __init__(self, poll_interval_seconds: float = 1.0):
        self.poll_interval = poll_interval_seconds
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Starts the background worker loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Deferrable Queue Worker started.")

    async def stop(self) -> None:
        """Stops the worker gracefully."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Deferrable Queue Worker stopped.")

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self.process_batch()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"Error in queue worker loop: {exc}")
            await asyncio.sleep(self.poll_interval)

    async def process_batch(self, batch_size: int = 5) -> int:
        """Processes one batch of due tasks."""
        try:
            client = await get_redis_client()
        except Exception:
            return 0

        due_task_ids = await queue_manager.get_due_tasks(limit=batch_size, redis_client=client)
        if not due_task_ids:
            return 0

        processed = 0
        for task_id in due_task_ids:
            task_key = f"{queue_manager.TASK_PREFIX}{task_id}"
            raw = await client.get(task_key)
            if not raw:
                # Task data missing, remove from queue
                await client.zrem(queue_manager.QUEUE_KEY, task_id)
                continue

            try:
                task_data = json.loads(raw)
                req_dict = task_data.get("request", {})
                req = ChatCompletionRequest.model_validate(req_dict)

                # Attempt route and execution
                response = await router.route_and_execute(req)

                # Store response in idempotency cache
                if req.idempotency_key:
                    await idempotency_manager.store_response(req.idempotency_key, response, redis_client=client)

                # Mark completed
                await queue_manager.complete_task(task_id, response, redis_client=client)
                processed += 1

            except Exception as exc:
                logger.warning(f"Retry attempt for task {task_id} failed: {exc}")
                await queue_manager.reschedule_or_fail_task(task_id, str(exc), redis_client=client)

        return processed


queue_worker = DeferrableQueueWorker()
