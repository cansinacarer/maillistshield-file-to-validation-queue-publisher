import asyncio

from app.utilities.reporting import ping_uptime_monitor
from app.utilities.logging import logger
from app.file_handler import enqueue_new_files


async def main():
    tasks = []

    # S3 monitoring and enqueuing coroutine
    file_enqueue_coroutine = asyncio.create_task(enqueue_new_files())
    tasks.append(file_enqueue_coroutine)

    # Uptime reporting coroutine
    uptime_heartbeat_coroutine = asyncio.create_task(ping_uptime_monitor())
    tasks.append(uptime_heartbeat_coroutine)

    try:
        # Run the tasks indefinitely
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        # Graceful shutdown with reporting
        logger.info("Tasks have been cancelled")
