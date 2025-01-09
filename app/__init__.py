import asyncio

from app.config import POLLING_INTERVAL, PAUSE
from app.uptime import ping_uptime_monitor
from app.utilities import enqueue_new_files, report_file_progress


async def main():
    tasks = []

    # Uptime reporting coroutine
    uptime_heartbeat_coroutine = asyncio.create_task(ping_uptime_monitor())
    tasks.append(uptime_heartbeat_coroutine)

    # S3 monitoring and enqueuing coroutine
    file_enqueue_coroutine = asyncio.create_task(enqueue_new_files())
    tasks.append(file_enqueue_coroutine)

    # Create a queue that we will use to store our "workload".
    queue = asyncio.Queue()

    # Initialize the worker coroutines, which will continuously monitor the queue and consume
    for i in range(3):
        task = asyncio.create_task(worker(f"worker-{i}", queue))
        tasks.append(task)

    # Progress report coroutine
    progress_report_coroutine = asyncio.create_task(report_file_progress())
    tasks.append(progress_report_coroutine)

    try:
        # Run the tasks indefinitely
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        # Graceful shutdown with reporting
        print("Tasks have been cancelled")
