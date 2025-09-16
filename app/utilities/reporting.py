import requests
import asyncio
from app.config import UPTIME_MONITOR, POLLING_INTERVAL
from app.utilities.logging import logger


# Send a heartbeat the the uptime monitor
async def ping_uptime_monitor():
    while True:
        try:
            requests.get(UPTIME_MONITOR)
        except Exception as e:
            logger.error(f"Error while sending heartbeat to uptime monitor: {e}")
        finally:
            # this is not blocking execution like time.sleep() does
            await asyncio.sleep(POLLING_INTERVAL)
