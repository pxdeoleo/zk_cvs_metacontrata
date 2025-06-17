import asyncio
import signal
import sys
import logging
from typing import Any

from logging_config import setup_logging
from sync_scheduler import start_scheduler

logger = logging.getLogger(__name__)


def main() -> None:
    setup_logging()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Handle shutdown signals
    stop_event = asyncio.Event()

    def shutdown_handler(*_: Any) -> None:
        logger.info("Shutdown signal received.")
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    logger.info("Starting MetaContrata-CVSecurity sync scheduler...")

    try:
        start_scheduler(loop)  # this schedules jobs but doesn't block
        loop.run_until_complete(stop_event.wait())
    finally:
        logger.info("Shutting down...")
        loop.stop()
        loop.close()


if __name__ == "__main__":
    main()
