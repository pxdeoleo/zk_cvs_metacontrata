from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from config import load_config
from main import main
import asyncio


def start_scheduler(loop: asyncio.AbstractEventLoop) -> None:
    config = load_config()
    cron_expr = config["cron"]["expression"]

    scheduler = AsyncIOScheduler(event_loop=loop)
    scheduler.add_job(main, CronTrigger.from_crontab(cron_expr))
    loop.call_soon(scheduler.start)
