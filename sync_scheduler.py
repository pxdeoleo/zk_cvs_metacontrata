from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from config import load_config
from main import main
import asyncio

def start_scheduler():
    config = load_config()
    cron_expr = config["cron"]["expression"]

    scheduler = AsyncIOScheduler()
    scheduler.add_job(main, CronTrigger.from_crontab(cron_expr))
    scheduler.start()

    asyncio.get_event_loop().run_forever()
