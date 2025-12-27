from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session

from .db import SessionLocal
from .settings import settings
from .services.ingest import ingest_wpb

scheduler = BackgroundScheduler()

def _run_ingest():
    db: Session = SessionLocal()
    try:
        ingest_wpb(db, days_back=settings.ingest_days_back)
    finally:
        db.close()

def start_scheduler():
    # Two runs/day (simple, reliable). You can move to every 4 hours later.
    scheduler.add_job(
        _run_ingest,
        CronTrigger(hour=settings.ingest_cron_hour_1, minute=5),
        id="ingest_am",
        replace_existing=True,
    )
    scheduler.add_job(
        _run_ingest,
        CronTrigger(hour=settings.ingest_cron_hour_2, minute=5),
        id="ingest_pm",
        replace_existing=True,
    )
    scheduler.start()
