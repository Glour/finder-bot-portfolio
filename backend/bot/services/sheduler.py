from datetime import datetime, timedelta
from typing import Any, Callable

from apscheduler.schedulers.asyncio import AsyncIOScheduler


async def set_scheduled_jobs(
        scheduler: AsyncIOScheduler,
        funk: Callable[..., Any],
        trigger: str = "date",
        days_before: int = 0,
        hours_before: int = 0,
        minutes_before: int = 0,
        *args: Any,
        **kwargs: Any
) -> None:
    """
    Schedules jobs to be run on the APScheduler based on specified parameters.

    :param scheduler: The scheduler instance to which the jobs will be added.
    :param funk: The function to be scheduled.
    :param trigger: Type of trigger for the job ('cron', 'interval', etc.). Defaults to 'cron'.
    :param days_before: Number of days before the job should be run. Defaults to 0.
    :param hours_before: Number of hours before the job should be run. Defaults to 0.
    :param minutes_before: Number of minutes before the job should be run. Defaults to 0.
    :param args: Additional positional arguments to pass to the function.
    :param kwargs: Additional keyword arguments to pass to the function.
    :return: None
    """

    run_date = datetime.now() + timedelta(days=days_before, hours=hours_before, minutes=minutes_before)
    # Add job to the scheduler
    scheduler.add_job(
        func=funk,
        trigger=trigger,
        run_date=run_date,
        args=args,
        kwargs=kwargs,
    )
