from __future__ import annotations

import logging
from typing import Dict, Set

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from telegram.ext import Application

from config import Settings
from db import PostRepository
from publisher import PublisherService


logger = logging.getLogger(__name__)


class ScheduleManager:
    def __init__(
        self,
        application: Application,
        settings: Settings,
        repo: PostRepository,
        publisher: PublisherService,
    ) -> None:
        self.application = application
        self.settings = settings
        self.repo = repo
        self.publisher = publisher
        self.scheduler = AsyncIOScheduler(timezone=settings.default_timezone)

    def start(self) -> None:
        self.scheduler.start()
        self.reload_jobs()
        self.scheduler.add_job(
            self.reload_jobs,
            "interval",
            seconds=self.settings.schedule_refresh_seconds,
            id="schedule-refresh",
            replace_existing=True,
        )
        logger.info("Scheduler started")

    def shutdown(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)

    def reload_jobs(self) -> None:
        schedules = self.repo.fetch_active_schedules(self.settings.default_timezone)
        desired_ids: Set[str] = {f"post-{row.id}" for row in schedules}

        for job in self.scheduler.get_jobs():
            if job.id.startswith("post-") and job.id not in desired_ids:
                self.scheduler.remove_job(job.id)

        for row in schedules:
            job_id = f"post-{row.id}"
            try:
                trigger = CronTrigger.from_crontab(row.cron_expr, timezone=row.timezone)
            except ValueError:
                logger.exception("Invalid cron expression for schedule id=%s: %s", row.id, row.cron_expr)
                continue
            self.scheduler.add_job(
                self._safe_publish,
                trigger=trigger,
                args=[row.target_chat_id],
                id=job_id,
                replace_existing=True,
                coalesce=True,
                max_instances=1,
            )

        logger.info("Loaded %s schedule jobs", len(schedules))

    async def _safe_publish(self, target_chat_id: int) -> None:
        try:
            await self.publisher.publish_next_for_chat(self.application.bot, target_chat_id)
        except Exception:
            logger.exception("Scheduled publish failed for chat_id=%s", target_chat_id)
