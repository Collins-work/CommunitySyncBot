from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    bot_token: str
    database_url: str
    admin_telegram_ids: List[int]
    default_timezone: str = "UTC"
    schedule_refresh_seconds: int = 120

    @staticmethod
    def from_env() -> "Settings":
        token = os.getenv("BOT_TOKEN", "").strip()
        if not token:
            raise ValueError("BOT_TOKEN is required")

        db_url = os.getenv("DATABASE_URL", "").strip()
        if not db_url:
            raise ValueError("DATABASE_URL is required")

        admin_raw = os.getenv("ADMIN_TELEGRAM_IDS", "").strip()
        if not admin_raw:
            raise ValueError("ADMIN_TELEGRAM_IDS is required (comma-separated Telegram user IDs)")

        admin_ids: List[int] = []
        for value in admin_raw.split(","):
            value = value.strip()
            if not value:
                continue
            admin_ids.append(int(value))

        timezone = os.getenv("DEFAULT_TIMEZONE", "UTC").strip() or "UTC"
        refresh_seconds = int(os.getenv("SCHEDULE_REFRESH_SECONDS", "120"))

        return Settings(
            bot_token=token,
            database_url=db_url,
            admin_telegram_ids=admin_ids,
            default_timezone=timezone,
            schedule_refresh_seconds=refresh_seconds,
        )
