from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


@dataclass(frozen=True)
class ScheduleRow:
    id: int
    target_chat_id: int
    cron_expr: str
    timezone: str
    active: bool = True


class PostRepository:
    def __init__(self, database_url: str) -> None:
        self.pool = ConnectionPool(database_url, kwargs={"row_factory": dict_row}, open=False)

    def open(self) -> None:
        self.pool.open(wait=True)

    def close(self) -> None:
        self.pool.close()

    def init_schema(self) -> None:
        schema_sql = """
        CREATE TABLE IF NOT EXISTS posts (
            id BIGSERIAL PRIMARY KEY,
            target_chat_id BIGINT NOT NULL,
            content TEXT NOT NULL,
            posted BOOLEAN NOT NULL DEFAULT FALSE,
            in_progress BOOLEAN NOT NULL DEFAULT FALSE,
            content_type TEXT NOT NULL DEFAULT 'text',
            source_chat_id BIGINT,
            source_message_id BIGINT,
            media_url TEXT,
            caption TEXT,
            poll_question TEXT,
            poll_options JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            posted_at TIMESTAMPTZ
        );

        CREATE INDEX IF NOT EXISTS idx_posts_dispatch
            ON posts (target_chat_id, posted, in_progress, id);

        CREATE TABLE IF NOT EXISTS channel_schedules (
            id BIGSERIAL PRIMARY KEY,
            target_chat_id BIGINT NOT NULL,
            cron_expr TEXT NOT NULL,
            timezone TEXT NOT NULL DEFAULT 'UTC',
            active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(target_chat_id, cron_expr, timezone)
        );

        ALTER TABLE posts ADD COLUMN IF NOT EXISTS source_chat_id BIGINT;
        ALTER TABLE posts ADD COLUMN IF NOT EXISTS source_message_id BIGINT;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(schema_sql)
            conn.commit()

    def queue_telegram_message(
        self,
        target_chat_id: int,
        source_chat_id: int,
        source_message_id: int,
        content_preview: str,
        content_type: str,
    ) -> int:
        sql = """
        INSERT INTO posts (
            target_chat_id,
            content,
            content_type,
            source_chat_id,
            source_message_id,
            posted,
            in_progress
        )
        VALUES (
            %(target_chat_id)s,
            %(content)s,
            %(content_type)s,
            %(source_chat_id)s,
            %(source_message_id)s,
            FALSE,
            FALSE
        )
        RETURNING id;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    {
                        "target_chat_id": target_chat_id,
                        "content": content_preview,
                        "content_type": content_type,
                        "source_chat_id": source_chat_id,
                        "source_message_id": source_message_id,
                    },
                )
                row = cur.fetchone()
            conn.commit()
        return int(row["id"])

    def claim_next_unposted_post(self, target_chat_id: int) -> Optional[Dict[str, Any]]:
        sql = """
        WITH next_post AS (
            SELECT id
            FROM posts
            WHERE target_chat_id = %(target_chat_id)s
              AND posted = FALSE
              AND in_progress = FALSE
            ORDER BY id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE posts p
        SET in_progress = TRUE
        FROM next_post
        WHERE p.id = next_post.id
        RETURNING p.*;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"target_chat_id": target_chat_id})
                row = cur.fetchone()
            conn.commit()
        return row

    def mark_posted(self, post_id: int) -> None:
        sql = """
        UPDATE posts
        SET posted = TRUE,
            in_progress = FALSE,
            posted_at = NOW()
        WHERE id = %(post_id)s;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"post_id": post_id})
            conn.commit()

    def release_claim(self, post_id: int) -> None:
        sql = """
        UPDATE posts
        SET in_progress = FALSE
        WHERE id = %(post_id)s
          AND posted = FALSE;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"post_id": post_id})
            conn.commit()

    def fetch_active_schedules(self, default_timezone: str) -> List[ScheduleRow]:
        sql = """
        SELECT id, target_chat_id, cron_expr, timezone, active
        FROM channel_schedules
        WHERE active = TRUE
        ORDER BY id;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        schedules: List[ScheduleRow] = []
        for row in rows:
            schedules.append(
                ScheduleRow(
                    id=row["id"],
                    target_chat_id=row["target_chat_id"],
                    cron_expr=row["cron_expr"],
                    timezone=(row["timezone"] or default_timezone),
                    active=row["active"],
                )
            )
        return schedules

    def upsert_schedule(self, target_chat_id: int, cron_expr: str, timezone: str, active: bool = True) -> int:
        sql = """
        INSERT INTO channel_schedules (
            target_chat_id,
            cron_expr,
            timezone,
            active
        )
        VALUES (
            %(target_chat_id)s,
            %(cron_expr)s,
            %(timezone)s,
            %(active)s
        )
        ON CONFLICT (target_chat_id, cron_expr, timezone)
        DO UPDATE SET active = EXCLUDED.active
        RETURNING id;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    {
                        "target_chat_id": target_chat_id,
                        "cron_expr": cron_expr,
                        "timezone": timezone,
                        "active": active,
                    },
                )
                row = cur.fetchone()
            conn.commit()
        return int(row["id"])

    def fetch_all_schedules(self, default_timezone: str) -> List[ScheduleRow]:
        sql = """
        SELECT id, target_chat_id, cron_expr, timezone, active
        FROM channel_schedules
        ORDER BY id;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()

        schedules: List[ScheduleRow] = []
        for row in rows:
            schedules.append(
                ScheduleRow(
                    id=row["id"],
                    target_chat_id=row["target_chat_id"],
                    cron_expr=row["cron_expr"],
                    timezone=(row["timezone"] or default_timezone),
                    active=row["active"],
                )
            )
        return schedules

    def get_dashboard_snapshot(self, recent_limit: int = 20) -> Dict[str, Any]:
        stats_sql = """
        SELECT
            COUNT(*) AS total_posts,
            COUNT(*) FILTER (WHERE posted = FALSE) AS unposted_posts,
            COUNT(*) FILTER (WHERE posted = TRUE) AS posted_posts,
            COUNT(*) FILTER (WHERE in_progress = TRUE) AS in_progress_posts,
            COUNT(DISTINCT target_chat_id) AS target_count
        FROM posts;
        """

        recent_sql = """
        SELECT
            id,
            target_chat_id,
            content_type,
            posted,
            in_progress,
            created_at,
            posted_at,
            LEFT(content, 120) AS preview
        FROM posts
        ORDER BY id DESC
        LIMIT %(recent_limit)s;
        """

        schedules_sql = """
        SELECT
            COUNT(*) FILTER (WHERE active = TRUE) AS active_schedules,
            COUNT(*) AS total_schedules
        FROM channel_schedules;
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(stats_sql)
                stats_row = cur.fetchone() or {}
                cur.execute(recent_sql, {"recent_limit": recent_limit})
                recent_rows = cur.fetchall() or []
                cur.execute(schedules_sql)
                schedules_row = cur.fetchone() or {}

        return {
            "stats": stats_row,
            "recent": recent_rows,
            "schedules": schedules_row,
        }
