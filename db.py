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
            posted_chat_id BIGINT,
            posted_message_id BIGINT,
            view_count BIGINT NOT NULL DEFAULT 0,
            reaction_count BIGINT NOT NULL DEFAULT 0,
            last_engagement_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            posted_at TIMESTAMPTZ
        );

        CREATE INDEX IF NOT EXISTS idx_posts_dispatch
            ON posts (target_chat_id, posted, in_progress, id);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_posts_published_message
            ON posts (posted_chat_id, posted_message_id)
            WHERE posted_chat_id IS NOT NULL AND posted_message_id IS NOT NULL;

        CREATE TABLE IF NOT EXISTS post_engagement_logs (
            id BIGSERIAL PRIMARY KEY,
            post_id BIGINT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            views BIGINT,
            reactions BIGINT,
            actor_id BIGINT,
            payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_post_engagement_logs_post
            ON post_engagement_logs (post_id, created_at DESC);

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
        ALTER TABLE posts ADD COLUMN IF NOT EXISTS posted_chat_id BIGINT;
        ALTER TABLE posts ADD COLUMN IF NOT EXISTS posted_message_id BIGINT;
        ALTER TABLE posts ADD COLUMN IF NOT EXISTS view_count BIGINT NOT NULL DEFAULT 0;
        ALTER TABLE posts ADD COLUMN IF NOT EXISTS reaction_count BIGINT NOT NULL DEFAULT 0;
        ALTER TABLE posts ADD COLUMN IF NOT EXISTS last_engagement_at TIMESTAMPTZ;
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

    def mark_posted(
        self,
        post_id: int,
        posted_chat_id: Optional[int] = None,
        posted_message_id: Optional[int] = None,
        view_count: int = 0,
    ) -> None:
        sql = """
        UPDATE posts
        SET posted = TRUE,
            in_progress = FALSE,
            posted_chat_id = %(posted_chat_id)s,
            posted_message_id = %(posted_message_id)s,
            view_count = GREATEST(%(view_count)s, 0),
            posted_at = NOW()
        WHERE id = %(post_id)s;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    {
                        "post_id": post_id,
                        "posted_chat_id": posted_chat_id,
                        "posted_message_id": posted_message_id,
                        "view_count": view_count,
                    },
                )
            conn.commit()

    def upsert_view_snapshot(
        self,
        posted_chat_id: int,
        posted_message_id: int,
        view_count: int,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        sql = """
        UPDATE posts
        SET view_count = GREATEST(%(view_count)s, 0),
            last_engagement_at = NOW()
        WHERE posted_chat_id = %(posted_chat_id)s
          AND posted_message_id = %(posted_message_id)s
        RETURNING id;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    {
                        "posted_chat_id": posted_chat_id,
                        "posted_message_id": posted_message_id,
                        "view_count": view_count,
                    },
                )
                row = cur.fetchone()
                if row:
                    cur.execute(
                        """
                        INSERT INTO post_engagement_logs (post_id, event_type, views, payload)
                        VALUES (%(post_id)s, 'view_snapshot', %(views)s, %(payload)s);
                        """,
                        {
                            "post_id": row["id"],
                            "views": max(view_count, 0),
                            "payload": payload or {},
                        },
                    )
            conn.commit()
        return int(row["id"]) if row else None

    def apply_reaction_delta(
        self,
        posted_chat_id: int,
        posted_message_id: int,
        delta: int,
        actor_id: Optional[int] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        sql = """
        UPDATE posts
        SET reaction_count = GREATEST(reaction_count + %(delta)s, 0),
            last_engagement_at = NOW()
        WHERE posted_chat_id = %(posted_chat_id)s
          AND posted_message_id = %(posted_message_id)s
        RETURNING id, reaction_count;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    {
                        "posted_chat_id": posted_chat_id,
                        "posted_message_id": posted_message_id,
                        "delta": delta,
                    },
                )
                row = cur.fetchone()
                if row:
                    cur.execute(
                        """
                        INSERT INTO post_engagement_logs (post_id, event_type, reactions, actor_id, payload)
                        VALUES (%(post_id)s, 'reaction_delta', %(reactions)s, %(actor_id)s, %(payload)s);
                        """,
                        {
                            "post_id": row["id"],
                            "reactions": row["reaction_count"],
                            "actor_id": actor_id,
                            "payload": payload or {},
                        },
                    )
            conn.commit()
        return int(row["id"]) if row else None

    def upsert_reaction_snapshot(
        self,
        posted_chat_id: int,
        posted_message_id: int,
        reaction_count: int,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        sql = """
        UPDATE posts
        SET reaction_count = GREATEST(%(reaction_count)s, 0),
            last_engagement_at = NOW()
        WHERE posted_chat_id = %(posted_chat_id)s
          AND posted_message_id = %(posted_message_id)s
        RETURNING id;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    {
                        "posted_chat_id": posted_chat_id,
                        "posted_message_id": posted_message_id,
                        "reaction_count": reaction_count,
                    },
                )
                row = cur.fetchone()
                if row:
                    cur.execute(
                        """
                        INSERT INTO post_engagement_logs (post_id, event_type, reactions, payload)
                        VALUES (%(post_id)s, 'reaction_snapshot', %(reactions)s, %(payload)s);
                        """,
                        {
                            "post_id": row["id"],
                            "reactions": max(reaction_count, 0),
                            "payload": payload or {},
                        },
                    )
            conn.commit()
        return int(row["id"]) if row else None

    def fetch_engagement_summary(self, target_chat_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        sql = """
        SELECT
            id,
            posted_chat_id,
            posted_message_id,
            content_type,
            posted_at,
            view_count,
            reaction_count,
            COALESCE(view_count, 0) + (COALESCE(reaction_count, 0) * 5) AS engagement_score,
            LEFT(content, 120) AS preview
        FROM posts
        WHERE target_chat_id = %(target_chat_id)s
          AND posted = TRUE
        ORDER BY engagement_score DESC, id DESC
        LIMIT %(limit)s;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"target_chat_id": target_chat_id, "limit": limit})
                rows = cur.fetchall() or []
        return rows

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
            COUNT(DISTINCT target_chat_id) AS target_count,
            COALESCE(SUM(view_count), 0) AS total_views,
            COALESCE(SUM(reaction_count), 0) AS total_reactions
        FROM posts;
        """

        recent_sql = """
        SELECT
            id,
            target_chat_id,
            content_type,
            posted,
            in_progress,
            view_count,
            reaction_count,
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
