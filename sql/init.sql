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
