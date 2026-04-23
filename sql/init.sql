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
