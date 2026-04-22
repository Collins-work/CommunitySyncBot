-- Replace target_chat_id values with your channel/group IDs.
-- Channel IDs usually look like -1001234567890.
-- Group IDs are negative integers.

-- Schedules: every day 09:00 UTC and 17:00 UTC for two targets
INSERT INTO channel_schedules (target_chat_id, cron_expr, timezone, active)
VALUES
    (-1001234567890, '0 9 * * *', 'UTC', TRUE),
    (-1002223334445, '0 17 * * *', 'UTC', TRUE)
ON CONFLICT DO NOTHING;

-- Text post
INSERT INTO posts (target_chat_id, content, content_type)
VALUES (-1001234567890, 'Good morning channel A', 'text');

-- Image post
INSERT INTO posts (target_chat_id, content, content_type, media_url, caption)
VALUES (
    -1001234567890,
    'Image fallback text',
    'image',
    'https://picsum.photos/1200/800',
    'Daily image update'
);

-- Video post
INSERT INTO posts (target_chat_id, content, content_type, media_url, caption)
VALUES (
    -1002223334445,
    'Video fallback text',
    'video',
    'https://example.com/video.mp4',
    'Weekly video update'
);

-- Poll post
INSERT INTO posts (target_chat_id, content, content_type, poll_question, poll_options)
VALUES (
    -1002223334445,
    'Poll fallback question',
    'poll',
    'Which topic should we post next?',
    '["Python", "DevOps", "AI"]'::jsonb
);
