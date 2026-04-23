# CommunitySyncBot (Telegram + PostgreSQL)

Python Telegram bot that auto-publishes queued posts to multiple channels and groups.

## Features

- Uses BotFather token to connect to Telegram Bot API
- PostgreSQL-backed post queue (`posts` table) with `id`, `content`, `posted`
- Prevents repeat posting by only selecting `posted = false` rows
- Supports multiple targets using `target_chat_id` per post (channels or groups)
- Scheduler via cron expressions in DB (`channel_schedules` table)
- Admin-only bot commands via your Telegram user ID (`ADMIN_TELEGRAM_IDS`)
- Exact-format posting flow: queue pasted/replied Telegram messages and publish with `copy_message`
- Rich content support: text, image, video, audio, document, animation, voice, sticker, poll

## Project Files

- `bot.py` - Telegram bot app + commands + startup
- `db.py` - PostgreSQL repository and schema init
- `publisher.py` - dispatch logic for next unposted post
- `scheduler_service.py` - APScheduler cron job manager
- `config.py` - env configuration
- `sql/init.sql` - schema
- `sql/sample_data.sql` - seed examples

## Requirements

- Python 3.10+
- PostgreSQL 13+
- Bot added as admin in each destination channel/group

## Setup

1. Create and activate virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy `.env.example` to `.env` and fill values:

```env
BOT_TOKEN=<botfather_token>
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/community_sync_bot
ADMIN_TELEGRAM_IDS=<your_numeric_telegram_user_id>
DEFAULT_TIMEZONE=UTC
SCHEDULE_REFRESH_SECONDS=120
```

4. Create DB and schema:

```sql
CREATE DATABASE community_sync_bot;
```

Then run `sql/init.sql` on that database.

5. (Optional) Insert test data from `sql/sample_data.sql`.

6. Start bot:

```bash
python bot.py
```

## Bot Commands

- `/whoami` - returns your Telegram user ID
- `/chatid` - admin-only, shows the current chat ID and chat type
- `/start` - admin-only status command
- `/queue <chat_id>` - admin-only, reply to a pasted message/media to queue it for exact-format auto-post
- `/queuebulk <chat_id>` - admin-only, start bulk queue mode for this target
- `/queuebulkstop` - admin-only, stop bulk queue mode
- `/bulkstatus` - admin-only, show whether bulk queue mode is active
- `/postnow <chat_id>` - admin-only immediate publish of next queued post for target
- `/setschedule <chat_id> <minute> <hour> <day-of-month> <month> <day-of-week> [timezone]` - admin-only set a per-chat schedule
- `/listschedules` - admin-only list configured schedules
- `/reloadschedules` - admin-only force schedule reload from DB

## Chat UX

- In private chat, `/start` shows a cleaner welcome card with command shortcuts.
- In groups, the bot stays command-only so the chat does not get button clutter.
- Bot commands are scoped so Telegram shows a tailored menu in private chat and group chat.
- Schedules are per chat, so different channels or groups can have different posting times.

## Queueing Posts Exactly As Pasted

1. Send or paste the content to the bot chat (text, image, video, audio, document, sticker, voice, poll).
2. Reply to that message with `/queue <chat_id>`.
3. Bot stores the source message reference in PostgreSQL.
4. On schedule (or `/postnow`), bot uses Telegram `copy_message` to publish in the same format.

## Setting Per-Chat Schedules

Use `/setschedule` to define a cron-based posting time for a specific channel or group:

```text
/setschedule -1001234567890 0 9 * * 1-5 Europe/London
```

That example posts to the target chat at 9:00 AM Monday through Friday in `Europe/London`.

Use `/listschedules` to verify the configured timing for each chat.

## Bulk Queue Mode

Use this when you want to paste many posts quickly:

1. Send `/queuebulk <chat_id>`.
2. Paste/send multiple posts to the bot chat; each message is auto-queued.
3. Send `/queuebulkstop` when done.

## How Posting Works

1. Scheduler triggers for a configured `target_chat_id`.
2. Bot claims next row where `posted=false` and `in_progress=false`.
3. Bot copies and sends the original queued Telegram message to the target.
4. If successful, row is updated to `posted=true`.
5. If failed, claim is released (`in_progress=false`) so it can retry later.

## Multi-Channel and Group Support

Use Telegram chat IDs in `target_chat_id`:

- Channels typically: `-100...`
- Groups/supergroups: negative IDs

Each post is routed by its own `target_chat_id`, so different content can go to different channels/groups.

## Extending Rich Content

Supported queued message types include text, images, videos, audio, documents, animations, voice, stickers, and polls.

Legacy DB-seeded mode is still supported via `content_type` + `media_url` fields for scripted inserts.

## Important Notes

- Bot must be admin in each channel/group to post.
- If running multiple bot instances, `FOR UPDATE SKIP LOCKED` + `in_progress` helps avoid duplicate dispatch.
- Keep your `BOT_TOKEN` private.

## Admin Dashboard Script

Run a quick PostgreSQL status snapshot:

```bash
python admin_dashboard.py
```

It prints counts of queued/posted posts, schedules, and recent posts.
