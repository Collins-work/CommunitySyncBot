from __future__ import annotations

import logging
from typing import Callable, Coroutine, Any
from functools import wraps

from telegram import Message, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from config import Settings
from db import PostRepository
from publisher import PublisherService
from scheduler_service import ScheduleManager


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

USER_BULK_TARGET_KEY = "bulk_target_chat_id"


def admin_only(handler: Callable[[Update, ContextTypes.DEFAULT_TYPE], Coroutine[Any, Any, None]]):
    @wraps(handler)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        settings: Settings = context.application.bot_data["settings"]
        user = update.effective_user
        if not user or user.id not in settings.admin_telegram_ids:
            if update.effective_message:
                await update.effective_message.reply_text("Unauthorized. Your Telegram ID is not in ADMIN_TELEGRAM_IDS.")
            return
        await handler(update, context)

    return wrapped


@admin_only
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_message:
        return
    await update.effective_message.reply_text(
        "Bot is running. Use /whoami for your Telegram ID, /queue <chat_id> as a reply to content you pasted, and /postnow <chat_id> for immediate publish."
    )


async def whoami_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.effective_message:
        return
    await update.effective_message.reply_text(f"Your Telegram user ID: {user.id}")


@admin_only
async def chatid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat:
        return

    reply_info = ""
    if message.reply_to_message and message.reply_to_message.chat:
        reply_info = f"\nReplied message chat ID: {message.reply_to_message.chat.id}"

    await message.reply_text(
        f"Current chat ID: {chat.id}\nChat type: {chat.type}{reply_info}"
    )


@admin_only
async def postnow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_message:
        return

    if not context.args:
        await update.effective_message.reply_text("Usage: /postnow <chat_id>")
        return

    try:
        target_chat_id = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("chat_id must be an integer (channel/group id)")
        return

    publisher: PublisherService = context.application.bot_data["publisher"]
    posted = await publisher.publish_next_for_chat(context.application.bot, target_chat_id)
    if posted:
        await update.effective_message.reply_text(f"Posted next queued message to {target_chat_id}")
    else:
        await update.effective_message.reply_text(f"No unposted content found for {target_chat_id}")


def detect_message_content_type(message: Message) -> str:
    if message.text:
        return "text"
    if message.photo:
        return "image"
    if message.video:
        return "video"
    if message.audio:
        return "audio"
    if message.document:
        return "document"
    if message.animation:
        return "animation"
    if message.voice:
        return "voice"
    if message.video_note:
        return "video_note"
    if message.sticker:
        return "sticker"
    if message.poll:
        return "poll"
    return "unknown"


async def queue_message_for_target(
    message: Message,
    source_message: Message,
    target_chat_id: int,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    content_type = detect_message_content_type(source_message)
    if content_type == "unknown":
        raise ValueError("Unsupported message type")

    if not source_message.chat:
        raise ValueError("Could not read source chat")

    preview = source_message.text or source_message.caption or f"[{content_type}]"
    preview = preview.strip() if isinstance(preview, str) else f"[{content_type}]"
    if not preview:
        preview = f"[{content_type}]"

    repo: PostRepository = context.application.bot_data["repo"]
    post_id = repo.queue_telegram_message(
        target_chat_id=target_chat_id,
        source_chat_id=int(source_message.chat.id),
        source_message_id=int(source_message.message_id),
        content_preview=preview,
        content_type=content_type,
    )
    return post_id


@admin_only
async def queue_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return

    if not context.args:
        await message.reply_text("Usage: /queue <chat_id> (as a reply to the post you want to queue)")
        return

    try:
        target_chat_id = int(context.args[0])
    except ValueError:
        await message.reply_text("chat_id must be an integer (channel/group id)")
        return

    source_message = message.reply_to_message
    if not source_message:
        await message.reply_text("Reply to the exact message/media you want posted, then run /queue <chat_id>.")
        return

    try:
        post_id = await queue_message_for_target(message, source_message, target_chat_id, context)
    except ValueError:
        await message.reply_text("Unsupported message type. Try text, photo, video, audio, document, sticker, voice, video note, or poll.")
        return

    await message.reply_text(f"Queued post id={post_id} for {target_chat_id}. It will be copied in the exact format during auto-post.")


@admin_only
async def queuebulk_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return

    if not context.args:
        await message.reply_text("Usage: /queuebulk <chat_id>")
        return

    try:
        target_chat_id = int(context.args[0])
    except ValueError:
        await message.reply_text("chat_id must be an integer (channel/group id)")
        return

    context.user_data[USER_BULK_TARGET_KEY] = target_chat_id
    await message.reply_text(
        f"Bulk queue mode enabled for {target_chat_id}. Send messages/media to this bot and each one will be queued automatically. Use /queuebulkstop to end."
    )


@admin_only
async def queuebulkstop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return

    if USER_BULK_TARGET_KEY in context.user_data:
        context.user_data.pop(USER_BULK_TARGET_KEY, None)
        await message.reply_text("Bulk queue mode disabled.")
        return

    await message.reply_text("Bulk queue mode was not active.")


@admin_only
async def bulkstatus_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return

    target_chat_id = context.user_data.get(USER_BULK_TARGET_KEY)
    if target_chat_id is None:
        await message.reply_text("Bulk queue mode is currently OFF.")
        return

    await message.reply_text(f"Bulk queue mode is ON for target chat {target_chat_id}.")


@admin_only
async def bulk_capture_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return

    target_chat_id = context.user_data.get(USER_BULK_TARGET_KEY)
    if target_chat_id is None:
        return

    if message.text and message.text.startswith("/"):
        return

    try:
        post_id = await queue_message_for_target(message, message, int(target_chat_id), context)
    except ValueError:
        return

    await message.reply_text(f"Queued in bulk mode as post id={post_id} for {target_chat_id}.")


@admin_only
async def reloadschedules_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_message:
        return

    schedule_manager: ScheduleManager = context.application.bot_data["schedule_manager"]
    schedule_manager.reload_jobs()
    await update.effective_message.reply_text("Schedules reloaded from database.")


def main() -> None:
    settings = Settings.from_env()

    repo = PostRepository(settings.database_url)
    repo.open()
    repo.init_schema()

    publisher = PublisherService(repo)

    async def post_init(app: Application) -> None:
        app.bot_data["settings"] = settings
        app.bot_data["repo"] = repo
        app.bot_data["publisher"] = publisher
        app.bot_data["schedule_manager"] = schedule_manager
        schedule_manager.start()

    async def post_shutdown(app: Application) -> None:
        schedule_manager.shutdown()
        repo.close()

    application = (
        Application.builder()
        .token(settings.bot_token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    schedule_manager = ScheduleManager(application, settings, repo, publisher)

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("whoami", whoami_cmd))
    application.add_handler(CommandHandler("chatid", chatid_cmd))
    application.add_handler(CommandHandler("queue", queue_cmd))
    application.add_handler(CommandHandler("queuebulk", queuebulk_cmd))
    application.add_handler(CommandHandler("queuebulkstop", queuebulkstop_cmd))
    application.add_handler(CommandHandler("bulkstatus", bulkstatus_cmd))
    application.add_handler(CommandHandler("postnow", postnow_cmd))
    application.add_handler(CommandHandler("reloadschedules", reloadschedules_cmd))
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, bulk_capture_message))

    logger.info("Starting bot polling")
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
