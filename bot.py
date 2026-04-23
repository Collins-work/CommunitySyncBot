from __future__ import annotations

import logging
from html import escape
from typing import Callable, Coroutine, Any
from functools import wraps

from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeDefault,
    Message,
    Update,
)
from telegram.constants import ChatType, ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

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

PRIVATE_SHORTCUT_KEYBOARD = InlineKeyboardMarkup(
    [
        [
            InlineKeyboardButton("Who am I", callback_data="quick:whoami"),
            InlineKeyboardButton("Chat ID", callback_data="quick:chatid"),
        ],
        [
            InlineKeyboardButton("Bulk status", callback_data="quick:bulkstatus"),
            InlineKeyboardButton("Stop bulk", callback_data="quick:queuebulkstop"),
        ],
        [
            InlineKeyboardButton("Reload schedules", callback_data="quick:reloadschedules"),
            InlineKeyboardButton("Refresh panel", callback_data="quick:start"),
        ],
    ]
)


def html_code(value: object) -> str:
    return f"<code>{escape(str(value))}</code>"


def format_schedule_row(row: object) -> str:
    active = getattr(row, "active", True)
    status = "ON" if active else "OFF"
    return (
        f"ID {html_code(getattr(row, 'id', '?'))} | "
        f"chat {html_code(getattr(row, 'target_chat_id', '?'))} | "
        f"cron {html_code(getattr(row, 'cron_expr', '?'))} | "
        f"tz {html_code(getattr(row, 'timezone', '?'))} | "
        f"{html_code(status)}"
    )


async def send_start_panel(message: Message, private_chat: bool = False) -> None:
    if private_chat:
        await message.reply_text(
            "<b>CommunitySyncBot</b>\n"
            "Ready to queue posts, publish them on schedule, and keep group chats command-only.\n\n"
            "<b>Quick actions</b>\n"
            "• /whoami - show your Telegram user ID\n"
            "• /chatid - show the current chat ID\n"
            "• /queue &lt;chat_id&gt; - queue the message you replied to\n"
            "• /queuebulk &lt;chat_id&gt; - enable bulk queue mode\n"
            "• /postnow &lt;chat_id&gt; - publish the next queued post now\n"
            "• /reloadschedules - refresh schedule jobs from the database\n\n"
            "<b>Tip</b>\n"
            "Use the buttons below for one-tap actions. Group chats stay command-only.",
            parse_mode=ParseMode.HTML,
            reply_markup=PRIVATE_SHORTCUT_KEYBOARD,
            disable_web_page_preview=True,
        )
        return

    await message.reply_text(
        "<b>CommunitySyncBot</b>\n"
        "Group mode is command-only. Use /chatid, /queue, /postnow, or /reloadschedules as needed.",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


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
    chat = update.effective_chat
    await send_start_panel(update.effective_message, private_chat=bool(chat and chat.type == ChatType.PRIVATE))


async def quick_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.message:
        return

    await query.answer()
    action = (query.data or "").removeprefix("quick:")
    message = query.message

    if action == "start":
        await send_start_panel(message, private_chat=True)
        return

    if action == "whoami":
        user = query.from_user
        if not user:
            return
        await message.reply_text(
            f"<b>Your Telegram user ID</b>\n{html_code(user.id)}",
            parse_mode=ParseMode.HTML,
        )
        return

    if action == "chatid":
        chat = message.chat
        await message.reply_text(
            f"<b>Chat details</b>\nCurrent chat ID: {html_code(chat.id)}\nChat type: {html_code(chat.type)}",
            parse_mode=ParseMode.HTML,
        )
        return

    if action == "bulkstatus":
        target_chat_id = context.user_data.get(USER_BULK_TARGET_KEY)
        if target_chat_id is None:
            await message.reply_text("<b>Bulk queue mode</b>\nCurrently OFF.", parse_mode=ParseMode.HTML)
            return
        await message.reply_text(
            f"<b>Bulk queue mode</b>\nCurrently ON for target chat {html_code(target_chat_id)}.",
            parse_mode=ParseMode.HTML,
        )
        return

    if action == "queuebulkstop":
        if USER_BULK_TARGET_KEY in context.user_data:
            context.user_data.pop(USER_BULK_TARGET_KEY, None)
            await message.reply_text("<b>Bulk queue mode disabled</b>", parse_mode=ParseMode.HTML)
            return
        await message.reply_text("<b>Bulk queue mode was not active</b>", parse_mode=ParseMode.HTML)
        return

    if action == "reloadschedules":
        schedule_manager: ScheduleManager = context.application.bot_data["schedule_manager"]
        schedule_manager.reload_jobs()
        await message.reply_text(
            "<b>Schedules reloaded</b>\nThe scheduler jobs were refreshed from the database.",
            parse_mode=ParseMode.HTML,
        )
        return

    await message.reply_text("<b>Unknown quick action</b>", parse_mode=ParseMode.HTML)


async def whoami_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.effective_message:
        return
    await update.effective_message.reply_text(
        f"<b>Your Telegram user ID</b>\n{html_code(user.id)}",
        parse_mode=ParseMode.HTML,
    )


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
        f"<b>Chat details</b>\n"
        f"Current chat ID: {html_code(chat.id)}\n"
        f"Chat type: {html_code(chat.type)}{reply_info}",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def postnow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_message:
        return

    if not context.args:
        await update.effective_message.reply_text(
            "<b>Usage</b>\n/postnow &lt;chat_id&gt;",
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        target_chat_id = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text(
            "<b>Invalid chat ID</b>\nchat_id must be an integer (channel or group id).",
            parse_mode=ParseMode.HTML,
        )
        return

    publisher: PublisherService = context.application.bot_data["publisher"]
    posted = await publisher.publish_next_for_chat(context.application.bot, target_chat_id)
    if posted:
        await update.effective_message.reply_text(
            f"<b>Published</b>\nPosted the next queued message to {html_code(target_chat_id)}.",
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.effective_message.reply_text(
            f"<b>No work to publish</b>\nNo unposted content found for {html_code(target_chat_id)}.",
            parse_mode=ParseMode.HTML,
        )


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
        await message.reply_text(
            "<b>Usage</b>\n/queue &lt;chat_id&gt; as a reply to the post you want to queue.",
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        target_chat_id = int(context.args[0])
    except ValueError:
        await message.reply_text(
            "<b>Invalid chat ID</b>\nchat_id must be an integer (channel or group id).",
            parse_mode=ParseMode.HTML,
        )
        return

    source_message = message.reply_to_message
    if not source_message:
        await message.reply_text(
            "<b>Queue needs a reply</b>\nReply to the exact message or media you want posted, then run /queue &lt;chat_id&gt;.",
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        post_id = await queue_message_for_target(message, source_message, target_chat_id, context)
    except ValueError:
        await message.reply_text(
            "<b>Unsupported message type</b>\nTry text, photo, video, audio, document, sticker, voice, video note, or poll.",
            parse_mode=ParseMode.HTML,
        )
        return

    await message.reply_text(
        f"<b>Queued</b>\nPost {html_code(post_id)} is ready for {html_code(target_chat_id)}. It will be copied in the exact format during auto-post.",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def queuebulk_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return

    if not context.args:
        await message.reply_text(
            "<b>Usage</b>\n/queuebulk &lt;chat_id&gt;",
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        target_chat_id = int(context.args[0])
    except ValueError:
        await message.reply_text(
            "<b>Invalid chat ID</b>\nchat_id must be an integer (channel or group id).",
            parse_mode=ParseMode.HTML,
        )
        return

    context.user_data[USER_BULK_TARGET_KEY] = target_chat_id
    await message.reply_text(
        f"<b>Bulk queue mode enabled</b>\nTarget chat: {html_code(target_chat_id)}\nSend messages or media to this bot and each one will be queued automatically. Use /queuebulkstop to end.",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def queuebulkstop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return

    if USER_BULK_TARGET_KEY in context.user_data:
        context.user_data.pop(USER_BULK_TARGET_KEY, None)
        await message.reply_text("<b>Bulk queue mode disabled</b>", parse_mode=ParseMode.HTML)
        return

    await message.reply_text("<b>Bulk queue mode was not active</b>", parse_mode=ParseMode.HTML)


@admin_only
async def bulkstatus_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return

    target_chat_id = context.user_data.get(USER_BULK_TARGET_KEY)
    if target_chat_id is None:
        await message.reply_text("<b>Bulk queue mode</b>\nCurrently OFF.", parse_mode=ParseMode.HTML)
        return

    await message.reply_text(
        f"<b>Bulk queue mode</b>\nCurrently ON for target chat {html_code(target_chat_id)}.",
        parse_mode=ParseMode.HTML,
    )


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

    await message.reply_text(
        f"<b>Queued in bulk mode</b>\nPost {html_code(post_id)} is ready for {html_code(target_chat_id)}.",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def reloadschedules_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_message:
        return

    schedule_manager: ScheduleManager = context.application.bot_data["schedule_manager"]
    schedule_manager.reload_jobs()
    await update.effective_message.reply_text(
        "<b>Schedules reloaded</b>\nThe scheduler jobs were refreshed from the database.",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def setschedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return

    if len(context.args) < 6:
        await message.reply_text(
            "<b>Usage</b>\n"
            "/setschedule &lt;chat_id&gt; &lt;minute&gt; &lt;hour&gt; &lt;day-of-month&gt; &lt;month&gt; &lt;day-of-week&gt; [timezone]\n\n"
            "<b>Example</b>\n"
            "/setschedule -1001234567890 0 9 * * 1-5 Europe/London",
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        target_chat_id = int(context.args[0])
    except ValueError:
        await message.reply_text("<b>Invalid chat ID</b>", parse_mode=ParseMode.HTML)
        return

    minute, hour, day_of_month, month, day_of_week = context.args[1:6]
    timezone = context.args[6] if len(context.args) > 6 else context.application.bot_data["settings"].default_timezone
    cron_expr = f"{minute} {hour} {day_of_month} {month} {day_of_week}"

    repo: PostRepository = context.application.bot_data["repo"]
    schedule_id = repo.upsert_schedule(target_chat_id, cron_expr, timezone, active=True)

    schedule_manager: ScheduleManager = context.application.bot_data["schedule_manager"]
    schedule_manager.reload_jobs()

    await message.reply_text(
        f"<b>Schedule saved</b>\n"
        f"Schedule ID: {html_code(schedule_id)}\n"
        f"Chat: {html_code(target_chat_id)}\n"
        f"Cron: {html_code(cron_expr)}\n"
        f"Timezone: {html_code(timezone)}",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def listschedules_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return

    repo: PostRepository = context.application.bot_data["repo"]
    settings: Settings = context.application.bot_data["settings"]
    schedules = repo.fetch_all_schedules(settings.default_timezone)

    if not schedules:
        await message.reply_text("<b>No schedules found</b>", parse_mode=ParseMode.HTML)
        return

    lines = ["<b>Schedules</b>"]
    for row in schedules:
        lines.append(format_schedule_row(row))

    await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)


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
        await app.bot.set_my_commands(
            [
                BotCommand("start", "Show quick actions"),
                BotCommand("whoami", "Show your Telegram user ID"),
                BotCommand("chatid", "Show the current chat ID"),
                BotCommand("queue", "Queue a replied message"),
                BotCommand("queuebulk", "Enable bulk queue mode"),
                BotCommand("queuebulkstop", "Disable bulk queue mode"),
                BotCommand("bulkstatus", "Show bulk queue status"),
                BotCommand("postnow", "Publish next queued post"),
                BotCommand("setschedule", "Set a posting schedule for a chat"),
                BotCommand("listschedules", "List configured schedules"),
                BotCommand("reloadschedules", "Reload schedules from DB"),
            ],
            scope=BotCommandScopeDefault(),
        )
        await app.bot.set_my_commands(
            [
                BotCommand("start", "Show quick actions"),
                BotCommand("whoami", "Show your Telegram user ID"),
                BotCommand("chatid", "Show the current chat ID"),
                BotCommand("queue", "Queue a replied message"),
                BotCommand("queuebulk", "Enable bulk queue mode"),
                BotCommand("queuebulkstop", "Disable bulk queue mode"),
                BotCommand("bulkstatus", "Show bulk queue status"),
                BotCommand("postnow", "Publish next queued post"),
                BotCommand("setschedule", "Set a posting schedule for a chat"),
                BotCommand("listschedules", "List configured schedules"),
                BotCommand("reloadschedules", "Reload schedules from DB"),
            ],
            scope=BotCommandScopeAllPrivateChats(),
        )
        await app.bot.set_my_commands(
            [
                BotCommand("chatid", "Show the current chat ID"),
                BotCommand("queue", "Queue a replied message"),
                BotCommand("postnow", "Publish next queued post"),
                BotCommand("setschedule", "Set a posting schedule for a chat"),
                BotCommand("listschedules", "List configured schedules"),
                BotCommand("reloadschedules", "Reload schedules from DB"),
            ],
            scope=BotCommandScopeAllGroupChats(),
        )
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
    application.add_handler(CommandHandler("setschedule", setschedule_cmd))
    application.add_handler(CommandHandler("listschedules", listschedules_cmd))
    application.add_handler(CommandHandler("reloadschedules", reloadschedules_cmd))
    application.add_handler(CallbackQueryHandler(quick_action_callback, pattern=r"^quick:"))
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, bulk_capture_message))

    logger.info("Starting bot polling")
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
