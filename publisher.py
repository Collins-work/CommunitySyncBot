from __future__ import annotations

import json
import logging
from typing import Any, Dict

from telegram import Bot

from db import PostRepository


logger = logging.getLogger(__name__)


class PublisherService:
    def __init__(self, repo: PostRepository) -> None:
        self.repo = repo

    async def publish_next_for_chat(self, bot: Bot, target_chat_id: int) -> bool:
        post = self.repo.claim_next_unposted_post(target_chat_id)
        if not post:
            logger.info("No unposted content for chat_id=%s", target_chat_id)
            return False

        post_id = int(post["id"])

        try:
            await self._send_post(bot, target_chat_id, post)
            self.repo.mark_posted(post_id)
            logger.info("Posted id=%s to chat_id=%s", post_id, target_chat_id)
            return True
        except Exception:
            self.repo.release_claim(post_id)
            logger.exception("Failed posting id=%s to chat_id=%s", post_id, target_chat_id)
            raise

    async def _send_post(self, bot: Bot, target_chat_id: int, post: Dict[str, Any]) -> None:
        source_chat_id = post.get("source_chat_id")
        source_message_id = post.get("source_message_id")
        if source_chat_id and source_message_id:
            await bot.copy_message(
                chat_id=target_chat_id,
                from_chat_id=int(source_chat_id),
                message_id=int(source_message_id),
            )
            return

        content_type = (post.get("content_type") or "text").lower()

        if content_type == "text":
            await bot.send_message(chat_id=target_chat_id, text=post["content"])
            return

        if content_type == "image":
            media_url = post.get("media_url")
            if not media_url:
                raise ValueError("image post requires media_url")
            caption = post.get("caption") or post.get("content")
            await bot.send_photo(chat_id=target_chat_id, photo=media_url, caption=caption)
            return

        if content_type == "video":
            media_url = post.get("media_url")
            if not media_url:
                raise ValueError("video post requires media_url")
            caption = post.get("caption") or post.get("content")
            await bot.send_video(chat_id=target_chat_id, video=media_url, caption=caption)
            return

        if content_type == "audio":
            media_url = post.get("media_url")
            if not media_url:
                raise ValueError("audio post requires media_url")
            caption = post.get("caption") or post.get("content")
            await bot.send_audio(chat_id=target_chat_id, audio=media_url, caption=caption)
            return

        if content_type == "document":
            media_url = post.get("media_url")
            if not media_url:
                raise ValueError("document post requires media_url")
            caption = post.get("caption") or post.get("content")
            await bot.send_document(chat_id=target_chat_id, document=media_url, caption=caption)
            return

        if content_type == "animation":
            media_url = post.get("media_url")
            if not media_url:
                raise ValueError("animation post requires media_url")
            caption = post.get("caption") or post.get("content")
            await bot.send_animation(chat_id=target_chat_id, animation=media_url, caption=caption)
            return

        if content_type == "voice":
            media_url = post.get("media_url")
            if not media_url:
                raise ValueError("voice post requires media_url")
            await bot.send_voice(chat_id=target_chat_id, voice=media_url, caption=post.get("caption"))
            return

        if content_type == "video_note":
            media_url = post.get("media_url")
            if not media_url:
                raise ValueError("video_note post requires media_url")
            await bot.send_video_note(chat_id=target_chat_id, video_note=media_url)
            return

        if content_type == "sticker":
            media_url = post.get("media_url")
            if not media_url:
                raise ValueError("sticker post requires media_url")
            await bot.send_sticker(chat_id=target_chat_id, sticker=media_url)
            return

        if content_type == "poll":
            question = post.get("poll_question") or post.get("content")
            options_raw = post.get("poll_options")

            options = options_raw
            if isinstance(options_raw, str):
                options = json.loads(options_raw)

            if not question or not options or not isinstance(options, list):
                raise ValueError("poll post requires poll_question/content and poll_options array")

            await bot.send_poll(chat_id=target_chat_id, question=question, options=options)
            return

        raise ValueError(f"Unsupported content_type: {content_type}")
