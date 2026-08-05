"""S1 cluster mixin: TelegramTypingMixin."""

from __future__ import annotations

import asyncio
from typing import Dict, List, Optional, Set, Any
from plugins.platforms.telegram.telegram_ids import (
    normalize_telegram_chat_id,
)

from ..adapter import (
    _redact_telegram_error_text,
    logger,
)

class TelegramTypingMixin:
    @staticmethod
    def _is_transient_typing_error(exc: Exception) -> bool:
        """Return True for Telegram typing errors worth cooling down."""
        retry_after = getattr(exc, "retry_after", None)
        if retry_after is not None:
            return True

        status_code = getattr(exc, "status_code", None) or getattr(exc, "code", None)
        if isinstance(status_code, int) and (status_code == 429 or status_code >= 500):
            return True

        text = str(exc).lower()
        if any(marker in text for marker in ("too many requests", "rate limit", "timed out", "timeout", "temporar")):
            return True
        if isinstance(exc, (OSError, TimeoutError, ConnectionError, asyncio.TimeoutError)):
            return True
        return False

    def _record_typing_cooldown(self, chat_id: str, exc: Exception) -> None:
        """Suppress Telegram typing refreshes for this chat after transient failures."""
        if not hasattr(self, "_telegram_typing_cooldown_until"):
            self._telegram_typing_cooldown_until = {}
        loop = asyncio.get_running_loop()
        retry_after = getattr(exc, "retry_after", None)
        try:
            delay = float(retry_after) if retry_after is not None else self._telegram_typing_cooldown_seconds
        except (TypeError, ValueError):
            delay = self._telegram_typing_cooldown_seconds
        delay = max(1.0, min(delay, 300.0))
        self._telegram_typing_cooldown_until[str(chat_id)] = loop.time() + delay

    def _typing_in_cooldown(self, chat_id: str) -> bool:
        if not hasattr(self, "_telegram_typing_cooldown_until"):
            self._telegram_typing_cooldown_until = {}
            self._telegram_typing_cooldown_seconds = 30.0
        until = self._telegram_typing_cooldown_until.get(str(chat_id))
        if until is None:
            return False
        if asyncio.get_running_loop().time() < until:
            return True
        self._telegram_typing_cooldown_until.pop(str(chat_id), None)
        return False

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Send typing indicator."""
        if not self._bot or self._typing_in_cooldown(chat_id):
            return

        _is_dm_topic: bool = False
        message_thread_id: Optional[int] = None
        try:
            _typing_thread = self._metadata_thread_id(metadata)
            _is_dm_topic = bool(metadata and metadata.get("telegram_dm_topic_reply_fallback"))
            message_thread_id = self._message_thread_id_for_typing(_typing_thread)
            await self._bot.send_chat_action(
                chat_id=normalize_telegram_chat_id(chat_id),
                action="typing",
                message_thread_id=message_thread_id,
            )
            self._telegram_typing_cooldown_until.pop(str(chat_id), None)
        except Exception as e:
            # For DM topic lanes, Telegram may reject message_thread_id.
            # Fall back to sending typing without thread_id so the typing
            # indicator at least appears in the main DM view.
            if _is_dm_topic and message_thread_id is not None:
                try:
                    await self._bot.send_chat_action(
                        chat_id=normalize_telegram_chat_id(chat_id),
                        action="typing",
                    )
                    self._telegram_typing_cooldown_until.pop(str(chat_id), None)
                    return
                except Exception as fallback_exc:
                    if self._is_transient_typing_error(fallback_exc):
                        self._record_typing_cooldown(chat_id, fallback_exc)
            elif self._is_transient_typing_error(e):
                self._record_typing_cooldown(chat_id, e)
            # Typing failures are non-fatal; log at debug level only.
            logger.debug(
                "[%s] Failed to send Telegram typing indicator: %s",
                self.name,
                _redact_telegram_error_text(e),
                exc_info=True,
            )
