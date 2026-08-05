"""S1 cluster mixin: TelegramMessagingMixin."""

from __future__ import annotations

import asyncio
import re
from typing import Dict, List, Optional, Set, Any
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    ProcessingOutcome,
    SendResult,
    classify_send_error,
    cache_image_from_bytes,
    cache_audio_from_bytes,
    cache_video_from_bytes,
    cache_document_from_bytes,
    resolve_proxy_url,
    SUPPORTED_VIDEO_TYPES,
    SUPPORTED_DOCUMENT_TYPES,
    SUPPORTED_IMAGE_DOCUMENT_TYPES,
    _TEXT_INJECT_EXTENSIONS,
    utf16_len,
)
from plugins.platforms.telegram.telegram_ids import (
    normalize_telegram_chat_id,
)

from ..adapter import (
    ParseMode,
    _redact_telegram_error_text,
    _separate_chunk_indicator_from_fence,
    _strip_mdv2,
    logger,
)

class TelegramMessagingMixin:
    def _should_thread_reply(self, reply_to: Optional[str], chunk_index: int) -> bool:
        """Determine if this message chunk should thread to the original message.

        Args:
            reply_to: The original message ID to reply to
            chunk_index: Index of this chunk (0 = first chunk)

        Returns:
            True if this chunk should be threaded to the original message
        """
        if not reply_to:
            return False
        mode = self._reply_to_mode
        if mode == "off":
            return False
        elif mode == "all":
            return True
        else:  # "first" (default)
            return chunk_index == 0

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> SendResult:
        """Send a message to a Telegram chat."""
        if not self._bot:
            return SendResult(success=False, error="Not connected")

        # getattr() — tests build adapters via object.__new__() (no __init__).
        if getattr(self, "_send_path_degraded", False):
            return SendResult(success=False, error="send_path_degraded", retryable=True)

        # Skip whitespace-only text to prevent Telegram 400 empty-text errors.
        if not content or not content.strip():
            return SendResult(success=True, message_id=None)

        try:
            # Bot API 10.1 rich fast-path: send the raw agent markdown via
            # sendRichMessage so tables/task lists/etc. render natively. Falls
            # through to the legacy MarkdownV2 path on permanent/capability
            # errors or DM-topic routing skips; returns directly on success or
            # on a transient failure (which must NOT be legacy-resent).
            if self._should_attempt_rich(content, metadata=metadata):
                rich_result = await self._try_send_rich(chat_id, content, reply_to, metadata)
                if rich_result is not None:
                    if rich_result.success:
                        # Re-trigger typing like the legacy success path does,
                        # but ONLY for intermediate sends. On the final reply
                        # (metadata["notify"]) the gateway has already torn down
                        # the typing refresh loop; re-arming Telegram's ~5s timer
                        # here would leave the "...typing" bubble lingering after
                        # the answer (no Bot API call cancels it). See #48678.
                        if not (metadata or {}).get("notify"):
                            try:
                                await self.send_typing(chat_id, metadata=metadata)
                            except Exception:
                                pass  # Typing failures are non-fatal
                    return rich_result

            # Format and split message if needed
            formatted = self.format_message(content)
            chunks = self.truncate_message(
                formatted, self.MAX_MESSAGE_LENGTH, len_fn=utf16_len,
            )
            if len(chunks) > 1:
                # truncate_message appends a raw " (1/2)" suffix. Escape the
                # MarkdownV2-special parentheses so Telegram doesn't reject the
                # chunk and fall back to plain text.
                chunks = [
                    _separate_chunk_indicator_from_fence(
                        re.sub(r" \((\d+)/(\d+)\)$", r" \\(\1/\2\\)", chunk)
                    )
                    for chunk in chunks
                ]

            message_ids = []
            thread_id = self._metadata_thread_id(metadata)
            requested_thread_id = self._message_thread_id_for_send(thread_id)
            used_thread_fallback = False

            try:
                from telegram.error import NetworkError as _NetErr
            except ImportError:
                _NetErr = OSError  # type: ignore[misc,assignment]

            try:
                from telegram.error import BadRequest as _BadReq
            except ImportError:
                _BadReq = None  # type: ignore[assignment,misc]

            try:
                from telegram.error import TimedOut as _TimedOut
            except (ImportError, AttributeError):
                _TimedOut = None  # type: ignore[assignment,misc]

            for i, chunk in enumerate(chunks):
                retried_thread_not_found = False
                metadata_reply_to = self._metadata_reply_to_message_id(metadata)
                private_dm_topic_send = self._is_private_dm_topic_send(chat_id, thread_id, metadata)
                # reply_to_mode="off" on the existing telegram_dm_topic_reply_fallback path
                # is an explicit user opt-in to "message_thread_id alone is enough" (PR #23994
                # / commit 21a15b671). Honor it — don't fail loud just because the anchor was
                # suppressed by config. The new fail-loud contract only applies when the caller
                # didn't ask for the anchor to be dropped.
                dm_topic_reply_to_off = (
                    private_dm_topic_send
                    and self._reply_to_mode == "off"
                    and bool(metadata and metadata.get("telegram_dm_topic_reply_fallback"))
                )
                reply_to_source = reply_to or (
                    str(metadata_reply_to) if private_dm_topic_send and metadata_reply_to is not None else None
                )
                if private_dm_topic_send:
                    should_thread = (
                        reply_to_source is not None
                        and self._reply_to_mode != "off"
                    )
                else:
                    should_thread = self._should_thread_reply(reply_to_source, i)
                reply_to_id = int(reply_to_source) if should_thread and reply_to_source else None
                if private_dm_topic_send and reply_to_id is None and not dm_topic_reply_to_off:
                    return SendResult(
                        success=False,
                        error=self._dm_topic_missing_anchor_error(),
                        retryable=False,
                    )
                thread_kwargs = self._thread_kwargs_for_send(
                    chat_id,
                    thread_id,
                    metadata,
                    reply_to_message_id=reply_to_id,
                    reply_to_mode=self._reply_to_mode,
                )
                if used_thread_fallback and thread_kwargs.get("message_thread_id") is not None:
                    thread_kwargs = dict(thread_kwargs)
                    thread_kwargs["message_thread_id"] = None
                effective_thread_id = thread_kwargs.get("message_thread_id")

                msg = None
                for _send_attempt in range(3):
                    try:
                        # Try Markdown first, fall back to plain text if it fails
                        try:
                            msg = await self._bot.send_message(
                                chat_id=normalize_telegram_chat_id(chat_id),
                                text=chunk,
                                parse_mode=ParseMode.MARKDOWN_V2,
                                reply_to_message_id=reply_to_id,
                                **thread_kwargs,
                                **self._link_preview_kwargs(),
                                **self._notification_kwargs(metadata),
                            )
                        except Exception as md_error:
                            # Markdown parsing failed, try plain text
                            if "parse" in str(md_error).lower() or "markdown" in str(md_error).lower():
                                logger.warning("[%s] MarkdownV2 parse failed, falling back to plain text: %s", self.name, md_error)
                                plain_chunk = _strip_mdv2(chunk)
                                msg = await self._bot.send_message(
                                    chat_id=normalize_telegram_chat_id(chat_id),
                                    text=plain_chunk,
                                    parse_mode=None,
                                    reply_to_message_id=reply_to_id,
                                    **thread_kwargs,
                                    **self._link_preview_kwargs(),
                                    **self._notification_kwargs(metadata),
                                )
                            else:
                                raise
                        break  # success
                    except _NetErr as send_err:
                        # BadRequest is a subclass of NetworkError in
                        # python-telegram-bot but represents permanent errors
                        # (not transient network issues). Detect and handle
                        # specific cases instead of blindly retrying.
                        if _BadReq and isinstance(send_err, _BadReq):
                            if self._is_thread_not_found_error(send_err) and effective_thread_id is not None:
                                if private_dm_topic_send or (metadata and metadata.get("telegram_dm_topic_created_for_send")):
                                    return SendResult(
                                        success=False,
                                        error=str(send_err),
                                        retryable=False,
                                    )
                                # Telegram has been observed to return a
                                # one-off "thread not found" that recovers on
                                # an immediate retry (transient flake — see
                                # test_send_retries_transient_thread_not_found_before_fallback).
                                # Try the same thread_id once without sleeping
                                # before falling back to a plain send.
                                if not retried_thread_not_found:
                                    retried_thread_not_found = True
                                    logger.warning(
                                        "[%s] Thread %s not found, retrying once with same thread_id",
                                        self.name, effective_thread_id,
                                    )
                                    continue
                                # Second failure: the thread is genuinely gone.
                                # Retry without ``message_thread_id`` so the
                                # message still reaches the chat, and prune
                                # the stale binding so future inbound
                                # messages aren't redirected back to it
                                # (#31501).
                                logger.warning(
                                    "[%s] Thread %s not found, retrying without message_thread_id",
                                    self.name, effective_thread_id,
                                )
                                self._prune_stale_dm_topic_binding(
                                    chat_id, effective_thread_id,
                                )
                                used_thread_fallback = True
                                effective_thread_id = None
                                thread_kwargs = {"message_thread_id": None}
                                continue
                            err_lower = str(send_err).lower()
                            if "message to be replied not found" in err_lower and reply_to_id is not None:
                                if private_dm_topic_send:
                                    safe_send_error = _redact_telegram_error_text(send_err)
                                    return SendResult(
                                        success=False,
                                        error=safe_send_error,
                                        retryable=False,
                                    )
                                # Original message was deleted before we
                                # could reply. For private-topic fallback
                                # sends, message_thread_id is only valid with
                                # the reply anchor, so drop both together.
                                safe_send_error = _redact_telegram_error_text(send_err)
                                logger.warning(
                                    "[%s] Reply target deleted, retrying without reply_to: %s",
                                    self.name, safe_send_error,
                                )
                                reply_to_id = None
                                if metadata and metadata.get("telegram_dm_topic_reply_fallback"):
                                    thread_kwargs = {}
                                    effective_thread_id = None
                                else:
                                    thread_kwargs = self._thread_kwargs_for_send(
                                        chat_id,
                                        thread_id,
                                        metadata,
                                        reply_to_message_id=reply_to_id,
                                        reply_to_mode=self._reply_to_mode,
                                    )
                                    effective_thread_id = thread_kwargs.get("message_thread_id")
                                continue
                            # Other BadRequest errors are permanent — don't retry
                            raise
                        # TimedOut is also a subclass of NetworkError. A
                        # generic timeout may have reached Telegram, so don't
                        # retry; a wrapped ConnectTimeout means no connection
                        # was established, so retrying is safe. A pool timeout
                        # (httpx pool exhausted) is explicitly "not sent to
                        # Telegram" -- retrying through the loop is safe and
                        # prevents silent drops when the pool frees up.
                        is_pool_timeout = self._looks_like_pool_timeout(send_err)
                        if (
                            _TimedOut
                            and isinstance(send_err, _TimedOut)
                            and not self._looks_like_connect_timeout(send_err)
                            and not is_pool_timeout
                        ):
                            raise
                        if is_pool_timeout:
                            await self._drain_general_connections_after_pool_timeout()
                        if _send_attempt < 2:
                            wait = 2 ** _send_attempt
                            safe_send_error = _redact_telegram_error_text(send_err)
                            logger.warning("[%s] Network error on send (attempt %d/3), retrying in %ds: %s",
                                           self.name, _send_attempt + 1, wait, safe_send_error)
                            await asyncio.sleep(wait)
                        else:
                            raise
                    except Exception as send_err:
                        retry_after = getattr(send_err, "retry_after", None)
                        if retry_after is not None or "retry after" in str(send_err).lower():
                            if _send_attempt < 2:
                                wait = float(retry_after) if retry_after is not None else 1.0
                                safe_send_error = _redact_telegram_error_text(send_err)
                                logger.warning(
                                    "[%s] Telegram flood control on send (attempt %d/3), retrying in %.1fs: %s",
                                    self.name,
                                    _send_attempt + 1,
                                    wait,
                                    safe_send_error,
                                )
                                await asyncio.sleep(wait)
                                continue
                        raise
                message_ids.append(str(msg.message_id))

            # Re-trigger typing indicator after sending a message.
            # Telegram clears the typing state when a new message is delivered,
            # so without this the "...typing" bubble disappears mid-response
            # (especially noticeable when the agent sends intermediate progress
            # messages like "Checking:" before running tools).
            # Skip this on the FINAL reply (metadata["notify"]): the gateway has
            # already cancelled the typing refresh loop by the time the final
            # send returns, so re-arming Telegram's ~5s timer here would leave
            # the indicator lingering after the answer with nothing to cancel
            # it (Telegram exposes no stop-typing API). See #48678.
            if not (metadata or {}).get("notify"):
                try:
                    await self.send_typing(chat_id, metadata=metadata)
                except Exception:
                    pass  # Typing failures are non-fatal

            return SendResult(
                success=True,
                message_id=message_ids[0] if message_ids else None,
                raw_response={
                    "message_ids": message_ids,
                    "requested_thread_id": requested_thread_id,
                    "thread_fallback": used_thread_fallback,
                },
            )

        except Exception as e:
            safe_error = _redact_telegram_error_text(e)
            logger.error("[%s] Failed to send Telegram message: %s", self.name, safe_error)
            err_str = str(e).lower()
            error_kind = classify_send_error(e)
            # Message too long — content exceeded 4096 chars. Return failure so
            # stream consumer enters fallback mode and sends the remainder.
            if "message_too_long" in err_str or "too long" in err_str:
                logger.debug(
                    "[%s] send() content too long, falling back to new-message continuation",
                    self.name,
                )
                return SendResult(success=False, error="message_too_long", error_kind="too_long")
            # TimedOut usually means the request may have reached Telegram —
            # mark as non-retryable so _send_with_retry() doesn't re-send.
            # Exceptions: a wrapped ConnectTimeout (no connection established)
            # and an httpx pool timeout (request explicitly not sent) -- both
            # are safe to re-send and must not be silently dropped.
            _to = locals().get("_TimedOut")
            is_timeout = (_to and isinstance(e, _to)) or "timed out" in err_str
            is_connect_timeout = self._looks_like_connect_timeout(e)
            is_pool_timeout = self._looks_like_pool_timeout(e)
            return SendResult(
                success=False,
                error=safe_error,
                retryable=(is_connect_timeout or is_pool_timeout or not is_timeout),
                error_kind=error_kind,
            )

    async def send_or_update_status(
        self,
        chat_id: str,
        status_key: str,
        content: str,
        *,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a status message, or edit the previous one with the same key.

        Issue #30045: progress/status callbacks (context-pressure, lifecycle,
        compression, etc.) used to append a fresh bubble on every call. With
        this method, the first call sends and the message id is remembered;
        subsequent calls with the same (chat_id, status_key) edit that same
        message in place. If the edit fails (message deleted, too old, etc.)
        we drop the cached id and send fresh.
        """
        key = (str(chat_id), str(status_key))
        cached_id = self._status_message_ids.get(key)
        if cached_id is not None:
            result = await self.edit_message(
                chat_id, cached_id, content, finalize=True, metadata=metadata,
            )
            if result.success:
                if result.message_id:
                    self._status_message_ids[key] = str(result.message_id)
                return result
            # Edit failed — clear the cached id and fall through to a fresh send.
            self._status_message_ids.pop(key, None)
        result = await self.send(chat_id, content, metadata=metadata)
        if result.success and result.message_id:
            self._status_message_ids[key] = str(result.message_id)
        return result

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Edit a previously sent Telegram message.

        Telegram caps single-message text at 4096 UTF-16 codeunits.  Streaming
        replies that grow past this limit must NOT be silently truncated and
        must NOT return failure (the consumer would re-send and create a
        duplicate).  Instead this method split-and-delivers: edit the
        existing message with the first chunk and send the rest as
        continuation messages, returning the final chunk's id so subsequent
        edits target the most recent visible message.
        """
        if not self._bot:
            return SendResult(success=False, error="Not connected")

        # Rich finalize (Bot API 10.1): when the completed content has
        # constructs the legacy MarkdownV2 edit degrades (tables → bullet
        # lists, task lists, <details>, block math) and rich is available,
        # edit the preview IN PLACE via editMessageText's rich_message param.
        # No fresh send + delete → no duplicate preview (the problem #46206
        # reverted the fresh-final path for).  Attempted before the 4,096
        # overflow pre-flight because the rich text cap is 32,768 — a rich
        # table that exceeds the MarkdownV2 limit must not be split into legacy
        # chunks.  Falls back to the legacy edit path (overflow split included)
        # on capability/permanent rejection.
        if finalize and self._rich_eligible(content):
            rich_result = await self._try_edit_rich(
                chat_id, message_id, content, metadata=metadata,
            )
            if rich_result is not None:
                return rich_result

        # Pre-flight: if content already exceeds the limit, split-and-deliver
        # without round-tripping a doomed edit.  During streaming
        # (finalize=False) we truncate instead of splitting — splitting creates
        # continuation messages whose IDs become the new edit target, and on
        # the next token chunk the full accumulated text is re-edited into the
        # continuation, triggering another split → infinite duplication loop
        # (#48648).  The full content is delivered when finalize=True.
        _preview_key = (str(chat_id), str(message_id))
        _saturated_preview = False
        if finalize:
            # Any saturation state for this message is finished with — the
            # final edit always delivers real (full) content.
            self._last_overflow_preview.pop(_preview_key, None)
        if utf16_len(content) > self.MAX_MESSAGE_LENGTH:
            if finalize:
                return await self._edit_overflow_split(
                    chat_id, message_id, content, finalize=finalize, metadata=metadata,
                )
            content = self._truncate_stream_overflow_preview(content)
            _saturated_preview = True
            # Saturated-preview dedup: past the cap, every progressive edit
            # truncates to the same text. Re-sending it is a visual no-op that
            # still burns flood budget (Telegram counts the request and answers
            # "message is not modified"). ~1 edit/0.8s for the rest of a long
            # stream trips flood control (200s+ penalties) and hangs the final
            # delivery. Skip silently until finalize.
            if self._last_overflow_preview.get(_preview_key) == content:
                return SendResult(success=True, message_id=message_id)
        elif not finalize:
            # Content shrank back under the cap (segment break / new message
            # id) — clear stale saturation state so dedup can't mask a real
            # edit later.
            self._last_overflow_preview.pop(_preview_key, None)

        try:
            if not finalize:
                await self._bot.edit_message_text(
                    chat_id=normalize_telegram_chat_id(chat_id),
                    message_id=int(message_id),
                    text=content,
                )
                if _saturated_preview:
                    self._last_overflow_preview[_preview_key] = content
                return SendResult(success=True, message_id=message_id)

            formatted = self.format_message(content)
            try:
                await self._bot.edit_message_text(
                    chat_id=normalize_telegram_chat_id(chat_id),
                    message_id=int(message_id),
                    text=formatted,
                    parse_mode=ParseMode.MARKDOWN_V2,
                )
            except Exception as fmt_err:
                # "Message is not modified" is a no-op, not an error
                if "not modified" in str(fmt_err).lower():
                    return SendResult(success=True, message_id=message_id)
                # Fallback: strip MarkdownV2 escapes and retry as clean plain text
                safe_format_error = _redact_telegram_error_text(fmt_err)
                logger.warning(
                    "[%s] MarkdownV2 edit failed, falling back to plain text: %s",
                    self.name,
                    safe_format_error,
                )
                _plain = _strip_mdv2(content) if content else content
                await self._bot.edit_message_text(
                    chat_id=normalize_telegram_chat_id(chat_id),
                    message_id=int(message_id),
                    text=_plain,
                )
            return SendResult(success=True, message_id=message_id)
        except Exception as e:
            err_str = str(e).lower()
            # "Message is not modified" — content identical, treat as success
            if "not modified" in err_str:
                return SendResult(success=True, message_id=message_id)
            # Reactive split-and-deliver: parse_mode formatting can inflate
            # the payload past the limit even when the raw text was under
            # (e.g. MarkdownV2 escapes).  Same fix as the pre-flight path.
            if "message_too_long" in err_str or "too long" in err_str:
                logger.debug(
                    "[%s] edit_message overflow (%d UTF-16 > %d), splitting",
                    self.name, utf16_len(content), self.MAX_MESSAGE_LENGTH,
                )
                if finalize:
                    return await self._edit_overflow_split(
                        chat_id, message_id, content, finalize=finalize, metadata=metadata,
                    )
                # Mid-stream: truncate and retry instead of splitting (#48648).
                truncated = self._truncate_stream_overflow_preview(content)
                if self._last_overflow_preview.get(_preview_key) == truncated:
                    # Saturated-preview dedup (see pre-flight path above).
                    return SendResult(success=True, message_id=message_id)
                await self._bot.edit_message_text(
                    chat_id=normalize_telegram_chat_id(chat_id),
                    message_id=int(message_id),
                    text=truncated,
                )
                self._last_overflow_preview[_preview_key] = truncated
                return SendResult(success=True, message_id=message_id)
            # Flood control / RetryAfter — short waits are retried inline,
            # long waits return a failure immediately so streaming can fall back
            # to a normal final send instead of leaving a truncated partial.
            retry_after = getattr(e, "retry_after", None)
            if retry_after is not None or "retry after" in err_str:
                wait = retry_after if retry_after else 1.0
                logger.warning(
                    "[%s] Telegram flood control, waiting %.1fs",
                    self.name, wait,
                )
                if wait > 5.0:
                    return SendResult(
                        success=False,
                        error=f"flood_control:{wait}",
                        retry_after=float(wait),
                    )
                await asyncio.sleep(wait)
                try:
                    await self._bot.edit_message_text(
                        chat_id=normalize_telegram_chat_id(chat_id),
                        message_id=int(message_id),
                        text=content,
                    )
                    return SendResult(success=True, message_id=message_id)
                except Exception as retry_err:
                    safe_retry_error = _redact_telegram_error_text(retry_err)
                    logger.error(
                        "[%s] Edit retry failed after flood wait: %s",
                        self.name, safe_retry_error,
                    )
                    return SendResult(success=False, error=safe_retry_error)
            # Transient network errors (ConnectError, timeouts, server
            # disconnects) should not permanently disable progress-message
            # editing.  Mark the result retryable so the caller knows it
            # can keep trying on the next update cycle.
            _transient_markers = (
                "connecterror",
                "connect error",
                "connection error",
                "networkerror",
                "network error",
                "timed out",
                "readtimeout",
                "writetimeout",
                "server disconnected",
                "temporarily unavailable",
                "temporary failure",
                "httpx",
            )
            _is_transient = any(m in err_str for m in _transient_markers)
            if _is_transient:
                safe_error = _redact_telegram_error_text(e)
                logger.warning(
                    "[%s] Transient network error editing message %s (will retry): %s",
                    self.name,
                    message_id,
                    safe_error,
                )
                return SendResult(success=False, error=safe_error, retryable=True)
            safe_error = _redact_telegram_error_text(e)
            logger.error(
                "[%s] Failed to edit Telegram message %s: %s",
                self.name,
                message_id,
                safe_error,
            )
            return SendResult(success=False, error=safe_error)

    def _truncate_stream_overflow_preview(self, content: str) -> str:
        """Return a one-message preview for oversized streaming edits.

        Streaming edits must keep targeting the original message. Splitting a
        mid-stream preview creates continuation messages and moves the active
        message id, so the next accumulated-token edit repeats the overflow
        cycle (#48648). Final edits still use ``_edit_overflow_split`` to
        deliver the complete response.
        """
        return self.truncate_message(
            content,
            self.MAX_MESSAGE_LENGTH,
            len_fn=utf16_len,
        )[0]

    async def _edit_overflow_split(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Split an oversized edit across the existing message + continuations.

        Edit the original ``message_id`` with chunk 1 (with the platform's
        usual ``(1/N)`` suffix preserved), then send the remaining chunks as
        new messages threaded as replies to the previous chunk so the user
        sees them grouped.  Returns ``SendResult(success=True,
        message_id=<last-chunk-id>, continuation_message_ids=(...))`` so the
        stream consumer can keep editing the most recent visible message
        and the gateway has full visibility into every message id we put on
        screen.

        Falls back to ``SendResult(success=False)`` only if even the first-
        chunk edit fails — that's a real adapter problem, not an overflow.
        """
        chunks = self.truncate_message(
            content, self.MAX_MESSAGE_LENGTH, len_fn=utf16_len,
        )
        if len(chunks) <= 1:
            # Defensive: shouldn't happen given the caller's pre-flight, but
            # if truncate_message returned a single chunk just edit normally.
            chunks = [content]

        # Step 1 — edit the existing message with the first chunk.
        first_chunk = chunks[0]
        try:
            if finalize:
                # Use format_message + parse_mode for the final chunk;
                # mirror edit_message's main happy-path.
                formatted = _separate_chunk_indicator_from_fence(
                    self.format_message(first_chunk)
                )
                try:
                    await self._bot.edit_message_text(
                        chat_id=normalize_telegram_chat_id(chat_id),
                        message_id=int(message_id),
                        text=formatted,
                        parse_mode=ParseMode.MARKDOWN_V2,
                    )
                except Exception as fmt_err:
                    if "not modified" not in str(fmt_err).lower():
                        logger.warning(
                            "[%s] Overflow split: MarkdownV2 first-chunk edit "
                            "failed, falling back to plain text: %s",
                            self.name, _redact_telegram_error_text(fmt_err),
                        )
                        await self._bot.edit_message_text(
                            chat_id=normalize_telegram_chat_id(chat_id),
                            message_id=int(message_id),
                            text=_strip_mdv2(first_chunk),
                        )
            else:
                await self._bot.edit_message_text(
                    chat_id=normalize_telegram_chat_id(chat_id),
                    message_id=int(message_id),
                    text=first_chunk,
                )
        except Exception as e:
            err_str = str(e).lower()
            if "not modified" in err_str:
                # First chunk identical to current text — fall through to
                # send continuations.
                pass
            else:
                logger.error(
                    "[%s] Overflow split: first-chunk edit failed: %s",
                    self.name, _redact_telegram_error_text(e), exc_info=True,
                )
                return SendResult(success=False, error=_redact_telegram_error_text(e))

        # Step 2 — send each remaining chunk as a continuation message,
        # threaded as a reply to the previous so the user sees them as a
        # contiguous block.  We call self._bot.send_message directly so the
        # continuation skips ``self.send``'s own pre-chunking pass (chunks
        # are already correctly sized).  Best-effort MarkdownV2 with plain
        # fallback, mirroring send().
        continuation_ids: list[str] = []
        delivered_chunks = [first_chunk]
        prev_id = message_id
        thread_id = self._metadata_thread_id(metadata)
        for chunk in chunks[1:]:
            sent_msg = None
            reply_to_id = int(prev_id) if prev_id else None
            thread_kwargs = self._thread_kwargs_for_send(
                chat_id,
                thread_id,
                metadata,
                reply_to_message_id=reply_to_id,
            )
            for use_markdown in (True, False) if finalize else (False,):
                try:
                    if use_markdown:
                        text = _separate_chunk_indicator_from_fence(
                            self.format_message(chunk)
                        )
                    else:
                        # Plain attempt: on finalize the MarkdownV2 attempt
                        # failed, so degrade to clean stripped text, never
                        # the raw chunk (raw ** / ``` markers would render
                        # literally); streaming previews stay raw.
                        text = _strip_mdv2(chunk) if finalize else chunk
                    sent_msg = await self._bot.send_message(
                        chat_id=normalize_telegram_chat_id(chat_id),
                        text=text,
                        parse_mode=ParseMode.MARKDOWN_V2 if use_markdown else None,
                        reply_to_message_id=reply_to_id,
                        **thread_kwargs,
                        **self._link_preview_kwargs(),
                        **self._notification_kwargs(metadata),
                    )
                    break
                except Exception as send_err:
                    if "reply message not found" in str(send_err).lower():
                        # Drop the reply anchor and try again.  Private DM
                        # topic fallback needs the anchor and topic id together;
                        # forum topics can still safely keep message_thread_id.
                        retry_thread_kwargs = (
                            {}
                            if metadata and metadata.get("telegram_dm_topic_reply_fallback")
                            else self._thread_kwargs_for_send(
                                chat_id, thread_id, metadata, reply_to_message_id=None
                            )
                        )
                        try:
                            sent_msg = await self._bot.send_message(
                                chat_id=normalize_telegram_chat_id(chat_id),
                                text=_strip_mdv2(chunk) if finalize else chunk,
                                **retry_thread_kwargs,
                                **self._link_preview_kwargs(),
                                **self._notification_kwargs(metadata),
                            )
                            break
                        except Exception as _retry_err:
                            logger.warning(
                                "[%s] Overflow continuation no-reply retry failed: %s",
                                self.name, _redact_telegram_error_text(_retry_err),
                            )
                            sent_msg = None
                            break
                    if use_markdown:
                        # try plain text on next loop iteration
                        continue
                    logger.warning(
                        "[%s] Overflow continuation send failed: %s",
                        self.name, _redact_telegram_error_text(send_err),
                    )
                    sent_msg = None
                    break
            if sent_msg is None:
                # Continuation failed — the user has chunk 1 + however many
                # continuations succeeded, but NOT the full response.  Do not
                # report success: the stream consumer treats a successful edit
                # as final delivery on got_done, which would suppress fallback
                # delivery and leave the Telegram topic clipped after the last
                # delivered chunk.
                logger.warning(
                    "[%s] Overflow split: stopped at %d/%d chunks delivered",
                    self.name, 1 + len(continuation_ids), len(chunks),
                )
                delivered_prefix = "".join(
                    re.sub(r" \(\d+/\d+\)$", "", delivered)
                    for delivered in delivered_chunks
                )
                return SendResult(
                    success=False,
                    message_id=prev_id,
                    error="overflow_continuation_failed",
                    retryable=True,
                    raw_response={
                        "partial_overflow": True,
                        "delivered_chunks": 1 + len(continuation_ids),
                        "total_chunks": len(chunks),
                        "last_message_id": prev_id,
                        "delivered_prefix": delivered_prefix,
                        "continuation_message_ids": tuple(continuation_ids),
                    },
                    continuation_message_ids=tuple(continuation_ids),
                )
            new_id = str(getattr(sent_msg, "message_id", "")) or prev_id
            continuation_ids.append(new_id)
            delivered_chunks.append(chunk)
            prev_id = new_id

        last_id = continuation_ids[-1] if continuation_ids else message_id
        logger.debug(
            "[%s] Overflow split delivered %d chunks; last_id=%s",
            self.name, 1 + len(continuation_ids), last_id,
        )
        return SendResult(
            success=True,
            message_id=last_id,
            continuation_message_ids=tuple(continuation_ids),
        )

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        """Delete a previously sent Telegram message.

        Used by the stream consumer's fresh-final cleanup path (ported
        from openclaw/openclaw#72038) to remove long-lived preview
        messages after sending the completed reply as a fresh message.
        Telegram's Bot API ``deleteMessage`` works for bot-posted
        messages in the last 48 hours.  Failures are non-fatal — the
        caller leaves the preview in place and logs at debug level.
        """
        if not self._bot:
            return False
        try:
            await self._bot.delete_message(
                chat_id=normalize_telegram_chat_id(chat_id),
                message_id=int(message_id),
            )
            return True
        except Exception as e:
            logger.debug(
                "[%s] Failed to delete Telegram message %s: %s",
                self.name, message_id, _redact_telegram_error_text(e),
            )
            return False

    def supports_draft_streaming(
        self,
        chat_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Telegram supports sendMessageDraft for private chats only.

        Bot API 9.5 (March 2026) opened ``sendMessageDraft`` to all bots
        unconditionally for private (DM) chats.  Groups, supergroups, and
        channels still rely on the edit-based path.

        We additionally require ``self._bot`` to expose ``send_message_draft``
        (added to python-telegram-bot in 22.6); older PTB installs gracefully
        fall back to the edit path even on DMs.
        """
        if not self._bot or not hasattr(self._bot, "send_message_draft"):
            return False
        return (chat_type or "").lower() in {"dm", "private"}

    async def send_draft(
        self,
        chat_id: str,
        draft_id: int,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Stream a partial message via Telegram's native draft API.

        Uses ``sendRichMessageDraft`` (Bot API 10.1) with the raw markdown when
        rich messages are enabled and supported, otherwise the plain-text
        ``sendMessageDraft``. The Bot API animates the preview when the same
        ``draft_id`` is reused across consecutive calls in the same chat.  When
        the response finishes, the caller sends the final text via the normal
        ``send`` path; the draft preview clears naturally on the client
        (Telegram has no Bot API to "promote" a draft to a real message — the
        final ``sendMessage``/``sendRichMessage`` is what the user receives in
        their history).
        """
        if not self._bot:
            return SendResult(success=False, error="not_connected")

        # Rich draft fast-path (Bot API 10.1 sendRichMessageDraft): render the
        # streaming preview with the same raw markdown the final
        # sendRichMessage will persist, so the animated draft matches the final
        # message. Any failure degrades to the legacy plain-text draft below.
        if self._should_attempt_rich_draft(content):
            if await self._try_send_rich_draft(chat_id, draft_id, content, metadata):
                # Drafts have no message_id; report success without one.
                return SendResult(success=True, message_id=None)

        if not hasattr(self._bot, "send_message_draft"):
            return SendResult(success=False, error="api_unavailable")

        # Trim to the same UTF-16 budget the platform enforces on regular
        # sends.  Drafts have the same length contract as messages.
        text = content if len(content) <= self.MAX_MESSAGE_LENGTH else \
            self.truncate_message(content, self.MAX_MESSAGE_LENGTH, len_fn=utf16_len)[0]

        thread_id = self._metadata_thread_id(metadata)

        # Apply the same MarkdownV2 conversion the regular ``send`` path uses
        # so the animated draft preview renders with identical formatting to
        # the final message.  Without this, the draft streams as raw text and
        # the final ``sendMessage`` (which DOES use MarkdownV2) snaps into
        # formatted output, producing a jarring visual shift at the end of the
        # response.  We try MarkdownV2 first and fall back to plain text if a
        # malformed escape would be rejected — mirroring the (True, False)
        # retry the streaming send loop uses — so a single bad token never
        # kills draft streaming for the whole response.
        for use_markdown in (True, False):
            kwargs: Dict[str, Any] = {
                "chat_id": normalize_telegram_chat_id(chat_id),
                "draft_id": int(draft_id),
                "text": self.format_message(text) if use_markdown else text,
            }
            if use_markdown:
                kwargs["parse_mode"] = ParseMode.MARKDOWN_V2
            if thread_id is not None:
                kwargs["message_thread_id"] = thread_id

            try:
                ok = await self._bot.send_message_draft(**kwargs)
                if ok:
                    # Drafts have no message_id; we report success without one
                    # so the caller knows the animation frame landed.
                    return SendResult(success=True, message_id=None)
                return SendResult(success=False, error="draft_rejected")
            except Exception as e:
                # A MarkdownV2 parse failure (BadRequest "can't parse entities")
                # is recoverable: retry once as plain text.  Any other failure
                # (chat doesn't allow drafts, transient hiccup) — or a failure
                # on the plain-text attempt — propagates to the caller, which
                # treats it as "fall back to edit-based for this response".
                if use_markdown and self._is_bad_request_error(e):
                    logger.debug(
                        "[%s] sendMessageDraft MarkdownV2 rejected, retrying "
                        "as plain text (chat=%s draft_id=%s): %s",
                        self.name, chat_id, draft_id, _redact_telegram_error_text(e),
                    )
                    continue
                logger.debug(
                    "[%s] sendMessageDraft failed (chat=%s draft_id=%s): %s",
                    self.name, chat_id, draft_id, e,
                )
                return SendResult(success=False, error=_redact_telegram_error_text(e))

        return SendResult(success=False, error="draft_rejected")

    async def _send_message_with_thread_fallback(self, **kwargs):
        """Send a Telegram message, retrying once without message_thread_id
        if Telegram returns 'Message thread not found'.

        Used for control-style sends (approval prompts, model picker,
        update prompts) that can carry a stale thread_id from a DM
        reply chain.  The streaming send loop has its own equivalent
        (PR #3390) at the body of ``send``; this helper applies the
        same retry pattern to the non-streaming control paths.
        """
        if not self._bot:
            raise RuntimeError("Not connected")

        message_thread_id = kwargs.get("message_thread_id")
        try:
            return await self._bot.send_message(**kwargs)
        except Exception as send_err:
            if (
                message_thread_id is not None
                and self._is_bad_request_error(send_err)
                and self._is_thread_not_found_error(send_err)
            ):
                logger.warning(
                    "[%s] Thread %s not found for control message, retrying without message_thread_id",
                    self.name,
                    message_thread_id,
                )
                # Same prune as the streaming send path — the
                # control-message retry tells us the topic is gone,
                # so the binding row in state.db must go too
                # (#31501).
                self._prune_stale_dm_topic_binding(
                    kwargs.get("chat_id"), message_thread_id,
                )
                retry_kwargs = dict(kwargs)
                retry_kwargs.pop("message_thread_id", None)
                return await self._bot.send_message(**retry_kwargs)
            raise

    def _notification_kwargs(
        self, metadata: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Return disable_notification kwargs when the adapter is in silent mode.

        In "important" mode, all message sends are silently delivered
        (disable_notification=True) unless the caller explicitly requests a
        notification by setting ``metadata["notify"] = True``.
        """
        if getattr(self, "_notifications_mode", "important") != "important":
            return {}
        if (metadata or {}).get("notify"):
            return {}
        return {"disable_notification": True}

    def _should_drop_delayed_delivery(self) -> bool:
        """True once teardown/fatal-error started — delayed flushes must drop.

        Buffered text/photo/media-group flushes sit behind an asyncio.sleep().
        If disconnect wins the race, dispatching them spawns an agent on a
        torn-down session, producing stale/duplicate deliveries.
        """
        return bool(getattr(self, "_drop_delayed_deliveries", False))
