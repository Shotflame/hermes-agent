"""
Telegram platform adapter.

Uses python-telegram-bot library for:
- Receiving messages from users/groups
- Sending responses back
- Handling media and commands
"""

import asyncio
import dataclasses
import faulthandler
import inspect
import json
import logging
import os
import html as _html
import re
import threading
import time
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Any

logger = logging.getLogger(__name__)


def _redact_telegram_error_text(error: object) -> str:
    """Redact secrets from Telegram transport errors before logging or returning them."""
    text = "" if error is None else str(error)
    if not text:
        return text
    try:
        from agent.redact import redact_sensitive_text

        return redact_sensitive_text(text, force=True)
    except Exception:
        return "<telegram error redacted>"


def _scoped_gate_env(name: str, default: str = "") -> str:
    """Read a TELEGRAM_*/GATEWAY_* authorization gate env var per-profile.

    Under gateway.multiplex_profiles the process env is first-writer-wins
    (the YAML→env bridge in ``_apply_yaml_config``), so a raw ``os.getenv``
    can return ANOTHER profile's allowlist (issue #72348, Telegram mirror).
    Reads the active profile's secret scope when installed; falls back to
    ``os.getenv`` outside multiplex — identical single-profile behavior.
    """
    try:
        from gateway.authz_mixin import _platform_gate_env

        return _platform_gate_env(name, default)
    except Exception:
        return (os.getenv(name) or default).strip()


def _consume_abandoned_task(task: asyncio.Task) -> None:
    """Observe a detached task's terminal exception to avoid noisy loop logs."""
    try:
        task.exception()
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.debug("Abandoned Telegram init task failed after timeout", exc_info=True)


# Grace period after the wall-clock deadline fires: if the event loop still
# hasn't processed the expiry callback by then, the loop thread itself is
# blocked in a synchronous call — the exact state in which every asyncio-based
# timeout (including this helper's own expiry hand-off) goes silent, so the
# gateway hangs at "attempt 1/8" with no further output (#63309).
_LOOP_BLOCKED_DUMP_GRACE = 5.0


def _dump_loop_blocked_diagnostics(timeout: float, grace: float) -> None:
    """Emit diagnostics from the deadline timer thread when the loop is stuck.

    Runs OFF the event loop, so it works precisely when the loop cannot. The
    faulthandler dump names the frame the loop thread is blocked in — the one
    piece of information #63309-class hangs otherwise never surface.
    """
    logger.warning(
        "[Telegram] init deadline (%.0fs) expired but the event loop has not "
        "processed the expiry after a further %.0fs — the loop thread appears "
        "BLOCKED in a synchronous call, which is why no timeout fires (#63309). "
        "Dumping all thread stacks to stderr to identify the blocking frame.",
        timeout,
        grace,
    )
    try:
        faulthandler.dump_traceback(all_threads=True)
    except Exception:
        logger.debug("faulthandler traceback dump failed", exc_info=True)


async def _await_with_thread_deadline(awaitable, timeout: float, *, on_abandon=None):
    """Await with a wall-clock deadline that does not depend on loop timers.

    ``asyncio.wait_for`` schedules its timeout on the event loop and then waits
    for cancellation to propagate.  PTB/httpcore initialization can sit inside
    cancellation-shielded anyio scopes, so a timed-out initialize() may never
    hand control back to the retry ladder under some supervisors.  This helper
    lets a daemon ``threading.Timer`` wake the loop and, on timeout, abandons
    the shielded task instead of awaiting cancellation completion.

    ``on_abandon`` (optional) is a zero-arg callable returning an awaitable that
    is scheduled as a detached best-effort cleanup when the task is abandoned on
    timeout.  The abandoned initialize() may leave a half-built httpx client /
    connection pool open (it never completed and we do not await its
    cancellation), so the caller uses this to shut that state down and avoid
    leaking a pool per retry attempt.  Cleanup runs detached and its own errors
    are swallowed, so it can never re-block the retry ladder.
    """
    task = asyncio.ensure_future(awaitable)
    loop = asyncio.get_running_loop()
    deadline = loop.create_future()
    # Set the moment the loop actually runs the expiry callback (or the helper
    # exits normally). threading.Event so the watchdog thread can read it
    # without touching asyncio state from off-loop.
    loop_processed_expiry = threading.Event()

    def _mark_expired() -> None:
        loop_processed_expiry.set()
        if not deadline.done():
            deadline.set_result(None)

    def _expire_from_thread() -> None:
        loop.call_soon_threadsafe(_mark_expired)

    def _watchdog_check() -> None:
        # The deadline fired _LOOP_BLOCKED_DUMP_GRACE ago but the loop never
        # ran _mark_expired: the loop thread is stuck in a synchronous call.
        # Diagnose from this thread — the loop can't.
        if not loop_processed_expiry.is_set():
            _dump_loop_blocked_diagnostics(timeout, _LOOP_BLOCKED_DUMP_GRACE)

    timer = threading.Timer(max(timeout, 0.0), _expire_from_thread)
    timer.daemon = True
    timer.start()
    watchdog = threading.Timer(
        max(timeout, 0.0) + _LOOP_BLOCKED_DUMP_GRACE, _watchdog_check
    )
    watchdog.daemon = True
    watchdog.start()
    try:
        done, _ = await asyncio.wait(
            {task, deadline},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            if not deadline.done():
                deadline.cancel()
            return await task

        task.cancel()
        task.add_done_callback(_consume_abandoned_task)
        if on_abandon is not None:
            # Detached best-effort cleanup: close the half-built app's httpx
            # client/pool so an abandoned attempt can't leak sockets across the
            # retry ladder. Detached + exception-observed so it never re-blocks
            # or re-hangs the ladder we are trying to advance.
            cleanup = asyncio.ensure_future(_run_abandon_cleanup(on_abandon))
            cleanup.add_done_callback(_consume_abandoned_task)
        raise asyncio.TimeoutError()
    finally:
        timer.cancel()
        watchdog.cancel()
        # cancel() cannot stop a Timer whose callback is already running;
        # setting the event closes that race so a completed await can never
        # be misreported as a blocked loop.
        loop_processed_expiry.set()


async def _first_completed(*futures: "asyncio.Future") -> None:
    """Return when the first of ``futures`` completes.

    Used by the strict cold-start readiness gate to wait on "progress OR
    polling error", whichever fires first (#67498). Does not cancel the
    losers — the caller owns their lifecycle.
    """
    await asyncio.wait(set(futures), return_when=asyncio.FIRST_COMPLETED)


async def _run_abandon_cleanup(on_abandon) -> None:
    """Run the abandonment cleanup coroutine, swallowing any failure.

    Wrapped so a cleanup that itself hangs or raises cannot surface as an
    unhandled task error or block anything — it is fully fire-and-forget.
    """
    try:
        result = on_abandon()
        if asyncio.iscoroutine(result) or asyncio.isfuture(result):
            await result
    except Exception:
        logger.debug("Abandoned Telegram init cleanup failed", exc_info=True)


async def _shutdown_abandoned_app(app) -> None:
    """Release a half-built PTB app's httpx transports after init was abandoned.

    ``Application.shutdown()`` / ``Bot.shutdown()`` are gated on the app's
    ``_initialized`` / ``_requests_initialized`` flags, which a wedged
    ``initialize()`` (the case this whole path exists for) may never have set —
    so calling only ``app.shutdown()`` no-ops and leaks the connection pool it
    was meant to close.  ``HTTPXRequest`` builds its ``httpx.AsyncClient``
    eagerly in its constructor and its ``shutdown()`` gates only on
    ``client.is_closed``, so closing the request transports directly releases
    the pool regardless of PTB init state.  We try the clean path first, then
    fall back to the transports.  All best-effort and swallowed.
    """
    if app is None:
        return
    try:
        await app.shutdown()
    except Exception:
        logger.debug("Abandoned Telegram app.shutdown() failed", exc_info=True)
    # Directly close the underlying request transports (bypasses PTB's
    # init-gated shutdown so the eagerly-built httpx pool is released even when
    # the abandoned initialize() never flipped _initialized).
    bot = getattr(app, "bot", None)
    requests = getattr(bot, "_request", None) if bot is not None else None
    if not requests:
        return
    for request in requests:
        shutdown = getattr(request, "shutdown", None)
        if shutdown is None:
            continue
        try:
            result = shutdown()
            if asyncio.iscoroutine(result) or asyncio.isfuture(result):
                await result
        except Exception:
            logger.debug("Abandoned Telegram request shutdown failed", exc_info=True)

try:
    from telegram import Update, Bot, Message, InlineKeyboardButton, InlineKeyboardMarkup
    try:
        from telegram import LinkPreviewOptions
    except ImportError:
        LinkPreviewOptions = None
    from telegram.ext import (
        Application,
        CommandHandler,
        CallbackQueryHandler,
        MessageHandler as TelegramMessageHandler,
        ContextTypes,
        filters,
    )
    from telegram.constants import ParseMode, ChatType
    from telegram.request import HTTPXRequest
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    Update = Any
    Bot = Any
    Message = Any
    InlineKeyboardButton = Any
    InlineKeyboardMarkup = Any
    LinkPreviewOptions = None
    Application = Any
    CommandHandler = Any
    CallbackQueryHandler = Any
    TelegramMessageHandler = Any
    HTTPXRequest = Any
    filters = None
    ParseMode = None
    ChatType = None

    # Mock ContextTypes so type annotations using ContextTypes.DEFAULT_TYPE
    # don't crash during class definition when the library isn't installed.
    class _MockContextTypes:
        DEFAULT_TYPE = Any
    ContextTypes = _MockContextTypes

import sys
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parents[3]))

from gateway.authz_mixin import _coerce_allow_set
from gateway.config import Platform, PlatformConfig
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
from plugins.platforms.telegram.telegram_network import (
    TelegramFallbackTransport,
    discover_fallback_ips,
    parse_fallback_ip_env,
)
from utils import atomic_replace, env_float, env_int

_TELEGRAM_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
_TELEGRAM_IMAGE_MIME_TO_EXT = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/webp": ".webp",
    "image/gif": ".gif",
}
_TELEGRAM_IMAGE_EXT_TO_MIME = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif": "image/gif",
}

def _coerce_duration_seconds(value: Any) -> Optional[int]:
    """Round a raw length to whole positive seconds, or None if unusable."""
    try:
        secs = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    return secs if secs > 0 else None


def _probe_voice_duration_seconds(path: str) -> Optional[int]:
    """Best-effort audio length in whole seconds for outgoing voice/audio.

    Telegram only auto-derives a clip's duration from container metadata for
    short recordings; longer ones (roughly 5 min+) are sent with duration 0
    and render as ``0:00`` in the player. We read the length locally and pass
    it explicitly so the bubble shows the real time.

    Mirrors ``gateway.run._probe_audio_duration``: stdlib ``wave`` for WAV,
    then mutagen for OGG/Opus/MP3/M4A metadata, then an ``ffprobe`` fallback.
    All three are optional — when none can read the file we return ``None``
    and the caller omits ``duration``, falling back to Telegram's own
    (possibly absent) metadata, i.e. the prior behavior. Blocking (mutagen
    read + ffprobe subprocess), so call it via ``asyncio.to_thread``.
    """
    ext = os.path.splitext(path)[1].lower()

    if ext == ".wav":
        try:
            import wave

            with wave.open(path, "rb") as wf:
                rate = wf.getframerate() or 0
                if rate:
                    secs = _coerce_duration_seconds(wf.getnframes() / float(rate))
                    if secs is not None:
                        return secs
        except Exception:
            pass

    try:
        import mutagen

        audio = mutagen.File(path)
        secs = _coerce_duration_seconds(
            getattr(getattr(audio, "info", None), "length", None)
        )
        if secs is not None:
            return secs
    except Exception:
        pass

    try:
        import shutil
        import subprocess

        if shutil.which("ffprobe"):
            proc = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1", path],
                capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=5,
            )
            if proc.returncode == 0:
                return _coerce_duration_seconds(proc.stdout.strip())
    except Exception:
        pass

    return None


def check_telegram_requirements() -> bool:
    """Check if Telegram dependencies are available.

    If python-telegram-bot is missing, attempts to lazy-install it via
    ``tools.lazy_deps.ensure("platform.telegram")``. After a successful
    install, re-imports the SDK and flips ``TELEGRAM_AVAILABLE`` to True
    so the adapter's class-level type aliases get rebound.
    """
    global TELEGRAM_AVAILABLE, Update, Bot, Message, InlineKeyboardButton
    global InlineKeyboardMarkup, LinkPreviewOptions, Application
    global CommandHandler, CallbackQueryHandler, TelegramMessageHandler
    global ContextTypes, filters, ParseMode, ChatType, HTTPXRequest
    if TELEGRAM_AVAILABLE:
        return True
    try:
        from tools.lazy_deps import ensure as _lazy_ensure
        _lazy_ensure("platform.telegram", prompt=False)
    except Exception:
        return False
    try:
        from telegram import Update as _Update, Bot as _Bot, Message as _Message
        from telegram import InlineKeyboardButton as _IKB, InlineKeyboardMarkup as _IKM
        try:
            from telegram import LinkPreviewOptions as _LPO
        except ImportError:
            _LPO = None
        from telegram.ext import (
            Application as _App, CommandHandler as _CH,
            CallbackQueryHandler as _CQH,
            MessageHandler as _MH,
            ContextTypes as _CT, filters as _filters,
        )
        from telegram.constants import ParseMode as _PM, ChatType as _CtT
        from telegram.request import HTTPXRequest as _HR
    except ImportError:
        return False
    Update = _Update
    Bot = _Bot
    Message = _Message
    InlineKeyboardButton = _IKB
    InlineKeyboardMarkup = _IKM
    LinkPreviewOptions = _LPO
    Application = _App
    CommandHandler = _CH
    CallbackQueryHandler = _CQH
    TelegramMessageHandler = _MH
    ContextTypes = _CT
    filters = _filters
    ParseMode = _PM
    ChatType = _CtT
    HTTPXRequest = _HR
    TELEGRAM_AVAILABLE = True
    return True


# Matches every character that MarkdownV2 requires to be backslash-escaped
# when it appears outside a code span or fenced code block.
_MDV2_ESCAPE_RE = re.compile(r'([_*\[\]()~`>#\+\-=|{}.!\\])')


def _escape_mdv2(text: str) -> str:
    """Escape Telegram MarkdownV2 special characters with a preceding backslash."""
    return _MDV2_ESCAPE_RE.sub(r'\\\1', text)


def _strip_mdv2(text: str) -> str:
    """Strip MarkdownV2 escape backslashes to produce clean plain text.

    Also removes MarkdownV2 formatting markers so the fallback
    doesn't show stray syntax characters from format_message conversion.
    """
    # Remove escape backslashes before special characters
    cleaned = re.sub(r'\\([_*\[\]()~`>#\+\-=|{}.!\\])', r'\1', text)
    # Remove standard markdown bold (**text** → text) BEFORE MarkdownV2 bold
    cleaned = re.sub(r'\*\*([^*]+)\*\*', r'\1', cleaned)
    # Remove MarkdownV2 bold markers that format_message converted from **bold**
    cleaned = re.sub(r'\*([^*]+)\*', r'\1', cleaned)
    # Remove MarkdownV2 italic markers that format_message converted from *italic*
    # Use word boundary (\b) to avoid breaking snake_case like my_variable_name
    cleaned = re.sub(r'(?<!\w)_([^_]+)_(?!\w)', r'\1', cleaned)
    # Remove MarkdownV2 strikethrough markers (~text~ → text)
    cleaned = re.sub(r'~([^~]+)~', r'\1', cleaned)
    # Remove MarkdownV2 spoiler markers (||text|| → text)
    cleaned = re.sub(r'\|\|([^|]+)\|\|', r'\1', cleaned)
    return cleaned


_CHUNK_INDICATOR_ON_FENCE_RE = re.compile(
    r'(?m)^``` (?P<indicator>(?:\\)?\(\d+/\d+(?:\\)?\))$'
)


def _separate_chunk_indicator_from_fence(text: str) -> str:
    """Move ``(N/M)`` chunk markers off Telegram code-fence lines.

    ``truncate_message()`` appends chunk indicators to the end of a chunk. When
    the chunk had to close an in-progress fenced code block, that creates a
    line like ````` \\(1/2\\)`` after MarkdownV2 escaping. Telegram does not
    treat that as a clean closing fence, so it can reject MarkdownV2 and fall
    back to plain text. Put the indicator on its own line immediately after the
    closing fence.
    """
    return _CHUNK_INDICATOR_ON_FENCE_RE.sub(r'```\n\g<indicator>', text)


# ---------------------------------------------------------------------------
# Markdown table → Telegram-friendly row groups
# ---------------------------------------------------------------------------
# Telegram's MarkdownV2 has no table syntax — '|' is just an escaped literal,
# so pipe tables render as noisy backslash-pipe text with no alignment.
# The shared convert_table_to_bullets() in gateway.platforms.helpers handles
# the full conversion (detection + rendering); Telegram just calls it.

from gateway.platforms.helpers import (
    TABLE_SEPARATOR_RE as _TABLE_SEPARATOR_RE,
    compile_mention_patterns,
    convert_table_to_bullets as _wrap_markdown_tables,
)


# ---------------------------------------------------------------------------
# Rich-message newline normalization
# ---------------------------------------------------------------------------

# Matches a protected region whose internal newlines must stay bare in the
# rich-message path: a fenced code block (```...```) OR a GFM pipe-table block
# (a header row, a delimiter row of dashes/pipes, then any pipe data rows).
# Telegram renders both natively, so injecting Markdown hard breaks inside them
# would corrupt the code block / table.
_RICH_PROTECTED_REGION_RE = re.compile(
    r'(?:```[^\n]*\n[\s\S]*?```)'                       # fenced code block
    r'|(?:^[^\n]*\|[^\n]*\n'                            # table header row (has a pipe)
    r'[ \t]*\|?[ \t]*:?-+:?[ \t]*(?:\|[ \t]*:?-+:?[ \t]*)+\|?[ \t]*'  # delimiter
    r'(?:\n[^\n]*\|[^\n]*)*)',                          # data rows (newline-led, trailing \n left for prose)
    re.MULTILINE,
)


def _rich_normalize_linebreaks(text: str) -> str:
    """Convert single ``\\n`` to Markdown hard breaks for the rich-message path.

    Standard Markdown treats a lone ``\\n`` as whitespace (soft break), so
    Bot API 10.1 ``sendRichMessage`` collapses multi-line content — e.g.
    slash-command lists joined with ``"\\n".join(lines)`` — into a single
    paragraph.  Adding two trailing spaces before each single newline
    forces a hard line break (``<br>``) in the rendered output.

    Paragraph breaks (``\\n\\n``), fenced code blocks, and GFM pipe-table
    blocks are left untouched: tables render natively in the rich path and a
    hard break injected into a row separator would corrupt the table.
    """
    if not text or '\n' not in text:
        return text

    out: list[str] = []
    # Split off protected regions (fenced code OR table blocks) and only inject
    # hard breaks in the prose between them. Boundary newlines are handled by
    # the original single-\n regex, which sees each prose run as a whole string.
    pos = 0
    for m in _RICH_PROTECTED_REGION_RE.finditer(text):
        prose = text[pos:m.start()]
        out.append(re.sub(r'(?<!\n)\n(?!\n)', '  \n', prose))
        out.append(m.group(0))  # protected region kept verbatim
        pos = m.end()
    tail = text[pos:]
    out.append(re.sub(r'(?<!\n)\n(?!\n)', '  \n', tail))
    return ''.join(out)


# Watchdog bound for `await updater.stop()`. When the underlying TCP socket is
# in CLOSE-WAIT the PTB polling task is blocked on epoll on the dead socket and
# never wakes, so an unguarded stop() hangs indefinitely and wedges the whole
# reconnect/teardown ladder. This is an internal safety bound (not a user knob),
# applied identically at every stop() site so no path can hang on a dead socket.
_UPDATER_STOP_TIMEOUT = 15.0
# start_polling() can also hang when the connection pool is in a degraded state
# after _drain_polling_connections(), particularly when both primary and fallback
# Telegram endpoints are unreachable. Bounding start_polling() prevents the
# reconnect ladder from stalling indefinitely and allows the heartbeat loop to
# trigger its own recovery path. Refs: NousResearch/hermes-agent#59614
_UPDATER_START_TIMEOUT = 30.0
# Initial connect is not healthy until the dedicated getUpdates request completes
# one successful round trip. Unlike reconnect, initial bootstrap must fail closed
# so GatewayRunner disposes the partial adapter and retries with a fresh PTB app.
_INITIAL_POLLING_PROGRESS_TIMEOUT = 60.0
# shutdown()/initialize() on the getUpdates httpx request close and rebuild the
# connection pool. When a connection is wedged on a stale CLOSE-WAIT socket that
# close can block forever, hanging _drain_polling_connections() and freezing the
# whole reconnect ladder (the tracked _polling_error_task never completes, so
# every escalation path stays gated behind its in-flight guard). Bound the drain
# so the ladder always advances toward the fatal-restart escalation. Matches
# _UPDATER_STOP_TIMEOUT. Refs: NousResearch/hermes-agent#66377
_DRAIN_TIMEOUT = 15.0
# Cause-agnostic wedged-recovery watchdog (#66377). Every recovery path (the
# reconnect ladder's re-entry, the pending-update probe, PTB's error callback)
# gates new recovery on ``_polling_error_task.done()``; if that task ever wedges
# on a hung await that no local bound covers, the whole gateway goes silently
# deaf with nothing retrying. The heartbeat loop force-escalates a recovery task
# that stays in-flight far longer than any healthy ladder attempt could take —
# stop (_UPDATER_STOP_TIMEOUT) + drain (2x_DRAIN_TIMEOUT) + start
# (_UPDATER_START_TIMEOUT) + max backoff (60s) is ~135s, so 300s is
# unambiguously stuck.
_POLLING_ERROR_TASK_STUCK_TIMEOUT = 300.0
# A generation is not healthy until the dedicated getUpdates request returns
# successfully. This exceeds a normal long-poll cycle for healthy idle bots.
_POLLING_PROGRESS_TIMEOUT = 60.0
# Telegram transcodes an uploaded video before it answers sendVideo, so the
# wait for the response is unrelated to how fast the bytes went out and can
# outlast the 20s read timeout the rest of the Bot API is tuned for. Only
# media sends take this longer budget; ordinary calls keep the short one so a
# dead request is still noticed quickly. Kept modest deliberately — this is
# also how long a user waits to be told the attachment failed.
_MEDIA_SEND_READ_TIMEOUT = 60.0
_POLLING_GENERATION_CONTEXT: ContextVar[Optional[int]] = ContextVar(
    "telegram_polling_generation", default=None
)


class _PollingLifecycleAbort(RuntimeError):
    """Internal control flow for polling startup fenced by teardown."""


from .mixins import (
    TelegramAuthzMixin,
    TelegramConfigMixin,
    TelegramInitGuardsMixin,
    TelegramIngestMixin,
    TelegramInteractiveMixin,
    TelegramMediaMixin,
    TelegramMessagingMixin,
    TelegramNetworkMixin,
    TelegramReactionsMixin,
    TelegramRichMixin,
    TelegramTextFormatMixin,
    TelegramTopicsMixin,
    TelegramTypingMixin,
)


class TelegramAdapter(
    TelegramInitGuardsMixin,
    TelegramConfigMixin,
    TelegramAuthzMixin,
    TelegramTopicsMixin,
    TelegramNetworkMixin,
    TelegramRichMixin,
    TelegramTextFormatMixin,
    TelegramMessagingMixin,
    TelegramInteractiveMixin,
    TelegramMediaMixin,
    TelegramTypingMixin,
    TelegramIngestMixin,
    TelegramReactionsMixin,
    BasePlatformAdapter,
):
    """
    Telegram bot adapter.

    Handles:
    - Receiving messages from users and groups
    - Sending responses with Telegram markdown
    - Forum topics (thread_id support)
    - Media messages
    """

    # Telegram message limits
    MAX_MESSAGE_LENGTH = 4096
    supports_code_blocks = True  # Telegram MarkdownV2 renders fenced code blocks
    splits_long_messages = True  # send() chunks via truncate_message(MAX_MESSAGE_LENGTH)
    # Bot API 10.1 Rich Messages cap the raw markdown/html text at 32,768
    # UTF-8 characters. Content above this is sent via the legacy chunking path.
    RICH_MESSAGE_MAX_CHARS = 32768
    # Backwards-compatible alias for tests/external callers that referenced the
    # initial implementation name. The API limit is character-based, not bytes.
    RICH_MESSAGE_MAX_BYTES = RICH_MESSAGE_MAX_CHARS
    # Threshold for detecting Telegram client-side message splits.
    # When a chunk is near this limit, a continuation is almost certain.
    _SPLIT_THRESHOLD = 4000
    MEDIA_GROUP_WAIT_SECONDS = 0.8
    _GENERAL_TOPIC_THREAD_ID = "1"

    # Telegram's edit_message applies MarkdownV2 formatting only on the
    # finalize=True path.  Without this flag, stream_consumer._send_or_edit
    # short-circuits when the raw text is unchanged between the last streamed
    # edit and the final edit, skipping the plain-text → MarkdownV2 conversion.
    # Fixes #25710.
    REQUIRES_EDIT_FINALIZE: bool = True
    # Retrying a turn-final edit consumes more of the same Telegram flood
    # budget while the completed answer remains undelivered. Move directly to
    # the final fallback path instead.
    FALLBACK_ON_FINAL_EDIT_FLOOD: bool = True
    # A failed final edit can leave Telegram clients with only a partial or
    # non-durable preview. Commit empty-tail fallbacks as a fresh final message
    # instead of trusting the preview as completed delivery.
    RESEND_FINAL_ON_EMPTY_STREAM_FALLBACK: bool = True

    # Adaptive text-batch ingress: short messages need a tighter delay so the
    # first token reaches the agent fast.  Numbers tuned for "feels instant":
    # ≤320 codepoints (one short paragraph) settles in ~180ms; ≤1024
    # (a normal paragraph) in ~240ms; longer waits the configured cap.
    # Always clamped to ``_text_batch_delay_seconds`` so an operator can lower
    # the cap further via env var.
    _TEXT_BATCH_FAST_LEN = 320
    _TEXT_BATCH_FAST_DELAY_S = 0.18
    _TEXT_BATCH_SHORT_LEN = 1024
    _TEXT_BATCH_SHORT_DELAY_S = 0.24



    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.TELEGRAM)
        self._app: Optional[Application] = None
        self._bot: Optional[Bot] = None
        self._webhook_mode: bool = False
        self._mention_patterns = self._compile_mention_patterns()
        self._reply_to_mode: str = getattr(config, 'reply_to_mode', 'first') or 'first'
        self._disable_link_previews: bool = self._coerce_bool_extra("disable_link_previews", False)
        # Bot API 10.1 Rich Messages: render constructs the legacy MarkdownV2
        # path degrades (tables → bullet lists, task lists, <details>, block
        # math) via sendRichMessage / editMessageText's rich_message param using
        # the raw agent markdown. Disabled by default so Telegram messages stay
        # easy to copy as plain text; users can opt in for richer rendering on
        # clients that accept but render rich messages poorly via
        # platforms.telegram.extra.rich_messages: true.  Keep this opt-in:
        # current Telegram clients can make rich messages difficult to copy
        # as plain text, which is worse than degraded table/task-list rendering
        # for command snippets and mobile handoffs.
        self._rich_messages_enabled: bool = self._coerce_bool_extra("rich_messages", False)
        # Rich draft previews use a separate opt-in. Telegram macOS / Desktop
        # can leave Bot API 10.1 rich draft frames visually overlaid until the
        # chat is redrawn, while final rich messages remain useful.
        self._rich_drafts_enabled: bool = self._coerce_bool_extra("rich_drafts", False)
        # Latched off after a capability failure on sendRichMessage /
        # sendRichMessageDraft (e.g. older python-telegram-bot without the
        # endpoint) so later sends skip the doomed rich attempt entirely.
        self._rich_send_disabled: bool = False
        self._rich_draft_disabled: bool = False
        # Transient Telegram sendChatAction failures (network blips, 429/5xx)
        # can happen on every keep-typing tick while the agent is waiting on a
        # long model call. Back off per chat so a short Telegram-side outage
        # does not spam the API/logs or burn the keep-typing budget.
        self._telegram_typing_cooldown_until: Dict[str, float] = {}
        self._telegram_typing_cooldown_seconds: float = self._coerce_float_extra(
            "typing_cooldown_seconds",
            30.0,
            min_value=1.0,
            max_value=300.0,
        )
        # Buffer rapid/album photo updates so Telegram image bursts are handled
        # as a single MessageEvent instead of self-interrupting multiple turns.
        self._media_batch_delay_seconds = env_float("HERMES_TELEGRAM_MEDIA_BATCH_DELAY_SECONDS", 0.8)
        self._pending_photo_batches: Dict[str, MessageEvent] = {}
        self._pending_photo_batch_tasks: Dict[str, asyncio.Task] = {}
        self._media_group_events: Dict[str, MessageEvent] = {}
        self._media_group_tasks: Dict[str, asyncio.Task] = {}
        # Buffer rapid text messages so Telegram client-side splits of long
        # messages are aggregated into a single MessageEvent.  Lower defaults
        # (0.3s / 1.0s instead of 0.6s / 2.0s) let short replies stream
        # without a noticeable wait — combined with the adaptive fast-path
        # in ``_calc_text_batch_delay`` below, ≤320-codepoint replies settle
        # in ~180ms.  All bounds are conservative for Telegram's
        # ~1 edit/s flood envelope.
        self._text_batch_delay_seconds = self._env_float_clamped(
            "HERMES_TELEGRAM_TEXT_BATCH_DELAY_SECONDS",
            0.3,
            min_value=0.08,
            max_value=2.0,
        )
        self._text_batch_split_delay_seconds = self._env_float_clamped(
            "HERMES_TELEGRAM_TEXT_BATCH_SPLIT_DELAY_SECONDS",
            1.0,
            min_value=self._text_batch_delay_seconds,
            max_value=4.0,
        )
        self._pending_text_batches: Dict[str, MessageEvent] = {}
        self._pending_text_batch_tasks: Dict[str, asyncio.Task] = {}
        self._drop_delayed_deliveries = False
        self._polling_error_task: Optional[asyncio.Task] = None
        self._polling_conflict_count: int = 0
        self._polling_network_error_count: int = 0
        self._polling_generation: int = 0
        self._polling_progress_event = asyncio.Event()
        self._polling_progress_accepting: bool = False
        self._polling_progress_verifier_task: Optional[asyncio.Task] = None
        self._polling_teardown_started: bool = False
        self._polling_error_callback_ref = None
        self._polling_heartbeat_task: Optional[asyncio.Task] = None
        # Live @username, refreshed whenever Telegram tells us what it is.
        # PTB caches getMe() in Bot._bot_user at initialize() and only rewrites
        # it inside get_me(), so a BotFather rename leaves self._bot.username
        # pointing at the old handle until something calls getMe again. Every
        # mention/routing comparison reads _current_bot_username() instead.
        self._bot_username_observed: Optional[str] = None
        # None = never checked. Must NOT be 0.0: these are compared against
        # time.monotonic(), whose epoch is arbitrary and on a freshly-booted
        # host starts near zero — so a 0.0 sentinel reads as "checked just
        # now" and suppresses the first refresh for the first TTL seconds of
        # uptime.
        self._bot_identity_checked_at: Optional[float] = None
        self._bot_identity_refresh_task: Optional[asyncio.Task] = None
        # Consecutive heartbeat probes that saw queued updates the running
        # poller is not consuming. get_me() can't see this — the send path is
        # healthy while the getUpdates consumer is wedged — so the heartbeat
        # also probes get_webhook_info().pending_update_count and escalates to
        # recovery after two consecutive stuck probes (#42909).
        self._polling_pending_stuck_count: int = 0
        # Consecutive heartbeat probes that found the updater stopped entirely
        # (running=False) while we are in polling mode with no reconnect in
        # flight. Distinct from the wedged-but-running case above: the long-poll
        # task is simply gone, so neither the connectivity probe nor PTB's
        # error_callback ever fires and the gateway silently stops receiving
        # messages with the process still alive (#55769).
        self._polling_not_running_count: int = 0
        # A polling generation stays degraded until the dedicated getUpdates
        # request makes successful progress. start_polling() return and getMe()
        # success on the general request path are not polling-health signals.
        # While True, send() short-circuits to a failure so callers
        # (cron live-adapter branch) fall through to standalone delivery.
        self._send_path_degraded: bool = False
        self._general_request_drain_lock = asyncio.Lock()
        # DM Topics: map of topic_name -> message_thread_id (populated at startup)
        self._dm_topics: Dict[str, int] = {}
        # Track forum chats where we've already registered bot commands
        self._forum_command_registered: set[int] = set()
        # Lock per la registrazione sicura dei comandi nei forum supergroup
        self._forum_lock = asyncio.Lock()
        # Status indicator: when enabled, the bot's short description (the line
        # shown under its name in the profile) is set to "Online" on connect and
        # "Offline" on clean disconnect, so users can tell whether the gateway is
        # up. Telegram bots have no real presence/online dot (that's a user-account
        # feature), so the short description is the closest available surface.
        # Off by default — this mutates the bot's GLOBAL profile, visible to all
        # users. Opt in via gateway config: extra.status_indicator: true, or set
        # custom strings via extra.status_online / extra.status_offline.
        self._status_indicator_enabled: bool = bool(
            self.config.extra.get("status_indicator", False)
        )
        self._status_online_text: str = str(
            self.config.extra.get("status_online", "Online")
        )
        self._status_offline_text: str = str(
            self.config.extra.get("status_offline", "Offline")
        )
        # DM Topics config from extra.dm_topics
        self._dm_topics_config: List[Dict[str, Any]] = self.config.extra.get("dm_topics", [])
        # Precomputed chat_ids that have DM topics configured (for O(1) root-DM ignore check)
        self._dm_topic_chat_ids: Set[str] = {
            str(e["chat_id"]) for e in self._dm_topics_config if "chat_id" in e
        }
        # Document size cap. Telegram's public Bot API caps getFile at 20MB; a
        # locally-hosted telegram-bot-api server (configured via extra.base_url)
        # raises that to 2GB, so the presence of base_url is the opt-in.
        self._max_doc_bytes: int = (
            2 * 1024 * 1024 * 1024
            if self.config.extra.get("base_url")
            else 20 * 1024 * 1024
        )
        # Interactive model picker state per chat
        self._model_picker_state: Dict[str, dict] = {}
        self._choice_picker_state: Dict[str, dict] = {}
        # Approval button state: message_id → session_key
        self._approval_state: Dict[int, str] = {}
        # Slash-confirm button state: confirm_id → session_key (for /reload-mcp
        # and any other slash-confirm prompts; see GatewayRunner._request_slash_confirm).
        self._slash_confirm_state: Dict[str, str] = {}
        # Clarify button state: clarify_id → session_key (for the clarify tool's
        # multiple-choice prompts; see GatewayRunner clarify_callback wiring).
        self._clarify_state: Dict[str, str] = {}
        # Notification mode for message sends.
        # "important" — only final responses, approvals, and slash confirmations
        #               trigger notifications; tool progress, streaming, status
        #               messages are delivered silently via disable_notification.
        #               This is the default — Telegram users found per-tool-call
        #               push notifications too noisy.
        # "all"       — every message triggers a push notification (legacy
        #               behavior; opt-in via display.platforms.telegram.notifications).
        self._notifications_mode: str = "important"
        # send_or_update_status() bookkeeping: {(chat_id, status_key) -> bot message_id}
        # Tracks status bubbles owned by this adapter so subsequent calls with the
        # same key edit the same message instead of appending new ones (#30045).
        self._status_message_ids: Dict[tuple, str] = {}
        # Last truncated mid-stream preview delivered per (chat_id, message_id).
        # Once an oversized streaming edit saturates at the 4096 preview cap,
        # every subsequent progressive edit truncates to the SAME text; sending
        # it again is a no-op that still burns Telegram's flood budget (~1
        # edit/0.8s × the rest of the stream ⇒ flood control with 200s+
        # penalties, hanging final delivery). Dedup here so a saturated preview
        # goes quiet until finalize. Bounded: entries are dropped on finalize.
        self._last_overflow_preview: Dict[tuple, str] = {}
        # Background task that runs post-connect housekeeping (command-menu
        # registration + DM-topic setup) off the connect path so a slow Bot
        # API call (e.g. a set_my_commands stall for certain tokens) cannot
        # blow the gateway's connect timeout (#46298).
        self._post_connect_task: Optional[asyncio.Task] = None

    def _mark_connected(self) -> None:
        self._drop_delayed_deliveries = False
        super()._mark_connected()

    def _mark_disconnected(self) -> None:
        self._drop_delayed_deliveries = True
        super()._mark_disconnected()

    def _set_fatal_error(self, code: str, message: str, *, retryable: bool) -> None:
        self._drop_delayed_deliveries = True
        super()._set_fatal_error(code, message, retryable=retryable)





























    def _link_preview_kwargs(self) -> Dict[str, Any]:
        if not getattr(self, "_disable_link_previews", False):
            return {}
        if LinkPreviewOptions is not None:
            return {"link_preview_options": LinkPreviewOptions(is_disabled=True)}
        return {"disable_web_page_preview": True}

    # ------------------------------------------------------------------
    # Bot API 10.1 Rich Messages (sendRichMessage)
    #
    # Final / new-message replies opportunistically use sendRichMessage with
    # the RAW agent markdown so richer constructs (tables, task lists,
    # collapsible details, math, ...) render natively. The legacy MarkdownV2
    # send() path stays as the fallback for unsupported/oversized content and
    # older PTB/clients. Streaming edits stay on Hermes' existing MarkdownV2
    # edit path for now; finalization can re-send as rich and delete the stale
    # preview until rich_message edit support is wired directly.
    # ------------------------------------------------------------------


    _RICH_DETAILS_RE = re.compile(r"<details\b[^>]*>.*?</details>", re.IGNORECASE | re.DOTALL)
    _RICH_MATH_IN_DETAILS_RE = re.compile(
        r"(\$\$.*?\$\$|"
        r"\\\[.*?\\\]|"
        r"\\\(.*?\\\)|"
        r"\\(?:sum|frac|alpha|beta|gamma|delta|theta|lambda|mu|pi|sigma|"
        r"int|prod|sqrt|lim|infty|begin\{(?:equation|align|matrix|cases)\}))",
        re.IGNORECASE | re.DOTALL,
    )
    _RICH_CJK_RE = re.compile(
        "["
        "\u3040-\u30ff"  # Hiragana, Katakana
        "\u3400-\u4dbf"  # CJK Extension A
        "\u4e00-\u9fff"  # CJK Unified Ideographs
        "\uac00-\ud7af"  # Hangul syllables
        "\uf900-\ufaff"  # CJK Compatibility Ideographs
        "\U00020000-\U000323af"  # CJK extensions and compatibility supplement
        "]"
    )







    def prefers_fresh_final_streaming(
        self, content: str, metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Whether to replace a streamed preview with a fresh rich final.

        Disabled for Telegram. The fresh-final path briefly shows two copies of
        the final answer, then deletes the streaming preview after the rich send
        succeeds — it looks like duplicate delivery at the end of every streamed
        turn (the reason #46206 reverted it).  Rich finalize is instead handled
        by editing the existing preview in place via Bot API 10.1's
        ``editMessageText`` ``rich_message`` parameter (see
        :meth:`_try_edit_rich`), so no fresh re-send / delete is needed.
        """
        return False

    def streaming_overflow_limit(self) -> Optional[int]:
        """Allow the stream consumer to accumulate up to the rich-message cap
        before splitting, so a reply that fits one ``sendRichMessage`` /
        ``sendRichMessageDraft`` isn't fragmented at the 4,096 MarkdownV2 limit.

        Gated on the same rich capability as the send path (minus the
        content-length check — raising that cap is the whole point): rich not
        latched off and the bot exposes an async ``do_api_request``.  Returns
        ``None`` (→ legacy 4,096 limit) when rich isn't available, so non-rich
        streams split exactly as before.
        """
        if (
            getattr(self, "_rich_messages_enabled", True)
            and not getattr(self, "_rich_send_disabled", False)
            and self._bot_supports_rich()
        ):
            return self.RICH_MESSAGE_MAX_CHARS
        return None




















































    # Template attrs for the shared _format_exec_approval core (HTML mode).
    _EA_HEADER = "⚠️ <b>Command Approval Required</b>\n\n"
    _EA_CODE_OPEN = "<pre>"
    _EA_CODE_CLOSE = "</pre>\n\n"
    _EA_SMART_DENY_LINE = "\n\n<b>Smart DENY:</b> owner override applies to this one operation only."
    _EA_CMD_BUDGET = 3800






    _PROVIDER_PAGE_SIZE = 10



    _MODEL_PAGE_SIZE = 8






    # Maps `gt:<verb>` -> (script-name, extra-args, success-label, is_state).
    # Scripts live in ~/.hermes/scripts/gmail-triage/. `arg` from the callback
    # data is always passed as the first positional arg.
    # is_state=True means the verb is a sticky sender-rule change (mute, trust,
    # vip) that should leave the keyboard tappable for follow-on actions.
    # is_state=False is a per-email one-shot (send, archive, draft, spam) that
    # strips the keyboard on success.
    _GT_VERB_DISPATCH = {
        "send":         ("send-draft.sh",      [],         "✓ sent draft",         False),
        "archive":      ("archive.sh",         [],         "✓ archived",           False),
        "draft":        ("draft-blank.sh",     [],         "✓ drafted reply",      False),
        "spam":         ("spam.sh",            [],         "✓ marked spam",        False),
        "mute":         ("mute-add.sh",        ["email"],  "✓ muted",              True),
        "mute-domain":  ("mute-add.sh",        ["domain"], "✓ muted domain",       True),
        "trust":        ("trusted-ops-add.sh", ["email"],  "✓ trusted",            True),
        "trust-domain": ("trusted-ops-add.sh", ["domain"], "✓ trusted domain",     True),
        "vip":          ("vip-add.sh",         ["email"],  "✓ marked VIP",         True),
        "vip-domain":   ("vip-add.sh",         ["domain"], "✓ marked VIP domain",  True),
    }
















    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Get information about a Telegram chat."""
        if not self._bot:
            return {"name": "Unknown", "type": "dm"}
        
        try:
            chat = await self._bot.get_chat(normalize_telegram_chat_id(chat_id))
            
            chat_type = "dm"
            if chat.type == ChatType.GROUP:
                chat_type = "group"
            elif chat.type == ChatType.SUPERGROUP:
                chat_type = "group"
                if chat.is_forum:
                    chat_type = "forum"
            elif chat.type == ChatType.CHANNEL:
                chat_type = "channel"
            
            return {
                "name": chat.title or chat.full_name or str(chat_id),
                "type": chat_type,
                "username": chat.username,
                "is_forum": getattr(chat, "is_forum", False),
            }
        except Exception as e:
            logger.error(
                "[%s] Failed to get Telegram chat info for %s: %s",
                self.name,
                chat_id,
                _redact_telegram_error_text(e),
                exc_info=True,
            )
            return {"name": str(chat_id), "type": "dm", "error": str(e)}


    # ── Group mention gating ──────────────────────────────────────────────


    def _telegram_observe_unmentioned_group_messages(self) -> bool:
        """Return whether skipped unmentioned group messages are stored as context.

        When enabled with ``require_mention``, Telegram matches the Yuanbao /
        OpenClaw-style group UX: observe ordinary group chatter in the session
        transcript, but only dispatch the agent when the bot is explicitly
        addressed.
        """
        configured = self.config.extra.get("observe_unmentioned_group_messages")
        if configured is None:
            configured = self.config.extra.get("ingest_unmentioned_group_messages")
        if configured is not None:
            if isinstance(configured, str):
                return configured.lower() in {"true", "1", "yes", "on"}
            return bool(configured)
        return os.getenv("TELEGRAM_OBSERVE_UNMENTIONED_GROUP_MESSAGES", "false").lower() in {"true", "1", "yes", "on"}











    def _compile_mention_patterns(self) -> List[re.Pattern]:
        """Compile optional regex wake-word patterns for group triggers."""
        patterns = self.config.extra.get("mention_patterns")
        if patterns is None:
            raw = os.getenv("TELEGRAM_MENTION_PATTERNS", "").strip()
            if raw:
                try:
                    loaded = json.loads(raw)
                except Exception:
                    loaded = [part.strip() for part in raw.splitlines() if part.strip()]
                    if not loaded:
                        loaded = [part.strip() for part in raw.split(",") if part.strip()]
                patterns = loaded

        if patterns is None:
            # Parity with the historical inline implementation: return before
            # evaluating ``self.name`` (tests construct bare adapters via
            # object.__new__ that lack the attributes ``name`` reads).
            return []

        return compile_mention_patterns(
            patterns,
            log_prefix=self.name,
            platform_label="telegram",
            display_label="Telegram",
            logger_=logger,
        )

    def _is_group_chat(self, message: Message) -> bool:
        chat = getattr(message, "chat", None)
        if not chat:
            return False
        chat_type = str(getattr(chat, "type", "")).split(".")[-1].lower()
        return chat_type in {"group", "supergroup"}

    @classmethod
    def _effective_message_thread_id(cls, message: Message) -> Optional[str]:
        """Return the routable thread id for a Telegram message.

        Forum supergroup messages posted in the General topic arrive with
        ``message_thread_id=None`` while Telegram itself addresses that topic
        as thread id ``1``.  Ordinary replies are the opposite footgun:
        Telegram populates ``message_thread_id`` with a reply-UI anchor id on
        plain group/DM replies, but those ids are not topic/session routing
        ids and must not be treated as such.  Gating, skill binding, and
        outbound routing must all agree on the same normalized value.
        """
        chat = getattr(message, "chat", None)
        chat_type = str(getattr(chat, "type", "")).split(".")[-1].lower() if chat else ""
        raw = getattr(message, "message_thread_id", None)
        is_topic_message = bool(getattr(message, "is_topic_message", False))
        is_forum_group = chat_type in ("group", "supergroup") and getattr(chat, "is_forum", False) is True
        if raw is not None:
            if is_forum_group or (chat_type in ("group", "supergroup") and is_topic_message):
                return str(raw)
            if chat_type == "private" and is_topic_message:
                return str(raw)
            return None
        if is_forum_group:
            return cls._GENERAL_TOPIC_THREAD_ID
        return None

    # Telegram bot handles historically had to end in "bot", but collectible
    # (Fragment) usernames can be assigned to bots and drop that suffix
    # entirely (@jarvis, @pic, ...). This pattern is used ONLY to decide
    # whether some FOREIGN @handle in a message is bot-shaped; our own handle
    # is matched by identity, never by shape.
    _FOREIGN_BOT_HANDLE_RE = re.compile(r"[a-z0-9_]{2,29}bot", re.IGNORECASE)
    # How long an observed identity is trusted before the heartbeat re-checks.
    _BOT_IDENTITY_TTL_SECONDS = 300.0

    def _current_bot_username(self) -> str:
        """Return this bot's live @username (lowercased, no leading ``@``).

        Prefers the most recently observed handle over PTB's ``get_me()``
        cache. ``Bot.username`` reads ``Bot._bot_user``, which is written only
        by ``get_me()`` — after a BotFather rename it keeps returning the old
        handle, so every mention comparison silently stops matching and the
        exclusive-mention gate concludes the message is addressed to a
        different bot. Observing the handle from inbound updates closes that
        window without an extra Bot API round-trip.
        """
        observed = getattr(self, "_bot_username_observed", None)
        if observed:
            return observed
        return (getattr(self._bot, "username", None) or "").lstrip("@").lower()

    def _note_bot_username(self, username: Optional[str]) -> None:
        """Record the bot's current @username, logging real renames."""
        handle = (username or "").lstrip("@").lower()
        if not handle:
            return
        previous = getattr(self, "_bot_username_observed", None)
        if previous == handle:
            return
        self._bot_username_observed = handle
        self._bot_identity_checked_at = time.monotonic()
        if previous:
            logger.info(
                "[%s] Telegram bot username changed: @%s -> @%s "
                "(mention routing now follows the new handle)",
                self.name, previous, handle,
            )

    def _observe_bot_identity_from_message(self, message: Message) -> None:
        """Learn our own handle from a message Telegram says we authored.

        Telegram stamps the *current* username on the bot's own outgoing
        messages and on ``reply_to_message`` when a user replies to us, so a
        rename is observable from the update stream itself — no getMe needed.
        Only trusted when the user id matches this bot, so another account's
        handle can never be adopted as our own.
        """
        bot_id = getattr(self._bot, "id", None)
        if bot_id is None:
            return
        for candidate in (
            getattr(message, "from_user", None),
            getattr(getattr(message, "reply_to_message", None), "from_user", None),
        ):
            if candidate is None:
                continue
            if getattr(candidate, "id", None) != bot_id:
                continue
            self._note_bot_username(getattr(candidate, "username", None))

    def _bot_identity_is_fresh(self) -> bool:
        """True when identity was re-read within the TTL.

        ``None`` means never checked, which is always stale. Do not fold the
        sentinel into ``0.0``: monotonic clocks have an arbitrary epoch that
        can legitimately be smaller than the TTL on a freshly-booted host,
        which would make "never" look like "just now".
        """
        checked_at = getattr(self, "_bot_identity_checked_at", None)
        if checked_at is None:
            return False
        return (time.monotonic() - checked_at) < self._BOT_IDENTITY_TTL_SECONDS

    async def _refresh_bot_identity(self, *, force: bool = False) -> None:
        """Re-read the bot's identity from Telegram when the cache may be stale.

        ``get_me()`` rewrites PTB's ``Bot._bot_user`` in place, so this also
        repairs every other consumer of ``self._bot.username``. Best-effort:
        a failed probe leaves the last known handle in place.
        """
        bot = self._bot
        if bot is None or not callable(getattr(bot, "get_me", None)):
            return
        if not force and self._bot_identity_is_fresh():
            return
        try:
            me = await asyncio.wait_for(bot.get_me(), self._BOT_IDENTITY_PROBE_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug(
                "[%s] Telegram identity refresh failed (keeping @%s): %s",
                self.name, self._current_bot_username() or "unknown", exc,
            )
            return
        self._bot_identity_checked_at = time.monotonic()
        self._note_bot_username(getattr(me, "username", None))

    _BOT_IDENTITY_PROBE_TIMEOUT = 15.0

    def _is_reply_to_bot(self, message: Message) -> bool:
        if not self._bot or not getattr(message, "reply_to_message", None):
            return False
        reply_user = getattr(message.reply_to_message, "from_user", None)
        return bool(reply_user and getattr(reply_user, "id", None) == getattr(self._bot, "id", None))

    @classmethod
    def _extract_bot_mention_usernames(cls, message: Message, self_username: str = "") -> set[str]:
        """Extract explicit Telegram bot usernames mentioned in text/captions.

        Foreign handles are only treated as bot mentions when they look
        bot-shaped (``...bot``), which keeps human ``@handles`` from acting as
        routing hints. ``self_username`` opts our OWN handle into the same set
        regardless of shape: collectible (Fragment) usernames can be assigned
        to bots and need not end in "bot" (@jarvis, @pic), and a bot addressed
        by such a handle must still recognise itself.

        Entity mentions are authoritative. The raw-text fallback is intentionally narrow so
        entity-less mobile/client variants still work without treating email
        addresses or arbitrary substrings as bot mentions.
        """
        mentioned_bot_usernames: set[str] = set()
        own = (self_username or "").lstrip("@").lower()

        def _is_bot_handle(handle: str) -> bool:
            if not handle:
                return False
            if own and handle == own:
                return True
            return bool(cls._FOREIGN_BOT_HANDLE_RE.fullmatch(handle))

        def _iter_sources():
            yield getattr(message, "text", None) or "", getattr(message, "entities", None) or []
            yield getattr(message, "caption", None) or "", getattr(message, "caption_entities", None) or []

        for source_text, entities in _iter_sources():
            for entity in entities:
                entity_type = str(getattr(entity, "type", "")).split(".")[-1].lower()
                if entity_type not in {"mention", "bot_command"}:
                    continue
                offset = int(getattr(entity, "offset", -1))
                length = int(getattr(entity, "length", 0))
                if offset < 0 or length <= 0:
                    continue

                entity_text = source_text[offset:offset + length].strip()
                if entity_type == "mention":
                    handle = entity_text.lstrip("@").lower()
                    if _is_bot_handle(handle):
                        mentioned_bot_usernames.add(handle)
                    continue

                # Telegram emits /cmd@botname as one bot_command entity, not as
                # a separate mention entity. Treat that suffix as an explicit
                # bot address for exclusive multi-bot routing even when the
                # group has require_mention/free-response disabled.
                at_index = entity_text.find("@")
                if at_index < 0:
                    continue
                command_target = entity_text[at_index + 1:].strip().lower()
                if _is_bot_handle(command_target):
                    mentioned_bot_usernames.add(command_target)

        # Entity-less fallback for older/client-specific updates. If Telegram
        # supplied entities for a source, trust them and do not regex-rescue
        # malformed/URL/code spans that the server did not mark as mentions.
        for raw_text, entities in _iter_sources():
            if not raw_text or entities:
                continue
            for match in re.finditer(r"(?i)(?<![A-Za-z0-9_`/])@([A-Za-z0-9_]{2,31})\b", raw_text):
                handle = match.group(1).lower()
                if _is_bot_handle(handle):
                    mentioned_bot_usernames.add(handle)

        return mentioned_bot_usernames

    def _message_mentions_bot(self, message: Message) -> bool:
        if not self._bot:
            return False

        bot_username = self._current_bot_username()
        bot_id = getattr(self._bot, "id", None)
        expected = f"@{bot_username}" if bot_username else None

        def _iter_sources():
            yield getattr(message, "text", None) or "", getattr(message, "entities", None) or []
            yield getattr(message, "caption", None) or "", getattr(message, "caption_entities", None) or []

        # Telegram parses mentions server-side and emits MessageEntity objects
        # (type=mention for @username, type=text_mention for @FirstName targeting
        # a user without a public username). Those entities are authoritative:
        # raw substring matches like "foo@hermes_bot.example" are not mentions
        # (bug #12545). Entities also correctly handle @handles inside URLs, code
        # blocks, and quoted text, where a regex scan would over-match.
        for source_text, entities in _iter_sources():
            for entity in entities:
                entity_type = str(getattr(entity, "type", "")).split(".")[-1].lower()
                if entity_type == "mention" and expected:
                    offset = int(getattr(entity, "offset", -1))
                    length = int(getattr(entity, "length", 0))
                    if offset < 0 or length <= 0:
                        continue
                    if source_text[offset:offset + length].strip().lower() == expected:
                        return True
                elif entity_type == "text_mention":
                    user = getattr(entity, "user", None)
                    if user and getattr(user, "id", None) == bot_id:
                        return True
                elif entity_type == "bot_command" and expected:
                    # Telegram's official group-disambiguation form for slash
                    # commands (``/cmd@botname``) is emitted as a single
                    # ``bot_command`` entity covering the whole span — there
                    # is no accompanying ``mention`` entity. Treat it as a
                    # direct address to this bot when the ``@botname`` suffix
                    # matches. This is the form Telegram's own command menu
                    # autocomplete produces in groups, so dropping it at the
                    # mention gate would break /new, /reset, /help, ... for
                    # every group that has ``require_mention`` enabled (#15415).
                    offset = int(getattr(entity, "offset", -1))
                    length = int(getattr(entity, "length", 0))
                    if offset < 0 or length <= 0:
                        continue
                    command_text = source_text[offset:offset + length]
                    at_index = command_text.find("@")
                    if at_index < 0:
                        continue
                    if command_text[at_index:].strip().lower() == expected:
                        return True
        if bot_username:
            return bot_username in self._extract_bot_mention_usernames(message, bot_username)
        return False

    def _schedule_bot_identity_recheck(self) -> None:
        """Fire a TTL-guarded identity refresh in the background.

        Called when routing is about to discard a message because the bot
        handles it names don't include ours — the exact symptom of a stale
        username after a BotFather rename. The TTL in
        ``_refresh_bot_identity`` bounds this to one getMe per
        ``_BOT_IDENTITY_TTL_SECONDS``, so a busy group that legitimately
        addresses other bots cannot turn this into per-message API traffic.
        Fire-and-forget: the current message still routes on what we know now.
        """
        existing = getattr(self, "_bot_identity_refresh_task", None)
        if existing is not None and not existing.done():
            return
        if self._bot_identity_is_fresh():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(self._refresh_bot_identity())
        self._bot_identity_refresh_task = task
        tracked = getattr(self, "_background_tasks", None)
        if isinstance(tracked, set):
            tracked.add(task)
            task.add_done_callback(tracked.discard)

    def _explicit_bot_mentions_exclude_self(self, message: Message) -> bool:
        """Return True when explicit bot handles target other bots, not this one.

        Telegram groups can contain several Hermes bot profiles. A message like
        ``@bot3 hi @bot4`` must not wake ``@bot1`` through reply/wake-word
        fallbacks. Treat explicit bot-handle mentions as an exclusive routing
        hint: if at least one @...bot username is present and none matches this
        adapter's own bot username, this adapter should ignore the message.

        MessageEntity values are preferred, but some Telegram clients expose
        selected bot handles as plain text in group messages. Foreign handles
        are limited to the ``...bot`` shape so human @handles never suppress
        this bot; our own handle is matched by identity, so a collectible
        username without that suffix still counts as addressing us.
        """
        if not self._bot:
            return False

        bot_username = self._current_bot_username()
        if not bot_username:
            return False

        mentioned_bot_usernames = self._extract_bot_mention_usernames(message, bot_username)
        excludes_self = bool(mentioned_bot_usernames) and bot_username not in mentioned_bot_usernames
        if excludes_self:
            # Either the message really is for another bot, or our cached
            # handle is stale after a rename and we are about to ignore a
            # message addressed to us. Re-check identity out of band (TTL
            # bounded) so the mistake self-corrects instead of persisting.
            self._schedule_bot_identity_recheck()
        return excludes_self

    def _message_matches_mention_patterns(self, message: Message) -> bool:
        if not self._mention_patterns:
            return False
        for candidate in (getattr(message, "text", None), getattr(message, "caption", None)):
            if not candidate:
                continue
            for pattern in self._mention_patterns:
                if pattern.search(candidate):
                    return True
        return False

    def _is_guest_mention(self, message: Message) -> bool:
        """Return True for the narrow guest-mode bypass: explicit bot mention.

        The caller (:meth:`_should_process_message`) has already verified
        the message is a group chat, so that check is not repeated here.
        """
        return self._telegram_guest_mode() and self._message_mentions_bot(message)

    def _clean_bot_trigger_text(self, text: Optional[str]) -> Optional[str]:
        bot_username = self._current_bot_username()
        if not text or not bot_username:
            return text
        username = re.escape(bot_username)
        cleaned = re.sub(rf"(?i)@{username}\b[,:\-]*\s*", "", text).strip()
        return cleaned or text

    def _should_observe_unmentioned_group_message(self, message: Message) -> bool:
        """Return True when a group message should be stored but not dispatched."""
        if self._is_own_message(message):
            return False
        if not self._telegram_observe_unmentioned_group_messages():
            return False
        if not self._is_group_chat(message):
            return False

        thread_id = getattr(message, "message_thread_id", None)
        allowed_topics = self._telegram_allowed_topics()
        if allowed_topics:
            topic_id = str(thread_id) if thread_id is not None else self._GENERAL_TOPIC_THREAD_ID
            if topic_id not in allowed_topics:
                return False

        if thread_id is not None:
            try:
                if int(thread_id) in self._telegram_ignored_threads():
                    return False
            except (TypeError, ValueError):
                return False

        chat_id_str = str(getattr(getattr(message, "chat", None), "id", ""))
        if self._telegram_exclusive_bot_mentions() and self._explicit_bot_mentions_exclude_self(message):
            return False

        allowed = self._telegram_observe_allowed_chats()
        # Observed context is shared at chat/topic scope so a later trigger from
        # another user can see it.  Require an explicit chat allowlist; that
        # keeps shared observed history limited to operator-approved groups and
        # lets gateway authorization pass even after the shared session source
        # drops the per-sender user_id.
        if not allowed or chat_id_str not in allowed:
            return False

        # Only observe messages skipped by the require_mention gate.  If the
        # message would be processed normally, let the dispatcher handle it;
        # if require_mention is disabled, every group message is a request.
        if chat_id_str in self._telegram_free_response_chats():
            return False
        if self._telegram_is_free_response_topic(message):
            return False
        if not self._telegram_require_mention():
            return False
        if self._is_reply_to_bot(message):
            return False
        if self._message_mentions_bot(message):
            return False
        if self._message_matches_mention_patterns(message):
            return False
        return True

    def _telegram_group_observe_shared_source(self, source):
        """Return a chat/topic-scoped source for observed Telegram group context."""
        return dataclasses.replace(source, user_id=None, user_name=None, user_id_alt=None)

    def _telegram_group_observe_attributed_text(self, event: MessageEvent) -> str:
        user_id = event.source.user_id or "unknown"
        sender = event.source.user_name or user_id
        return f"[{sender}|{user_id}]\n{event.text or ''}"

    def _telegram_group_observe_channel_prompt(self) -> str:
        username = self._current_bot_username() or "unknown"
        bot_id = getattr(getattr(self, "_bot", None), "id", None) or "unknown"
        return (
            "You are handling a Telegram group chat message.\n"
            f"- Your identity: user_id={bot_id}, @-mention name in this group=@{username}\n"
            "- observed Telegram group context may be provided in a separate context-only block "
            "before the current message; it is not necessarily addressed to you.\n"
            "- Treat only the current new message as a request explicitly directed at you, "
            "and use observed context only when the current message asks for it."
        )

    def _apply_telegram_group_observe_attribution(self, event: MessageEvent) -> MessageEvent:
        """Align triggered group turns with observed-history attribution."""
        if not self._telegram_observe_unmentioned_group_messages():
            return event
        raw_message = getattr(event, "raw_message", None)
        if not raw_message or not self._is_group_chat(raw_message):
            return event
        chat_id_str = str(getattr(getattr(raw_message, "chat", None), "id", ""))
        allowed = self._telegram_observe_allowed_chats()
        if not allowed or chat_id_str not in allowed:
            return event
        shared_source = self._telegram_group_observe_shared_source(event.source)
        observe_prompt = self._telegram_group_observe_channel_prompt()
        channel_prompt = f"{event.channel_prompt}\n\n{observe_prompt}" if event.channel_prompt else observe_prompt
        if event.message_type == MessageType.COMMAND:
            # Commands must retain the original source (with user_id) so
            # slash-access control (_check_slash_access) can identify the
            # sender.  Replacing the source with an anonymised shared source
            # (user_id=None) causes admin-only commands like /new to be
            # denied even when the sender is an admin, because
            # SlashAccessPolicy.is_admin(None) is always False.
            # Still inject channel_prompt for group context.
            return dataclasses.replace(
                event,
                channel_prompt=channel_prompt,
            )
        return dataclasses.replace(
            event,
            text=self._telegram_group_observe_attributed_text(event),
            source=shared_source,
            channel_prompt=channel_prompt,
        )















    # ------------------------------------------------------------------
    # Text message aggregation (handles Telegram client-side splits)
    # ------------------------------------------------------------------




    # ------------------------------------------------------------------
    # Photo batching
    # ------------------------------------------------------------------















    # ── Message reactions (processing lifecycle) ──────────────────────────







# ──────────────────────────────────────────────────────────────────────────
# Plugin migration glue (#41112 / #3823)
#
# Added when the Telegram adapter (+ its telegram_network satellite) moved from
# gateway/platforms/ into this bundled plugin. Mirrors the Discord (#24356) /
# Slack migrations: a register(ctx) entry point plus hook implementations that
# replace the per-platform core touchpoints (the Platform.TELEGRAM branch in
# gateway/run.py, the telegram_cfg YAML→env/extra block in gateway/config.py,
# the _setup_telegram wizard + _PLATFORMS["telegram"] static dict in
# hermes_cli/{setup,gateway}.py, and the _send_telegram dispatch in
# tools/send_message_tool.py).  Telegram uses the generic token connected
# check, so no is_connected override is needed.
# ──────────────────────────────────────────────────────────────────────────


def _resolve_notifications_mode() -> str:
    """Resolve the Telegram notification mode (all/important) from env or
    config.yaml display.platforms.telegram.notifications, defaulting to
    'important'.  Mirrors the post-construction logic that used to live in
    gateway/run.py::_create_adapter()."""
    mode = os.getenv("HERMES_TELEGRAM_NOTIFICATIONS", "")
    if not mode:
        try:
            from gateway.config import load_gateway_config
            from gateway.run import cfg_get
            _gw_cfg = load_gateway_config()
            _raw = cfg_get(_gw_cfg, "display", "platforms", "telegram", "notifications")
            if _raw not in {None, ""}:
                mode = str(_raw).strip().lower()
        except Exception:
            pass
    mode = mode or "important"
    if mode not in {"all", "important"}:
        logger.warning(
            "Unknown telegram notifications mode '%s', defaulting to 'important' "
            "(valid: all, important)", mode,
        )
        mode = "important"
    return mode


def _build_adapter(config):
    """Factory wrapper that constructs TelegramAdapter and applies the
    notification mode (preserving the gateway/run.py post-construction step)."""
    adapter = TelegramAdapter(config)
    try:
        adapter._notifications_mode = _resolve_notifications_mode()
    except Exception:
        adapter._notifications_mode = "important"
    return adapter


def _is_connected(config) -> bool:
    """Telegram is connected when a bot token is configured.

    check_telegram_requirements() only verifies the python-telegram-bot SDK is
    importable, NOT that a token is set — so without this is_connected the
    registry-driven plugin-enable pass in gateway/config.py would enable
    Telegram on any machine that merely has the SDK installed. Gate on the
    token (env or PlatformConfig.token), matching the generic token check
    Telegram had as a built-in.
    """
    token = getattr(config, "token", None)
    if not token:
        import hermes_cli.gateway as gateway_mod
        token = gateway_mod.get_env_value("TELEGRAM_BOT_TOKEN") or ""
    return bool(str(token).strip())


async def _standalone_send(
    pconfig,
    chat_id,
    message,
    *,
    thread_id=None,
    media_files=None,
    force_document=False,
):
    """Out-of-process Telegram delivery. Delegates to the standalone
    ``_send_telegram`` REST sender in tools/send_message_tool.py (which already
    handles chunking-agnostic single sends, threads, media, retries, and
    parse-mode fallback). Implements the standalone_sender_fn contract so
    deliver=telegram cron jobs succeed when cron runs separately from the
    gateway."""
    token = getattr(pconfig, "token", None)
    if not token:
        # Profile-scoped read: honor the secret scope's verdict rather than
        # borrowing another profile's env-bridged token under multiplex.
        from agent.secret_scope import get_secret

        token = get_secret("TELEGRAM_BOT_TOKEN", "") or ""
    disable_link_previews = bool(
        getattr(pconfig, "extra", {}) and pconfig.extra.get("disable_link_previews")
    )
    from tools.send_message_tool import _send_telegram
    return await _send_telegram(
        token,
        chat_id,
        message,
        media_files=media_files,
        thread_id=thread_id,
        disable_link_previews=disable_link_previews,
        force_document=force_document,
    )


def interactive_setup() -> None:
    """Configure Telegram bot credentials and allowlist.

    Delegates to the existing CLI setup helpers (managed-bot QR onboarding,
    token validation, allowlist capture) via lazy import so the full wizard
    behavior is preserved without duplicating ~150 lines. Replaces the
    _PLATFORMS["telegram"] static dict dispatch in hermes_cli/gateway.py.
    """
    from hermes_cli import setup as _setup_mod
    _setup_mod._setup_telegram()


def _apply_yaml_config(yaml_cfg: dict, telegram_cfg: dict) -> dict | None:
    """Translate config.yaml telegram: keys into TELEGRAM_* env vars and
    PlatformConfig.extra entries.

    Implements the apply_yaml_config_fn contract (#24849). Mirrors the legacy
    telegram_cfg block from gateway/config.py::load_gateway_config(). Env vars
    take precedence over YAML. Returns a dict of extras to merge into
    PlatformConfig.extra (disable_topic_auto_rename + runtime flags), or None.
    """
    import json as _json
    extras: dict = {}

    # Under multiplex, a secondary profile's config loads inside its runtime
    # scope; its authorization gate values must NOT be written to the
    # process-global env, where first-writer-wins would pin them for every
    # other profile (issue #72348 Telegram mirror). They are seeded into
    # PlatformConfig.extra / read via the profile secret scope instead.
    try:
        from agent.secret_scope import current_secret_scope, is_multiplex_active

        _skip_env_bridge = bool(is_multiplex_active() and current_secret_scope() is not None)
    except Exception:
        _skip_env_bridge = False

    if "disable_topic_auto_rename" in telegram_cfg:
        extras.setdefault("disable_topic_auto_rename", telegram_cfg["disable_topic_auto_rename"])

    _effective_rm = telegram_cfg.get("require_mention", yaml_cfg.get("require_mention"))
    if _effective_rm is not None and not os.getenv("TELEGRAM_REQUIRE_MENTION"):
        os.environ["TELEGRAM_REQUIRE_MENTION"] = str(_effective_rm).lower()
    if "mention_patterns" in telegram_cfg and not os.getenv("TELEGRAM_MENTION_PATTERNS"):
        os.environ["TELEGRAM_MENTION_PATTERNS"] = _json.dumps(telegram_cfg["mention_patterns"])
    if "exclusive_bot_mentions" in telegram_cfg and not os.getenv("TELEGRAM_EXCLUSIVE_BOT_MENTIONS"):
        os.environ["TELEGRAM_EXCLUSIVE_BOT_MENTIONS"] = str(telegram_cfg["exclusive_bot_mentions"]).lower()
    if "allow_bots" in telegram_cfg and not os.getenv("TELEGRAM_ALLOW_BOTS"):
        os.environ["TELEGRAM_ALLOW_BOTS"] = str(telegram_cfg["allow_bots"]).lower()
    if "guest_mode" in telegram_cfg and not os.getenv("TELEGRAM_GUEST_MODE"):
        os.environ["TELEGRAM_GUEST_MODE"] = str(telegram_cfg["guest_mode"]).lower()
    if "observe_unmentioned_group_messages" in telegram_cfg and not os.getenv("TELEGRAM_OBSERVE_UNMENTIONED_GROUP_MESSAGES"):
        os.environ["TELEGRAM_OBSERVE_UNMENTIONED_GROUP_MESSAGES"] = str(telegram_cfg["observe_unmentioned_group_messages"]).lower()
    frc = telegram_cfg.get("free_response_chats")
    if frc is not None:
        extras.setdefault("free_response_chats", frc)
        if isinstance(frc, list):
            frc = ",".join(str(v) for v in frc)
        if not _skip_env_bridge and not os.getenv("TELEGRAM_FREE_RESPONSE_CHATS"):
            os.environ["TELEGRAM_FREE_RESPONSE_CHATS"] = str(frc)
    frt = telegram_cfg.get("free_response_topics")
    if frt is not None:
        if isinstance(frt, list):
            frt = ",".join(str(v) for v in frt)
        if not _skip_env_bridge and not os.getenv("TELEGRAM_FREE_RESPONSE_TOPICS"):
            os.environ["TELEGRAM_FREE_RESPONSE_TOPICS"] = str(frt)
    ac = telegram_cfg.get("allowed_chats")
    if ac is not None:
        if isinstance(ac, list):
            ac = ",".join(str(v) for v in ac)
        # NOTE: no extras seed here — gateway/config.py's shared-key loop
        # already bridges ``allowed_chats`` into PlatformConfig.extra with its
        # original type, and the apply_yaml_config merge would clobber it.
        if not _skip_env_bridge and not os.getenv("TELEGRAM_ALLOWED_CHATS"):
            os.environ["TELEGRAM_ALLOWED_CHATS"] = str(ac)
    allowed_topics = telegram_cfg.get("allowed_topics")
    if allowed_topics is not None:
        if isinstance(allowed_topics, list):
            allowed_topics = ",".join(str(v) for v in allowed_topics)
        # extras seed intentionally omitted (shared-key loop bridges allowed_topics).
        if not _skip_env_bridge and not os.getenv("TELEGRAM_ALLOWED_TOPICS"):
            os.environ["TELEGRAM_ALLOWED_TOPICS"] = str(allowed_topics)
    ignored_threads = telegram_cfg.get("ignored_threads")
    if ignored_threads is not None:
        extras.setdefault("ignored_threads", ignored_threads)
        if isinstance(ignored_threads, list):
            ignored_threads = ",".join(str(v) for v in ignored_threads)
        if not _skip_env_bridge and not os.getenv("TELEGRAM_IGNORED_THREADS"):
            os.environ["TELEGRAM_IGNORED_THREADS"] = str(ignored_threads)
    if "reactions" in telegram_cfg and not os.getenv("TELEGRAM_REACTIONS"):
        os.environ["TELEGRAM_REACTIONS"] = str(telegram_cfg["reactions"]).lower()
    if "proxy_url" in telegram_cfg and not os.getenv("TELEGRAM_PROXY"):
        os.environ["TELEGRAM_PROXY"] = str(telegram_cfg["proxy_url"]).strip()
    _telegram_extra = telegram_cfg.get("extra") if isinstance(telegram_cfg.get("extra"), dict) else {}
    _telegram_rtm = (
        telegram_cfg["reply_to_mode"] if "reply_to_mode" in telegram_cfg
        else _telegram_extra.get("reply_to_mode")
    )
    if _telegram_rtm is not None and not os.getenv("TELEGRAM_REPLY_TO_MODE"):
        _rtm_str = "off" if _telegram_rtm is False else str(_telegram_rtm).lower()
        os.environ["TELEGRAM_REPLY_TO_MODE"] = _rtm_str
    allowed_users = telegram_cfg.get("allow_from")
    if allowed_users is not None:
        if isinstance(allowed_users, list):
            allowed_users = ",".join(str(v) for v in allowed_users)
        if not _skip_env_bridge and not os.getenv("TELEGRAM_ALLOWED_USERS"):
            os.environ["TELEGRAM_ALLOWED_USERS"] = str(allowed_users)
    group_allowed_users = telegram_cfg.get("group_allow_from") or _telegram_extra.get("group_allow_from")
    if group_allowed_users is not None:
        if isinstance(group_allowed_users, list):
            group_allowed_users = ",".join(str(v) for v in group_allowed_users)
        if not _skip_env_bridge and not os.getenv("TELEGRAM_GROUP_ALLOWED_USERS"):
            os.environ["TELEGRAM_GROUP_ALLOWED_USERS"] = str(group_allowed_users)
    group_allowed_chats = telegram_cfg.get("group_allowed_chats") or _telegram_extra.get("group_allowed_chats")
    if group_allowed_chats is not None:
        if isinstance(group_allowed_chats, list):
            group_allowed_chats = ",".join(str(v) for v in group_allowed_chats)
        # extras seed intentionally omitted (shared-key loop bridges group_allowed_chats).
        if not _skip_env_bridge and not os.getenv("TELEGRAM_GROUP_ALLOWED_CHATS"):
            os.environ["TELEGRAM_GROUP_ALLOWED_CHATS"] = str(group_allowed_chats)
    for _key in ("guest_mode", "disable_link_previews", "observe_unmentioned_group_messages", "free_response_topics"):
        if _key in telegram_cfg:
            extras.setdefault(_key, telegram_cfg[_key])
    # Pass through telegram-specific extra keys (e.g. base_url proxy override),
    # but EXCLUDE the generic shared-config keys that _merge_platform_map in
    # gateway/config.py already merges with correct top-level-over-nested
    # precedence. The apply_yaml_config_fn dispatch merges our return via
    # dict.update() (clobber), so re-emitting those generic keys here would
    # undo that precedence (top-level losing to a nested-fallback block).
    _GENERIC_MERGE_KEYS = {
        "reply_prefix", "reply_in_thread", "reply_to_mode",
        "unauthorized_dm_behavior", "notice_delivery", "require_mention",
        "channel_skill_bindings", "channel_prompts", "gateway_restart_notification",
        "allow_from", "allow_admin_from", "dm_policy", "group_policy",
    }
    for _k, _v in _telegram_extra.items():
        if _k not in _GENERIC_MERGE_KEYS:
            extras.setdefault(_k, _v)

    return extras or None


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="telegram",
        label="Telegram",
        adapter_factory=_build_adapter,
        check_fn=check_telegram_requirements,
        is_connected=_is_connected,
        required_env=["TELEGRAM_BOT_TOKEN"],
        install_hint="Run `hermes setup` to install Telegram support.",
        setup_fn=interactive_setup,
        apply_yaml_config_fn=_apply_yaml_config,
        allowed_users_env="TELEGRAM_ALLOWED_USERS",
        allow_all_env="TELEGRAM_ALLOW_ALL_USERS",
        cron_deliver_env_var="TELEGRAM_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        max_message_length=4096,
        emoji="✈️",
        allow_update_command=True,
    )
