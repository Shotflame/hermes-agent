"""S1 cluster mixin: TelegramNetworkMixin."""

from __future__ import annotations

import asyncio
import os
import time
from typing import Dict, List, Optional, Set, Any
import sys
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
from plugins.platforms.telegram.telegram_network import (
    TelegramFallbackTransport,
    discover_fallback_ips,
    parse_fallback_ip_env,
)
from utils import atomic_replace, env_float, env_int

from ..adapter import (
    Application,
    CallbackQueryHandler,
    HTTPXRequest,
    TELEGRAM_AVAILABLE,
    TelegramMessageHandler,
    Update,
    _DRAIN_TIMEOUT,
    _INITIAL_POLLING_PROGRESS_TIMEOUT,
    _POLLING_ERROR_TASK_STUCK_TIMEOUT,
    _POLLING_GENERATION_CONTEXT,
    _POLLING_PROGRESS_TIMEOUT,
    _PollingLifecycleAbort,
    _UPDATER_START_TIMEOUT,
    _UPDATER_STOP_TIMEOUT,
    _await_with_thread_deadline,
    _first_completed,
    _redact_telegram_error_text,
    _shutdown_abandoned_app,
    filters,
    logger,
)

class TelegramNetworkMixin:
    def _fallback_ips(self) -> list[str]:
        """Return validated fallback IPs from config (populated by _apply_env_overrides)."""
        configured = self.config.extra.get("fallback_ips", []) if getattr(self.config, "extra", None) else []
        if isinstance(configured, str):
            configured = configured.split(",")
        return parse_fallback_ip_env(",".join(str(v) for v in configured) if configured else None)

    @staticmethod
    def _looks_like_polling_conflict(error: Exception) -> bool:
        text = str(error).lower()
        return (
            error.__class__.__name__.lower() == "conflict"
            or "terminated by other getupdates request" in text
            or "another bot instance is running" in text
        )

    @staticmethod
    def _looks_like_network_error(error: Exception) -> bool:
        """Return True for transient transport failures that warrant reconnect."""
        name = error.__class__.__name__.lower()
        if name in {"badrequest", "invalidtoken", "forbidden", "retryafter"}:
            return False
        if name in {"networkerror", "timedout", "connectionerror"}:
            return True
        try:
            from telegram.error import (
                BadRequest,
                Forbidden,
                InvalidToken,
                NetworkError,
                RetryAfter,
                TimedOut,
            )
            if isinstance(error, (BadRequest, InvalidToken, Forbidden, RetryAfter)):
                return False
            if isinstance(error, (NetworkError, TimedOut)):
                return True
        except ImportError:
            pass
        return isinstance(error, OSError)

    @staticmethod
    def _looks_like_connect_timeout(error: Exception) -> bool:
        """Return True when a Telegram TimedOut wraps a connect-timeout.

        A plain Telegram TimedOut may mean the request reached Telegram and
        should not be re-sent. A ConnectTimeout means the TCP connection was
        never established, so retrying is safe and prevents silent drops.
        """
        seen: set[int] = set()
        stack: list[BaseException] = [error]
        while stack:
            cur = stack.pop()
            ident = id(cur)
            if ident in seen:
                continue
            seen.add(ident)
            name = cur.__class__.__name__.lower()
            text = str(cur).lower()
            if "connecttimeout" in name or "connect timeout" in text or "connect timed out" in text:
                return True
            cause = getattr(cur, "__cause__", None)
            context = getattr(cur, "__context__", None)
            if cause is not None:
                stack.append(cause)
            if context is not None:
                stack.append(context)
        return False

    @staticmethod
    def _looks_like_pool_timeout(error: Exception) -> bool:
        """Return True when a Telegram TimedOut wraps an httpx pool timeout.

        PTB converts ``httpx.PoolTimeout`` into ``telegram.error.TimedOut`` with
        a message that explicitly states the request was *not* sent
        (``"Pool timeout: All connections in the connection pool are occupied.
        Request was *not* sent to Telegram."``). Because the request never left
        the process, re-sending is safe and cannot duplicate -- the opposite of
        a generic TimedOut, which may have reached Telegram. We match the
        wrapped ``httpx.PoolTimeout`` class as well as the message string so the
        check survives PTB message-wording changes.
        """
        seen: set[int] = set()
        stack: list[BaseException] = [error]
        while stack:
            cur = stack.pop()
            ident = id(cur)
            if ident in seen:
                continue
            seen.add(ident)
            name = cur.__class__.__name__.lower()
            text = str(cur).lower()
            if "pooltimeout" in name or "pool timeout" in text or (
                "connection pool" in text and "occupied" in text
            ):
                return True
            cause = getattr(cur, "__cause__", None)
            context = getattr(cur, "__context__", None)
            if cause is not None:
                stack.append(cause)
            if context is not None:
                stack.append(context)
        return False

    async def _drain_polling_connections(self) -> None:
        """Reset the httpx connection pool used for getUpdates polling.

        Network errors (especially through proxies like sing-box) can leave
        httpx connections in a half-closed state that still occupy pool slots.
        After enough reconnect cycles the pool fills up entirely, causing
        ``Pool timeout: All connections in the connection pool are occupied.``

        We reset ONLY ``_request[0]`` (the getUpdates request) — the general
        request (``_request[1]``) is left untouched so concurrent
        ``send_message`` / ``edit_message`` calls are never interrupted.

        Implementation note: accesses ``Bot._request[0]`` which is the
        get-updates ``BaseRequest`` in the PTB 22.x internal tuple
        ``(get_updates_request, general_request)``.  There is no public
        accessor for the polling request; review if upgrading to PTB 23+.
        """
        if not (self._app and self._app.bot):
            return
        try:
            # PTB 22.x: _request is a (get_updates, general) tuple;
            # no public accessor exists for the polling request.
            polling_req = self._app.bot._request[0]  # noqa: SLF001
        except Exception:
            return
        try:
            # Bounded: a wedged CLOSE-WAIT socket can make this close hang
            # forever and freeze the reconnect ladder (#66377).
            await asyncio.wait_for(polling_req.shutdown(), timeout=_DRAIN_TIMEOUT)
        except Exception:
            logger.debug(
                "[%s] Polling request shutdown failed/timed out (non-fatal)",
                self.name, exc_info=True,
            )
        try:
            await asyncio.wait_for(polling_req.initialize(), timeout=_DRAIN_TIMEOUT)
            logger.debug(
                "[%s] Polling request pool drained before reconnect", self.name
            )
        except Exception:
            logger.debug(
                "[%s] Polling request re-initialize failed/timed out (non-fatal)",
                self.name, exc_info=True,
            )

    def _begin_polling_generation(self) -> tuple[int, asyncio.Event]:
        """Start accepting progress for a new getUpdates polling generation."""
        if getattr(self, "_polling_teardown_started", False):
            self._polling_progress_accepting = False
            self._send_path_degraded = True
            progress = getattr(self, "_polling_progress_event", None)
            if progress is None:
                progress = asyncio.Event()
                self._polling_progress_event = progress
            return getattr(self, "_polling_generation", 0), progress

        verifier = getattr(self, "_polling_progress_verifier_task", None)
        if verifier is not None and not verifier.done():
            verifier.cancel()
        self._polling_progress_verifier_task = None
        self._polling_generation = getattr(self, "_polling_generation", 0) + 1
        self._polling_progress_event = asyncio.Event()
        self._polling_progress_accepting = True
        self._send_path_degraded = True
        return self._polling_generation, self._polling_progress_event

    def _record_polling_progress(self, generation: int) -> None:
        """Record successful getUpdates I/O for the current generation only."""
        if getattr(self, "_polling_teardown_started", False):
            return
        if not self._polling_progress_accepting:
            return
        if generation != self._polling_generation:
            return
        self._polling_progress_event.set()
        self._polling_network_error_count = 0
        self._polling_conflict_count = 0
        self._send_path_degraded = False

    def _observe_polling_request_result(self, request, generation, result):
        """Record getUpdates progress from an observed do_request result.

        Purely observational: PTB still parses the untouched payload and owns
        any resulting exception. Kept as its own method so the observation
        logic is shared and independently testable.
        """
        status_code, payload = result
        if generation is None or not (200 <= status_code < 300):
            return
        try:
            # Use the request's own parser so health observation agrees
            # exactly with PTB's authoritative response handling (e.g.
            # UTF-8 replacement decoding and BOM rejection).
            envelope = request.parse_json_payload(payload)
        except Exception:
            return
        if (
            isinstance(envelope, dict)
            and envelope.get("ok") is True
            and "result" in envelope
        ):
            self._record_polling_progress(generation)

    def _instrument_polling_request(self, request):
        """Instrument one dedicated PTB getUpdates request with progress tracking.

        PTB's request classes (``BaseRequest`` / ``HTTPXRequest``) use
        ``__slots__``. On Python 3.13 their instances no longer carry a
        ``__dict__`` (the ``AbstractAsyncContextManager`` MRO stopped yielding
        one), so ``request.do_request = wrapper`` raises
        ``AttributeError: 'HTTPXRequest' object attribute 'do_request' is
        read-only`` and the whole Telegram connect fails (#64482). It only
        appeared to work on Python 3.12, where those instances still had a
        ``__dict__``.

        Instead of monkey-patching the instance, re-tag it to a thin subclass
        that overrides ``do_request``. This is portable across Python versions
        and works for both the real request and the test doubles. The subclass
        declares ``__slots__ = ()`` so its instance layout stays identical to
        the base, which is what makes the ``__class__`` swap legal on a slotted
        instance.
        """
        adapter = self
        base_cls = type(request)

        class _InstrumentedPollingRequest(base_cls):
            __slots__ = ()

            async def do_request(self, *args, **kwargs):
                generation = _POLLING_GENERATION_CONTEXT.get()
                result = await super().do_request(*args, **kwargs)
                adapter._observe_polling_request_result(self, generation, result)
                return result

        request.__class__ = _InstrumentedPollingRequest
        return request

    async def _start_polling_once(
        self,
        app,
        *,
        drop_pending_updates: bool,
        error_callback,
        abandon_app_on_timeout: bool = False,
        schedule_verifier: bool = True,
    ) -> tuple[int, asyncio.Event]:
        """Start one generation and verify real getUpdates progress.

        Returns the ``(generation, progress_event)`` pair created for this
        polling generation so callers that must gate on readiness (strict
        cold start, #67498) can bind to exactly this generation instead of
        re-reading ``self._polling_progress_event`` — which a concurrent
        recovery task may have replaced with a newer generation's event.
        """
        if getattr(self, "_polling_teardown_started", False):
            raise _PollingLifecycleAbort("Telegram polling teardown started")
        generation, progress = self._begin_polling_generation()
        if not self._polling_progress_accepting:
            raise _PollingLifecycleAbort("Telegram polling teardown started")

        def _generation_error_callback(error: Exception) -> None:
            if getattr(self, "_polling_teardown_started", False):
                return
            if generation != self._polling_generation:
                return
            if error_callback is not None:
                callback_context_token = _POLLING_GENERATION_CONTEXT.set(None)
                try:
                    error_callback(error)
                finally:
                    _POLLING_GENERATION_CONTEXT.reset(callback_context_token)

        context_token = _POLLING_GENERATION_CONTEXT.set(generation)
        try:
            # asyncio.wait_for can wait forever for cancellation to escape
            # httpcore/AnyIO shielded scopes (#58236/#67498). Reuse the
            # proven wall-deadline helper and abandon the partial updater;
            # caller recovery will dispose/rebuild the whole adapter.
            await _await_with_thread_deadline(
                app.updater.start_polling(
                    allowed_updates=Update.ALL_TYPES,
                    drop_pending_updates=drop_pending_updates,
                    error_callback=_generation_error_callback,
                ),
                timeout=_UPDATER_START_TIMEOUT,
                on_abandon=(
                    (lambda app=app: _shutdown_abandoned_app(app))
                    if abandon_app_on_timeout
                    else None
                ),
            )
        finally:
            _POLLING_GENERATION_CONTEXT.reset(context_token)
        if getattr(self, "_polling_teardown_started", False):
            self._polling_progress_accepting = False
            self._send_path_degraded = True
            raise _PollingLifecycleAbort("Telegram polling teardown started")
        if schedule_verifier:
            self._schedule_polling_progress_verifier(generation, progress)
        return generation, progress

    def _schedule_polling_progress_verifier(
        self, generation: int, progress: asyncio.Event
    ) -> None:
        """Own exactly one tracked verifier for the current generation."""
        if getattr(self, "_polling_teardown_started", False):
            self._polling_progress_accepting = False
            self._send_path_degraded = True
            return
        previous = getattr(self, "_polling_progress_verifier_task", None)
        if previous is not None and not previous.done():
            previous.cancel()

        task = asyncio.get_running_loop().create_task(
            self._verify_polling_after_reconnect(generation, progress)
        )
        self._polling_progress_verifier_task = task
        self._background_tasks.add(task)

        def _clear_finished_verifier(finished: asyncio.Task) -> None:
            self._background_tasks.discard(finished)
            if self._polling_progress_verifier_task is finished:
                self._polling_progress_verifier_task = None

        task.add_done_callback(_clear_finished_verifier)

    def _get_general_request_drain_lock(self) -> asyncio.Lock:
        lock = getattr(self, "_general_request_drain_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._general_request_drain_lock = lock
        return lock

    async def _drain_general_connections_after_pool_timeout(self) -> None:
        """Reset the Bot API request pool after a confirmed send pool timeout.

        ``send_message`` uses PTB's general request pool (``_request[1]``).
        When httpx reports that this pool is exhausted, PTB says the request
        was not sent, so it is safe to reset the wedged pool before retrying.
        """
        bot = getattr(getattr(self, "_app", None), "bot", None)
        if bot is None:
            bot = getattr(self, "_bot", None)
        if bot is None:
            return
        try:
            # PTB 22.x: _request is (get_updates_request, general_request).
            general_req = bot._request[1]  # noqa: SLF001
        except Exception:
            return
        async with self._get_general_request_drain_lock():
            try:
                await general_req.shutdown()
            except Exception:
                logger.debug(
                    "[%s] General request shutdown failed after pool timeout (non-fatal)",
                    self.name, exc_info=True,
                )
            try:
                await general_req.initialize()
                logger.warning(
                    "[%s] General request pool drained after Telegram pool timeout",
                    self.name,
                )
            except Exception:
                logger.debug(
                    "[%s] General request re-initialize failed after pool timeout (non-fatal)",
                    self.name, exc_info=True,
                )

    def _schedule_polling_recovery(self, error: Exception, *, reason: str) -> None:
        """Schedule polling recovery without failing gateway startup.

        A Telegram bootstrap failure (deleteWebhook / initial start_polling)
        caused by a transient network error should degrade only the Telegram
        adapter: the gateway process stays alive and the existing reconnect
        ladder (``_handle_polling_network_error``) recovers in the background.
        """
        if getattr(self, "_polling_teardown_started", False):
            return
        if self.has_fatal_error:
            return
        if self._polling_error_task and not self._polling_error_task.done():
            logger.debug(
                "[%s] Telegram polling recovery already scheduled; ignoring %s: %s",
                self.name, reason, _redact_telegram_error_text(error),
            )
            return
        self._send_path_degraded = True
        logger.warning(
            "[%s] Telegram polling degraded (%s); gateway stays alive and will retry. Error: %s",
            self.name, reason, _redact_telegram_error_text(error),
        )
        loop = asyncio.get_running_loop()
        self._polling_error_task = loop.create_task(self._handle_polling_network_error(error))
        self._background_tasks.add(self._polling_error_task)
        self._polling_error_task.add_done_callback(self._background_tasks.discard)

    async def _delete_webhook_best_effort(
        self, *, require_success: bool = False
    ) -> bool:
        """Clear stale webhook, optionally failing closed on initial connect.

        Reconnect can recover a transient error in background. Cold startup uses
        ``require_success`` so GatewayRunner disposes the partial adapter and
        retries with a fresh PTB Application instead of publishing degraded state.
        """
        if not self._bot:
            return False
        delete_webhook = getattr(self._bot, "delete_webhook", None)
        if not callable(delete_webhook):
            return True
        try:
            # Same shielded-cancellation class as initialize/start_polling:
            # never let a wedged duplicate deleteWebhook pin initial connect.
            await _await_with_thread_deadline(
                delete_webhook(drop_pending_updates=False),
                timeout=_UPDATER_START_TIMEOUT,
            )
            return True
        except Exception as err:
            if self._looks_like_network_error(err):
                if require_success:
                    raise OSError(
                        "Telegram deleteWebhook did not complete during initial connect"
                    ) from err
                logger.warning(
                    "[%s] deleteWebhook failed with a recoverable network error; "
                    "continuing to polling so getUpdates/retry can recover: %s",
                    self.name, _redact_telegram_error_text(err),
                )
                self._send_path_degraded = True
                return False
            raise

    async def _start_polling_resilient(
        self,
        *,
        drop_pending_updates: bool,
        error_callback,
        require_progress: bool = False,
    ) -> bool:
        """Start PTB polling and optionally require real getUpdates readiness.

        Reconnects may recover in background. Initial connect sets
        ``require_progress`` so a bootstrap failure or missing first successful
        getUpdates response raises; GatewayRunner then disposes this partial
        adapter and retries with a fresh PTB Application.
        """
        if getattr(self, "_polling_teardown_started", False):
            return False
        if not (self._app and self._app.updater):
            raise RuntimeError("Telegram application/updater not initialized")

        # Strict cold start (#67498): background recovery must not run while
        # the readiness gate is waiting. A G1 polling error would otherwise
        # schedule _handle_polling_network_error(), which starts generation
        # G2 on the same partial application while this coroutine still waits
        # on G1's event — the cold connect then either times out on G1 despite
        # G2 succeeding, or G2 "heals" the partial app so GatewayRunner never
        # disposes it and retries fresh. Instead, capture the first polling
        # error and fail the cold attempt immediately; GatewayRunner owns
        # disposal and retry with a fresh adapter.
        strict_error: list[BaseException] = []
        strict_error_event = asyncio.Event()
        strict_gate_open = True
        effective_callback = error_callback
        if require_progress:
            loop = asyncio.get_running_loop()

            def _strict_error_callback(error: Exception) -> None:
                # PTB registers this callback for the whole polling
                # generation. After the readiness gate closes (success),
                # delegate to the real callback so ongoing polling errors
                # keep flowing into background recovery.
                if not strict_gate_open:
                    if error_callback is not None:
                        error_callback(error)
                    return
                if not strict_error:
                    strict_error.append(error)
                # PTB invokes error callbacks from the polling task; the
                # event must be set on the loop to wake the strict waiter.
                loop.call_soon_threadsafe(strict_error_event.set)

            effective_callback = _strict_error_callback
        try:
            # Same watchdog bound as the reconnect ladders: a wedged httpx
            # connection pool can hang start_polling() forever at bootstrap
            # too (#59614). A propagating TimeoutError is a builtins
            # TimeoutError (OSError subclass), so the except below classifies
            # it via _looks_like_network_error and schedules background
            # recovery instead of blocking connect() indefinitely.
            generation, progress = await self._start_polling_once(
                self._app,
                drop_pending_updates=drop_pending_updates,
                error_callback=effective_callback,
                abandon_app_on_timeout=require_progress,
                # The strict gate below IS the cold-start verifier; the
                # background verifier would only race it on the partial app.
                schedule_verifier=not require_progress,
            )
            if require_progress:
                # Bind to THIS generation's progress event (returned above),
                # not self._polling_progress_event — a concurrent task could
                # have replaced it with a later generation's event.
                progress_wait = asyncio.ensure_future(progress.wait())
                error_wait = asyncio.ensure_future(strict_error_event.wait())
                try:
                    await _await_with_thread_deadline(
                        _first_completed(progress_wait, error_wait),
                        timeout=_INITIAL_POLLING_PROGRESS_TIMEOUT,
                    )
                except asyncio.TimeoutError as exc:
                    raise OSError(
                        "Telegram getUpdates made no progress within "
                        f"{_INITIAL_POLLING_PROGRESS_TIMEOUT:.0f}s during initial "
                        "connect — failing startup so the gateway retries with a "
                        "fresh adapter (#67498)"
                    ) from exc
                finally:
                    for fut in (progress_wait, error_wait):
                        if not fut.done():
                            fut.cancel()
                    await asyncio.gather(
                        progress_wait, error_wait, return_exceptions=True
                    )
                if strict_error and not progress.is_set():
                    raise OSError(
                        "Telegram polling errored before first getUpdates "
                        "success during initial connect: "
                        f"{_redact_telegram_error_text(strict_error[0])}"
                    ) from strict_error[0]
                if not progress.is_set():
                    raise OSError(
                        "Telegram getUpdates did not become ready during initial connect"
                    )
                # Readiness proven — close the strict gate so any later
                # polling error flows to the real background-recovery
                # callback instead of the (now finished) cold-start gate.
                strict_gate_open = False
                self._polling_error_callback_ref = error_callback
            return True
        except _PollingLifecycleAbort:
            return False
        except Exception as err:
            if getattr(self, "_polling_teardown_started", False):
                return False
            if require_progress:
                raise
            if self._looks_like_polling_conflict(err):
                logger.warning(
                    "[%s] Telegram polling bootstrap conflict; gateway stays alive "
                    "while conflict retry runs: %s",
                    self.name, _redact_telegram_error_text(err),
                )
                loop = asyncio.get_running_loop()
                self._polling_error_task = loop.create_task(self._handle_polling_conflict(err))
                self._background_tasks.add(self._polling_error_task)
                self._polling_error_task.add_done_callback(self._background_tasks.discard)
                return False
            if self._looks_like_network_error(err):
                self._schedule_polling_recovery(err, reason="polling bootstrap")
                return False
            raise

    async def _handle_polling_network_error(self, error: Exception) -> None:
        """Reconnect polling after a transient network interruption.

        Triggered by NetworkError/TimedOut in the polling error callback, which
        happen when the host loses connectivity (Mac sleep, WiFi switch, VPN
        reconnect, etc.).  The gateway process stays alive but the long-poll
        connection silently dies; without this handler the bot never recovers.

        Strategy: exponential back-off (5s, 10s, 20s, 40s, 60s cap) up to
        MAX_NETWORK_RETRIES attempts, then mark the adapter retryable-fatal so
        the supervisor restarts the gateway process.
        """
        if getattr(self, "_polling_teardown_started", False):
            return
        if self.has_fatal_error:
            return

        MAX_NETWORK_RETRIES = 10
        BASE_DELAY = 5
        MAX_DELAY = 60

        self._polling_network_error_count += 1
        self._send_path_degraded = True
        attempt = self._polling_network_error_count

        if attempt > MAX_NETWORK_RETRIES:
            message = (
                "Telegram polling could not reconnect after %d network error retries. "
                "Escalating to gateway recovery." % MAX_NETWORK_RETRIES
            )
            logger.error("[%s] %s Last error: %s", self.name, message, _redact_telegram_error_text(error))
            self._set_fatal_error("telegram_network_error", message, retryable=True)
            await self._handoff_polling_fatal_error()
            return

        delay = min(BASE_DELAY * (2 ** (attempt - 1)), MAX_DELAY)
        safe_error = _redact_telegram_error_text(error)
        logger.warning(
            "[%s] Telegram network error (attempt %d/%d), reconnecting in %ds. Error: %s",
            self.name, attempt, MAX_NETWORK_RETRIES, delay, safe_error,
        )
        await asyncio.sleep(delay)

        if getattr(self, "_polling_teardown_started", False):
            return

        # Capture a stable local reference: self._app can be reassigned to None
        # by a concurrent disconnect() while we're suspended across the awaits
        # below, and re-reading self._app after that point would silently swap
        # in None mid-sequence instead of failing fast in one place.
        app = self._app

        try:
            if app and app.updater and app.updater.running:
                try:
                    # Guard stop() with a timeout: when the underlying TCP
                    # connection is in CLOSE-WAIT the PTB polling task is
                    # blocked on epoll on the dead socket and never wakes up,
                    # so an unguarded stop() hangs indefinitely.  The result
                    # is that _polling_error_task stays alive-but-blocked
                    # forever, every subsequent heartbeat probe sees it as
                    # "in-flight" and skips triggering a new reconnect, and
                    # the gateway silently drops messages for hours.
                    # Bounding stop() lets the reconnect ladder always advance.
                    # Refs: NousResearch/hermes-agent#58270
                    await asyncio.wait_for(app.updater.stop(), timeout=_UPDATER_STOP_TIMEOUT)
                except asyncio.TimeoutError:
                    logger.warning(
                        "[%s] updater.stop() timed out during network-error "
                        "reconnect (likely CLOSE-WAIT socket); forcing drain "
                        "and restart without clean stop",
                        self.name,
                    )
        except Exception:
            pass

        if getattr(self, "_polling_teardown_started", False):
            return
        await self._drain_polling_connections()

        if getattr(self, "_polling_teardown_started", False):
            return

        try:
            if not app:
                raise RuntimeError("Telegram application was torn down during reconnect")
            await self._start_polling_once(
                app,
                drop_pending_updates=False,
                error_callback=self._polling_error_callback_ref,
            )
            logger.info(
                "[%s] Telegram polling restarted after network error (attempt %d); "
                "health pending getUpdates progress",
                self.name, attempt,
            )
        except _PollingLifecycleAbort:
            return
        except Exception as retry_err:
            if getattr(self, "_polling_teardown_started", False):
                return
            safe_retry_error = _redact_telegram_error_text(retry_err)
            logger.warning("[%s] Telegram polling reconnect failed: %s", self.name, safe_retry_error)
            # start_polling failed — polling is dead and no further error
            # callbacks will fire, so schedule the next retry ourselves.
            if (
                not self.has_fatal_error
                and not getattr(self, "_polling_teardown_started", False)
            ):
                task = asyncio.ensure_future(
                    self._handle_polling_network_error(retry_err)
                )
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
                # This chained retry IS the in-flight recovery attempt — it
                # must replace the reentrancy guard, otherwise the heartbeat
                # loop, the pending-updates probe, and the PTB error callback
                # all see _polling_error_task as "done" and can each start a
                # second, concurrent recovery for the same outage.
                self._polling_error_task = task

    async def _polling_heartbeat_loop(self) -> None:
        """Detect dead Telegram TCP sockets (CLOSE-WAIT) by periodic probing.

        PTB's long-poll task blocks on epoll waiting for Telegram to push an
        update.  When the underlying TCP connection enters CLOSE-WAIT (the remote
        sent a FIN but the httpx pool has not yet noticed), epoll still reports
        the socket as readable and no exception is raised — so PTB's
        ``error_callback`` never fires and the gateway silently stops receiving
        messages.

        This loop probes ``get_me()`` every ``HEARTBEAT_INTERVAL`` seconds on the
        *general* request path (not the getUpdates pool), so a healthy long-poll
        waiting for the 30-second Telegram window is never interrupted.  On any
        connect-level failure the loop hands off to
        ``_handle_polling_network_error`` — the same path triggered by PTB's own
        ``error_callback`` — which drains the dead pool and restarts polling.

        Unlike the generation verifier (a one-shot progress deadline after
        every polling start), this loop runs for the full lifetime of the
        polling connection, so it catches a socket that wedges later during
        steady-state operation without any prior error event.
        """
        HEARTBEAT_INTERVAL = 90   # seconds between probes
        PROBE_TIMEOUT = 15        # seconds before declaring the path dead

        # Wedged-recovery watchdog state (#66377). Tracked locally so no
        # _polling_error_task assignment site needs to stamp a timestamp: the
        # heartbeat notes when it first observes a given recovery task still
        # in-flight, and force-escalates if the *same* task object is still
        # running after _POLLING_ERROR_TASK_STUCK_TIMEOUT. A healthy ladder
        # attempt completes (task done) or chains to a new task well before
        # then, so a single long-lived task is unambiguously wedged.
        stuck_task_ref: Optional[asyncio.Task] = None
        stuck_task_since = 0.0

        while True:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                if getattr(self, "_polling_teardown_started", False):
                    return
                if self.has_fatal_error:
                    return

                # Independent wedged-recovery watchdog (#66377): if the tracked
                # recovery task has hung (any await no local bound covers), every
                # other recovery path is gated behind it and returns early
                # forever — the gateway stays alive but deaf. Force a
                # retryable-fatal so the background reconnector rebuilds the
                # adapter instead of relying on the frozen ladder.
                recovery_task = self._polling_error_task
                if recovery_task is not None and not recovery_task.done():
                    now = time.monotonic()
                    if recovery_task is not stuck_task_ref:
                        stuck_task_ref = recovery_task
                        stuck_task_since = now
                    elif now - stuck_task_since > _POLLING_ERROR_TASK_STUCK_TIMEOUT:
                        stuck_for = now - stuck_task_since
                        logger.error(
                            "[%s] Telegram reconnect task wedged for %.0fs with no "
                            "ladder progress; forcing retryable-fatal so the gateway "
                            "reconnects instead of staying silently deaf.",
                            self.name, stuck_for,
                        )
                        try:
                            recovery_task.cancel()
                        except Exception:
                            pass
                        self._set_fatal_error(
                            "telegram_network_error",
                            "Telegram reconnect task wedged for %.0fs; forcing "
                            "gateway reconnect." % stuck_for,
                            retryable=True,
                        )
                        await self._handoff_polling_fatal_error()
                        return
                else:
                    stuck_task_ref = None

                bot = self._app.bot if self._app else None
                if bot is None:
                    continue
                # A real PTB Bot always exposes get_me(); if it's absent the
                # app isn't a live polling client (e.g. torn down or a test
                # double), so there is nothing to probe — exit rather than spin.
                if not callable(getattr(bot, "get_me", None)):
                    return
                await asyncio.wait_for(bot.get_me(), PROBE_TIMEOUT)
                # get_me() refreshes PTB's cached bot user in place, so this is
                # also where a BotFather rename gets picked up: adopt whatever
                # handle Telegram just reported before anything routes on it.
                self._bot_identity_checked_at = time.monotonic()
                self._note_bot_username(getattr(bot, "username", None))
                # get_me() succeeded — the general/send request path is healthy.
                # That does NOT prove the getUpdates consumer is alive: PTB can
                # report updater.running=True while the long-poll task is wedged,
                # so DMs queue in the Bot API and never reach handlers (#42909).
                # get_me() is blind to this; get_webhook_info() exposes it via
                # pending_update_count. Escalate only after two consecutive
                # probes see a non-zero queue while we believe we're polling, so
                # a single in-flight update (consumed before the next probe)
                # never trips recovery.
                await self._probe_pending_updates(bot, PROBE_TIMEOUT)
            except asyncio.CancelledError:
                return
            except (asyncio.TimeoutError, OSError) as probe_err:
                self._schedule_polling_recovery(probe_err, reason="heartbeat probe")
            except Exception as probe_err:
                if self._looks_like_network_error(probe_err):
                    self._schedule_polling_recovery(probe_err, reason="heartbeat probe")
                    continue
                # Non-connectivity errors (e.g. TelegramError 401) are not
                # CLOSE-WAIT symptoms — let PTB's own handlers surface them.
                pass

    async def _probe_pending_updates(self, bot, probe_timeout: float) -> None:
        """Detect a wedged getUpdates consumer via pending_update_count.

        PTB can report ``updater.running == True`` while its long-poll task is
        silently stuck (e.g. a socket that epoll keeps reporting readable on
        WSL2). ``get_me()`` stays healthy because it uses the general request
        path, so the CLOSE-WAIT heartbeat never fires — yet DMs queue in the
        Bot API and never reach handlers (#42909).

        ``get_webhook_info().pending_update_count`` is the one signal that
        exposes this: a growing/stuck queue while we believe we're polling means
        the consumer is dead. We only escalate after two consecutive stuck
        probes so a single update that's simply in-flight between probes does
        not trip a needless recovery. Recovery reuses
        ``_handle_polling_network_error`` — the same ladder PTB's own
        ``error_callback`` feeds — so no new restart machinery is introduced.

        This also covers the harsher case where the updater has stopped
        entirely (``running=False``) with no reconnect in flight: the long-poll
        task is gone rather than wedged, so even ``get_webhook_info`` can't
        report a queue against a live consumer. We detect the stopped updater
        directly and feed the same ladder (#55769).
        """
        if getattr(self, "_polling_teardown_started", False):
            return
        # Only meaningful in polling mode; in webhook mode Telegram pushes
        # updates and holds no server-side queue.
        if self._webhook_mode:
            return
        # A reconnect already in flight owns recovery — don't double-trigger,
        # and don't misread its brief stop()->start_polling() window (where
        # updater.running is transiently False) as a dead updater below.
        if self._polling_error_task and not self._polling_error_task.done():
            self._polling_not_running_count = 0
            return
        updater = getattr(self._app, "updater", None) if self._app else None
        if updater is None:
            self._polling_pending_stuck_count = 0
            return
        if not getattr(updater, "running", False):
            # We are in polling mode with no reconnect in flight, yet PTB's
            # updater has stopped entirely. This is distinct from the
            # wedged-but-running consumer handled below: the long-poll task is
            # gone, get_me()/get_webhook_info() on the general request path
            # still succeed, so no error_callback or connectivity probe ever
            # fires and the gateway silently stops receiving messages while the
            # process stays alive (#55769). Escalate through the same reconnect
            # ladder as a wedged consumer, debounced over two consecutive probes
            # so a just-starting updater never trips it.
            self._polling_pending_stuck_count = 0
            self._polling_not_running_count += 1
            logger.warning(
                "[%s] Telegram polling heartbeat: updater stopped while in "
                "polling mode (stuck probe %d/2)",
                self.name, self._polling_not_running_count,
            )
            if self._polling_not_running_count >= 2:
                self._polling_not_running_count = 0
                if getattr(self, "_polling_teardown_started", False):
                    return
                logger.warning(
                    "[%s] Telegram updater is not running (long-poll task "
                    "gone); triggering polling restart",
                    self.name,
                )
                loop = asyncio.get_running_loop()
                self._polling_error_task = loop.create_task(
                    self._handle_polling_network_error(
                        RuntimeError("Telegram updater stopped while in polling mode")
                    )
                )
            return
        self._polling_not_running_count = 0
        get_webhook_info = getattr(bot, "get_webhook_info", None)
        if not callable(get_webhook_info):
            return
        try:
            info = await asyncio.wait_for(get_webhook_info(), probe_timeout)  # type: ignore[arg-type]
        except (asyncio.TimeoutError, OSError):
            # A failed probe is a connectivity symptom the get_me() path or the
            # outer handler will catch; don't treat it as a stuck-queue signal.
            return
        pending = int(getattr(info, "pending_update_count", 0) or 0)
        if pending <= 0:
            self._polling_pending_stuck_count = 0
            return
        self._polling_pending_stuck_count += 1
        logger.warning(
            "[%s] Telegram polling heartbeat: %d update(s) queued but not "
            "consumed (stuck probe %d/2)",
            self.name, pending, self._polling_pending_stuck_count,
        )
        if self._polling_pending_stuck_count >= 2:
            self._polling_pending_stuck_count = 0
            if getattr(self, "_polling_teardown_started", False):
                return
            logger.warning(
                "[%s] getUpdates consumer appears wedged (queue not draining); "
                "triggering polling restart",
                self.name,
            )
            loop = asyncio.get_running_loop()
            self._polling_error_task = loop.create_task(
                self._handle_polling_network_error(
                    RuntimeError("getUpdates consumer wedged: pending updates not draining")
                )
            )

    async def _verify_polling_after_reconnect(
        self,
        generation: Optional[int] = None,
        progress: Optional[asyncio.Event] = None,
    ) -> None:
        """Require getUpdates progress, using getMe only to classify failure.

        The generation-bound event is set only by a successful response on the
        dedicated getUpdates request. A general-path getMe success can classify
        connectivity, but cannot heal polling health. Connectivity failures
        enter the guarded recovery ladder; auth/validation errors do not churn.
        """
        PROBE_TIMEOUT = 10
        if getattr(self, "_polling_teardown_started", False):
            return
        if generation is None:
            generation = self._polling_generation
        if progress is None:
            progress = self._polling_progress_event

        try:
            await asyncio.wait_for(
                progress.wait(), timeout=_POLLING_PROGRESS_TIMEOUT
            )
        except asyncio.TimeoutError:
            pass

        if getattr(self, "_polling_teardown_started", False):
            return
        if progress.is_set() or self.has_fatal_error:
            return
        if not self._polling_progress_accepting:
            return
        if generation != self._polling_generation:
            return
        if progress is not self._polling_progress_event:
            return

        app = self._app
        if not (app and app.updater and app.updater.running):
            logger.warning(
                "[%s] Updater made no getUpdates progress and is not running",
                self.name,
            )
            self._schedule_polling_recovery(
                RuntimeError("Updater not running after polling progress deadline"),
                reason="polling progress verifier: updater not running",
            )
            return

        try:
            await asyncio.wait_for(app.bot.get_me(), PROBE_TIMEOUT)
        except Exception as probe_err:
            if getattr(self, "_polling_teardown_started", False):
                return
            if self.has_fatal_error or not self._polling_progress_accepting:
                return
            if generation != self._polling_generation:
                return
            if progress is not self._polling_progress_event or progress.is_set():
                return
            if not self._looks_like_network_error(probe_err):
                logger.warning(
                    "[%s] Polling progress verifier hit a non-connectivity error"
                    " (not retrying): %s",
                    self.name, _redact_telegram_error_text(probe_err),
                )
                return
            logger.warning(
                "[%s] Polling progress verifier connectivity probe failed: %s",
                self.name, _redact_telegram_error_text(probe_err),
            )
            self._schedule_polling_recovery(
                probe_err,
                reason="polling progress verifier connectivity failure",
            )
            return

        if getattr(self, "_polling_teardown_started", False):
            return
        if self.has_fatal_error or not self._polling_progress_accepting:
            return
        if generation != self._polling_generation:
            return
        if progress is not self._polling_progress_event or progress.is_set():
            return
        self._schedule_polling_recovery(
            RuntimeError("getUpdates made no progress before verifier deadline"),
            reason="polling progress verifier: general path healthy but getUpdates stalled",
        )

    def _disarm_ptb_retry_loop(self) -> None:
        """Synchronously stop PTB's internal polling retry loop.

        PTB wraps ``getUpdates`` in ``network_retry_loop`` with
        ``max_retries=-1`` (retry forever).  When a ``TelegramError`` (including
        a 409 ``Conflict``) fires, that loop calls our ``error_callback``
        *synchronously*, then sleeps and re-checks ``while is_running()`` before
        polling again.  Our ``error_callback`` only schedules an async recovery
        task (``loop.create_task(...)``) and returns immediately, so PTB's loop
        keeps polling while our handler concurrently runs
        ``stop -> sleep -> start_polling``.  The two polling sessions overlap and
        Telegram returns a fresh 409 — a self-inflicted conflict loop on a
        ~31s cadence.

        The loop is wired with ``is_running=lambda: updater.running`` and a
        private ``stop_event`` (``do_action`` races that event and returns the
        moment it is set).  Setting that event *synchronously inside the
        callback* — before it returns — makes PTB's loop exit on its own next
        tick instead of racing our recovery.  Our async handler then performs
        the real ``await updater.stop()`` (idempotent) followed by
        drain + ``start_polling()``, which builds a fresh ``stop_event`` so the
        restart is not poisoned.

        Best-effort and defensive: PTB names the attribute differently across
        versions (``_Updater__polling_task_stop_event`` via name-mangling), so
        we probe for both spellings.  If neither is found we do nothing and
        fall back to the prior behaviour (async ``updater.stop()`` racing PTB) —
        i.e. we never make things worse than before.

        We deliberately do NOT fall back to flipping ``updater._running``:
        ``stop()`` raises ``RuntimeError`` when ``running`` is already False and
        our recovery handler guards its ``stop()`` call on ``running``, so
        clearing the flag here would skip the real teardown and leave PTB's
        stop_event uncleared — poisoning the subsequent ``start_polling()``.
        The stop_event lever leaves ``_running`` True, so the handler's
        ``await updater.stop()`` still runs, drains the polling task, and clears
        the event for a clean restart.
        """
        updater = getattr(self._app, "updater", None) if self._app else None
        if updater is None:
            return
        # Preferred (and only) lever: PTB's polling stop_event. Name-mangled on
        # Updater, so probe both the mangled and unmangled spellings.
        for attr in (
            "_Updater__polling_task_stop_event",
            "_polling_task_stop_event",
        ):
            stop_event = getattr(updater, attr, None)
            if isinstance(stop_event, asyncio.Event):
                if not stop_event.is_set():
                    stop_event.set()
                    logger.debug(
                        "[%s] Disarmed PTB polling retry loop via %s",
                        self.name, attr,
                    )
                return
        logger.debug(
            "[%s] Could not disarm PTB polling retry loop "
            "(stop_event not found on this PTB version); "
            "falling back to async stop()",
            self.name,
        )

    async def _handle_polling_conflict(self, error: Exception) -> None:
        if getattr(self, "_polling_teardown_started", False):
            return
        if self.has_fatal_error and self.fatal_error_code == "telegram_polling_conflict":
            return
        # Transient 409 Conflict errors arise when the previous gateway process
        # has been killed (e.g. during `hermes update` or `--replace` handoffs)
        # but its long-poll connection hasn't yet expired on Telegram's servers.
        # Telegram holds open getUpdates sessions for up to ~30s after the
        # client disconnects, so a new gateway starting immediately will receive
        # a 409 until that server-side session expires.
        #
        # Strategy: stop the local updater, wait long enough for Telegram's
        # server-side session to expire (RETRY_DELAY grows with each attempt),
        # drain the connection pool, then restart polling.  We attempt this
        # MAX_CONFLICT_RETRIES times before declaring a fatal error.
        #
        # Crucially, a failed retry must NOT leave polling in an ambiguous
        # state.  If start_polling() raises, the updater is neither running
        # nor fatal — messages are silently dropped.  We schedule another
        # retry attempt instead of returning silently, and only escalate to
        # fatal after all retries are exhausted.
        self._polling_conflict_count += 1

        MAX_CONFLICT_RETRIES = 5
        # Delay grows with each attempt: 15s, 25s, 35s, 45s, 55s.
        # Telegram server-side getUpdates sessions typically expire within
        # 30s; the increasing back-off ensures we clear that window without
        # hammering the API on fast-restart loops.
        RETRY_DELAY = 10 + (self._polling_conflict_count * 10)  # seconds

        if self._polling_conflict_count <= MAX_CONFLICT_RETRIES:
            logger.warning(
                "[%s] Telegram polling conflict (%d/%d) — previous session still "
                "held open on Telegram's servers. Waiting %ds for it to expire. "
                "Error: %s",
                self.name, self._polling_conflict_count, MAX_CONFLICT_RETRIES,
                RETRY_DELAY, _redact_telegram_error_text(error),
            )
            # Stop the local updater cleanly before sleeping.  If it's already
            # stopped (e.g. PTB raised before updater.running was set) this is
            # a no-op.  Bounded with a timeout for the same reason as the
            # network-error path: a CLOSE-WAIT socket can wedge stop() on epoll
            # forever, which would stall the conflict-retry ladder.
            try:
                if self._app and self._app.updater and self._app.updater.running:
                    try:
                        await asyncio.wait_for(self._app.updater.stop(), timeout=_UPDATER_STOP_TIMEOUT)
                    except asyncio.TimeoutError:
                        logger.warning(
                            "[%s] updater.stop() timed out during conflict "
                            "retry (likely CLOSE-WAIT socket); continuing",
                            self.name,
                        )
            except Exception:
                pass

            await asyncio.sleep(RETRY_DELAY)
            if getattr(self, "_polling_teardown_started", False):
                return
            await self._drain_polling_connections()
            if getattr(self, "_polling_teardown_started", False):
                return

            # Capture a stable local reference: self._app can be reassigned to
            # None by a concurrent disconnect() while we're suspended across
            # the awaits above (same race #55992 fixed on the network path).
            # Re-reading self._app after that point would raise
            # AttributeError deep inside start_polling instead of failing fast
            # here, where the except below reschedules or escalates to fatal.
            app = self._app
            try:
                if not app:
                    raise RuntimeError("Telegram application was torn down during conflict reconnect")
                await self._start_polling_once(
                    app,
                    drop_pending_updates=False,
                    error_callback=self._polling_error_callback_ref,
                )
                logger.info(
                    "[%s] Telegram polling restarted after conflict retry %d/%d; "
                    "health pending getUpdates progress",
                    self.name, self._polling_conflict_count, MAX_CONFLICT_RETRIES,
                )
                return
            except _PollingLifecycleAbort:
                return
            except Exception as retry_err:
                if getattr(self, "_polling_teardown_started", False):
                    return
                logger.warning(
                    "[%s] Telegram polling retry %d/%d failed: %s. "
                    "Scheduling next attempt.",
                    self.name, self._polling_conflict_count, MAX_CONFLICT_RETRIES,
                    _redact_telegram_error_text(retry_err),
                )
                # Schedule the next retry rather than returning silently.
                # Returning here without either restarting polling or setting
                # a fatal error leaves the adapter in a limbo state: the
                # gateway process is alive and reports "connected" but
                # no messages are received or sent.
                if (
                    self._polling_conflict_count < MAX_CONFLICT_RETRIES
                    and not getattr(self, "_polling_teardown_started", False)
                ):
                    # We are inside a running coroutine, so the running loop is
                    # guaranteed to exist. asyncio.get_event_loop() is deprecated
                    # and raises "RuntimeError: There is no current event loop in
                    # thread 'MainThread'" on Python 3.10+ when invoked from a
                    # context without an attached loop (which can happen when PTB
                    # dispatches this error callback). Use get_running_loop().
                    loop = asyncio.get_running_loop()
                    self._polling_error_task = loop.create_task(
                        self._handle_polling_conflict(retry_err)
                    )
                    return
                # Fall through to fatal on the last retry.

        if getattr(self, "_polling_teardown_started", False):
            return

        # Exhausted all retries — declare a fatal error so the gateway
        # runner can surface this clearly and the user knows to act.
        message = (
            "Telegram polling could not recover after %d retries (%ds total wait). "
            "The previous gateway session is still held open on Telegram's servers, "
            "or another process is using the same bot token. "
            "To recover: ensure no other Hermes or OpenClaw instance is running "
            "with this token, then restart the gateway with 'hermes gateway restart'."
            % (MAX_CONFLICT_RETRIES, sum(10 + i * 10 for i in range(1, MAX_CONFLICT_RETRIES + 1)))
        )
        logger.error(
            "[%s] %s Original error: %s",
            self.name, message, _redact_telegram_error_text(error),
        )
        # Snapshot whether we are the call that actually transitions to fatal.
        # A concurrent retry task scheduled by an earlier conflict may already
        # be suspended past the entry guard; once _set_fatal_error flips the
        # flag, adding an await below (the bounded stop()) yields the loop and
        # lets that task reach this branch too — double-notifying the fatal
        # handler.  Only the first transition notifies.
        _already_fatal = (
            self.has_fatal_error
            and self.fatal_error_code == "telegram_polling_conflict"
        )
        self._set_fatal_error("telegram_polling_conflict", message, retryable=False)
        try:
            if self._app and self._app.updater:
                await asyncio.wait_for(self._app.updater.stop(), timeout=_UPDATER_STOP_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning(
                "[%s] updater.stop() timed out after exhausting conflict "
                "retries (likely CLOSE-WAIT socket); proceeding to fatal notify",
                self.name,
            )
        except Exception as stop_error:
            logger.warning(
                "[%s] Failed stopping Telegram updater after exhausting conflict retries: %s",
                self.name, stop_error, exc_info=True,
            )
        if not _already_fatal:
            await self._handoff_polling_fatal_error()

    async def _handoff_polling_fatal_error(self) -> None:
        """Notify the runner without letting child teardown cancel this owner.

        The runner bounds adapter cleanup in a child task.  ``disconnect()``
        cancels the tracked polling-recovery task and the heartbeat task, so
        retaining the current notifier in either field would cancel the fatal
        callback before the runner can finish its reconnect or shutdown
        decision.  Release only the current owner from whichever field tracks
        it; unrelated tasks remain under teardown control.
        """
        current_task = asyncio.current_task()
        if self._polling_error_task is current_task:
            self._polling_error_task = None
        if getattr(self, "_polling_heartbeat_task", None) is current_task:
            self._polling_heartbeat_task = None
        await self._notify_fatal_error()

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Connect to Telegram via polling or webhook.

        By default, uses long polling (outbound connection to Telegram).
        If ``TELEGRAM_WEBHOOK_URL`` is set, starts an HTTP webhook server
        instead.  Webhook mode is useful for cloud deployments (Fly.io,
        Railway) where inbound HTTP can wake a suspended machine.

        ``is_reconnect`` distinguishes a cold first boot (False — drop any
        stale Bot API queue) from a watcher reconnect after a prolonged
        outage (True — preserve the updates Telegram queued while the bot
        was offline, otherwise every message sent during the outage is
        silently lost). The in-process network-error ladder and the
        409-conflict handler already pass ``drop_pending_updates=False``
        for the same reason; bootstrap follows suit on the reconnect path.

        Env vars for webhook mode::

            TELEGRAM_WEBHOOK_URL    Public HTTPS URL (e.g. https://app.fly.dev/telegram)
            TELEGRAM_WEBHOOK_PORT   Local listen port (default 8443)
            TELEGRAM_WEBHOOK_HOST   Bind host (default: unset → dual-stack,
                                    all interfaces IPv4+IPv6)
            TELEGRAM_WEBHOOK_SECRET Secret token for update verification
        """
        # Explicit connect() is the only operation allowed to reopen polling
        # after a completed, serialized teardown. Background recovery never
        # clears this fence.
        self._polling_teardown_started = False
        # Mode selection is re-evaluated on every explicit connection. Keep
        # webhook state false unless this connection starts its webhook.
        self._webhook_mode = False

        if not TELEGRAM_AVAILABLE:
            logger.error(
                "[%s] python-telegram-bot not installed. Run: pip install python-telegram-bot",
                self.name,
            )
            self._set_fatal_error("missing_dependency", "python-telegram-bot not installed", retryable=False)
            return False

        if not self.config.token:
            logger.error("[%s] No bot token configured", self.name)
            self._set_fatal_error("missing_credentials", "No bot token configured", retryable=False)
            return False

        try:
            if not self._acquire_platform_lock('telegram-bot-token', self.config.token, 'Telegram bot token'):
                return False

            # Build the application
            builder = Application.builder().token(self.config.token)
            custom_base_url = self.config.extra.get("base_url")
            if custom_base_url:
                builder = builder.base_url(custom_base_url)
                builder = builder.base_file_url(
                    self.config.extra.get("base_file_url", custom_base_url)
                )
                logger.info(
                    "[%s] Using custom Telegram base_url: %s",
                    self.name, custom_base_url,
                )
            # In local-mode telegram-bot-api, file_path is an absolute path on the
            # server's filesystem rather than a relative HTTP path. PTB needs
            # local_mode=True so download_*() reads from disk instead of issuing
            # an HTTP GET that would 404. Requires that the same path is
            # readable by the Hermes process (shared mount, same machine, etc.).
            if self.config.extra.get("local_mode"):
                builder = builder.local_mode(True)
                logger.info("[%s] Using Telegram local_mode (read files from disk)", self.name)

            # PTB defaults (pool_timeout=1s) are too aggressive on flaky networks and
            # can trigger "Pool timeout: All connections in the connection pool are occupied"
            # during reconnect/bootstrap. Use safer defaults and allow env overrides.
            def _env_int(name: str, default: int) -> int:
                try:
                    return int(os.getenv(name, str(default)))
                except (TypeError, ValueError):
                    return default

            def _env_float(name: str, default: float) -> float:
                try:
                    return float(os.getenv(name, str(default)))
                except (TypeError, ValueError):
                    return default

            request_kwargs = {
                "connection_pool_size": _env_int("HERMES_TELEGRAM_HTTP_POOL_SIZE", 512),
                "pool_timeout": _env_float("HERMES_TELEGRAM_HTTP_POOL_TIMEOUT", 8.0),
                "connect_timeout": _env_float("HERMES_TELEGRAM_HTTP_CONNECT_TIMEOUT", 10.0),
                "read_timeout": _env_float("HERMES_TELEGRAM_HTTP_READ_TIMEOUT", 20.0),
                "write_timeout": _env_float("HERMES_TELEGRAM_HTTP_WRITE_TIMEOUT", 20.0),
                # Not a duplicate of write_timeout: PTB routes any request
                # carrying files to media_write_timeout instead, so the line
                # above never applied to an upload and every upload was pinned
                # to PTB's own 20s default. httpx budgets this per socket
                # write rather than across the upload, so it is stall
                # tolerance, not a size or bandwidth allowance — a slow but
                # steady uplink never accumulates against it. 60s rides out
                # the buffer stalls a congested link produces; going higher
                # only lengthens how long a dead socket takes to report
                # itself.
                "media_write_timeout": 60.0,
            }

            # CLOSE_WAIT fd leak (#31599, same class as #18451): PTB's
            # HTTPXRequest builds the underlying httpx.AsyncClient with
            # `limits = httpx.Limits(max_connections=connection_pool_size)`
            # and *no* keepalive tuning, so httpx's default
            # keepalive_expiry=5.0 applies. Behind an HTTP proxy (Cloudflare
            # Warp etc.) a peer-initiated FIN can sit in CLOSE_WAIT longer
            # than that, leaking fds in the general request pool (_request[1])
            # which _drain_polling_connections never resets. Wire the shared
            # platform_httpx_limits() helper into the httpx client so idle
            # keepalive sockets drain aggressively, while preserving PTB's
            # max_connections (= connection_pool_size). httpx_kwargs is spread
            # last into PTB's client kwargs, so `limits` here wins.
            from gateway.platforms._http_client_limits import platform_httpx_limits

            _base_limits = platform_httpx_limits()
            if _base_limits is not None:
                import httpx as _httpx

                _pool_limits = _httpx.Limits(
                    max_connections=request_kwargs["connection_pool_size"],
                    max_keepalive_connections=_base_limits.max_keepalive_connections,
                    keepalive_expiry=_base_limits.keepalive_expiry,
                )
            else:  # pragma: no cover — httpx always present alongside PTB
                _pool_limits = None

            def _with_limits(httpx_kwargs: Optional[dict] = None) -> dict:
                """Merge tuned keepalive limits into httpx client kwargs.

                Used by the proxy and direct-DNS branches, where httpx honours
                the client-level ``limits`` kwarg. A caller-supplied ``limits``
                is left untouched; otherwise the CLOSE_WAIT-safe limits are
                injected. The fallback-IP branch does NOT use this helper — see
                the ``_transport_kwargs`` note below for why.
                """
                kwargs = dict(httpx_kwargs or {})
                if _pool_limits is not None and "limits" not in kwargs:
                    kwargs["limits"] = _pool_limits
                return kwargs

            disable_fallback = (os.getenv("HERMES_TELEGRAM_DISABLE_FALLBACK_IPS", "").strip().lower() in {"1", "true", "yes", "on"})
            fallback_ips = self._fallback_ips()
            if not fallback_ips:
                logger.warning("[%s] Discovering Telegram API fallback IPs via DNS-over-HTTPS…", self.name)
                fallback_ips = await discover_fallback_ips()
                logger.info(
                    "[%s] Auto-discovered Telegram fallback IPs: %s",
                    self.name,
                    ", ".join(fallback_ips),
                )

            proxy_targets = ["api.telegram.org", *fallback_ips]
            proxy_url = resolve_proxy_url("TELEGRAM_PROXY", target_hosts=proxy_targets)
            if fallback_ips and not proxy_url and not disable_fallback:
                logger.info(
                    "[%s] Telegram fallback IPs active: %s",
                    self.name,
                    ", ".join(fallback_ips),
                )
                # Keep request/update pools separate to reduce contention during
                # polling reconnect + bot API bootstrap/delete_webhook calls.
                # httpx ignores the client-level `limits` kwarg when a custom
                # `transport` is supplied (#58790).  Unlike the proxy/direct
                # branches (which inject limits at the client level via
                # `_with_limits`), this branch MUST pass the tuned limits
                # directly into TelegramFallbackTransport so its inner
                # AsyncHTTPTransport instances honour keepalive_expiry — do not
                # route this through `_with_limits`, httpx would discard it.
                _transport_kwargs: dict = {}
                if _pool_limits is not None:
                    _transport_kwargs["limits"] = _pool_limits
                request = HTTPXRequest(
                    **request_kwargs,
                    httpx_kwargs={
                        "transport": TelegramFallbackTransport(
                            fallback_ips, **_transport_kwargs
                        )
                    },
                )
                get_updates_request = HTTPXRequest(
                    **request_kwargs,
                    httpx_kwargs={
                        "transport": TelegramFallbackTransport(
                            fallback_ips, **_transport_kwargs
                        )
                    },
                )
            elif proxy_url:
                logger.info("[%s] Proxy detected; passing explicitly to HTTPXRequest: %s", self.name, proxy_url)
                request = HTTPXRequest(
                    **request_kwargs, proxy=proxy_url, httpx_kwargs=_with_limits()
                )
                get_updates_request = HTTPXRequest(
                    **request_kwargs, proxy=proxy_url, httpx_kwargs=_with_limits()
                )
            else:
                if disable_fallback:
                    logger.info("[%s] Telegram fallback-IP transport disabled via env", self.name)
                request = HTTPXRequest(**request_kwargs, httpx_kwargs=_with_limits())
                get_updates_request = HTTPXRequest(
                    **request_kwargs, httpx_kwargs=_with_limits()
                )

            get_updates_request = self._instrument_polling_request(get_updates_request)
            builder = builder.request(request).get_updates_request(get_updates_request)
            self._app = builder.build()
            self._bot = self._app.bot

            # Register handlers
            self._app.add_handler(TelegramMessageHandler(
                filters.TEXT & ~filters.COMMAND,
                self._handle_text_message
            ))
            self._app.add_handler(TelegramMessageHandler(
                filters.COMMAND,
                self._handle_command
            ))
            self._app.add_handler(TelegramMessageHandler(
                filters.LOCATION | getattr(filters, "VENUE", filters.LOCATION),
                self._handle_location_message
            ))
            self._app.add_handler(TelegramMessageHandler(
                filters.PHOTO | filters.VIDEO | filters.AUDIO | filters.VOICE | filters.Document.ALL | filters.Sticker.ALL,
                self._handle_media_message
            ))
            # Handle inline keyboard button callbacks (update prompts)
            self._app.add_handler(CallbackQueryHandler(self._handle_callback_query))

            # Start polling — retry initialize() for transient TLS resets.
            # Each attempt is capped by _init_timeout so a single unreachable
            # fallback-IP chain can't block startup indefinitely.
            _max_connect = 8
            _init_timeout = _env_float("HERMES_TELEGRAM_INIT_TIMEOUT", 30.0)
            # Total watchdog: ensure the entire connect loop has an upper bound
            # even if the retry loop itself silently stalls (#67498). This is
            # the per-attempt timeout PLUS generous margins between attempts so
            # we never hang past the sum even when all attempts are exhausted.
            _total_deadline = (
                asyncio.get_running_loop().time()
                + _init_timeout * _max_connect
                + 120.0  # extra margin for between-attempt sleeps + overhead
            )
            for _attempt in range(_max_connect):
                rebuild_app = False
                try:
                    # Check total watchdog deadline — if we blew past it the
                    # retry ladder must yield even if no individual attempt
                    # has raised.
                    if asyncio.get_running_loop().time() >= _total_deadline:
                        raise OSError(
                            f"Telegram initialization timed out after {_max_connect} attempts "
                            f"({_init_timeout:.0f}s each) — total connect watchdog "
                            f"deadline ({_init_timeout * _max_connect + 120.0:.0f}s) exceeded. "
                            f"Check network connectivity to api.telegram.org "
                            f"or set HERMES_TELEGRAM_HTTP_CONNECT_TIMEOUT / "
                            f"HERMES_TELEGRAM_INIT_TIMEOUT to a lower value."
                        )
                    logger.warning(
                        "[%s] Connecting to Telegram (attempt %d/%d)…",
                        self.name, _attempt + 1, _max_connect,
                    )
                    await _await_with_thread_deadline(
                        self._app.initialize(),
                        timeout=_init_timeout,
                        # On timeout the initialize() task is abandoned without
                        # awaiting its cancellation (it may be wedged in a
                        # shielded scope). Best-effort release the half-built
                        # app's httpx client/connection pool so it isn't leaked
                        # across the retry ladder (mirrors the client-close-on-
                        # timeout pattern in agent/auxiliary_client.py).
                        on_abandon=lambda app=self._app: _shutdown_abandoned_app(app),
                    )
                    break
                except asyncio.TimeoutError:
                    rebuild_app = True
                    if _attempt < _max_connect - 1:
                        wait = min(2 ** _attempt, 15)
                        logger.warning(
                            "[%s] Connect attempt %d/%d timed out after %.0fs — retrying in %ds",
                            self.name, _attempt + 1, _max_connect, _init_timeout, wait,
                        )
                        await asyncio.sleep(wait)
                    else:
                        raise OSError(
                            f"Telegram initialization timed out after {_max_connect} attempts "
                            f"({_init_timeout:.0f}s each). Check network connectivity to api.telegram.org "
                            f"or set HERMES_TELEGRAM_HTTP_CONNECT_TIMEOUT to a lower value."
                        )
                except OSError as init_err:
                    rebuild_app = True
                    if _attempt < _max_connect - 1:
                        wait = min(2 ** _attempt, 15)
                        logger.warning(
                            "[%s] Connect attempt %d/%d failed: %s — retrying in %ds",
                            self.name, _attempt + 1, _max_connect, init_err, wait,
                        )
                        await asyncio.sleep(wait)
                    else:
                        raise
                except Exception as init_err:
                    rebuild_app = True
                    if not self._looks_like_network_error(init_err):
                        raise
                    if _attempt < _max_connect - 1:
                        wait = min(2 ** _attempt, 15)
                        logger.warning(
                            "[%s] Connect attempt %d/%d failed: %s — retrying in %ds",
                            self.name, _attempt + 1, _max_connect, init_err, wait,
                        )
                        await asyncio.sleep(wait)
                    else:
                        raise
                except BaseException:
                    # Catch CancelledError and other BaseException subclasses
                    # that the existing except handlers miss. Log the event so
                    # the operator can diagnose, then reraise so cancellation
                    # semantics are preserved (#67498).
                    # NOTE: placed LAST so Exception handlers above have
                    # priority — BaseException catches everything including
                    # Exception.
                    logger.warning(
                        "[%s] Connect attempt %d/%d interrupted by %s — propagating",
                        self.name,
                        _attempt + 1,
                        _max_connect,
                        "CancelledError"
                        if isinstance(sys.exc_info()[1], asyncio.CancelledError)
                        else type(sys.exc_info()[1]).__name__,
                    )
                    raise
                finally:
                    # After a failed attempt the app may be in a partially-
                    # initialized state (closed transports, half-built handlers).
                    # Rebuild from the same token/config so the next attempt
                    # starts with a fresh Application — the old one is discarded
                    # and will be GC'd (#67498).
                    if rebuild_app and _attempt < _max_connect - 1:
                        old_app = self._app
                        self._app = builder.build()
                        self._bot = self._app.bot
                        # Re-register handlers on the new app
                        self._app.add_handler(TelegramMessageHandler(
                            filters.TEXT & ~filters.COMMAND,
                            self._handle_text_message
                        ))
                        self._app.add_handler(TelegramMessageHandler(
                            filters.COMMAND,
                            self._handle_command
                        ))
                        self._app.add_handler(TelegramMessageHandler(
                            filters.LOCATION | getattr(filters, "VENUE", filters.LOCATION),
                            self._handle_location_message
                        ))
                        self._app.add_handler(TelegramMessageHandler(
                            filters.PHOTO | filters.VIDEO | filters.AUDIO | filters.VOICE | filters.Document.ALL | filters.Sticker.ALL,
                            self._handle_media_message
                        ))
                        self._app.add_handler(CallbackQueryHandler(self._handle_callback_query))
                        # Best-effort discard the old app's resources
                        try:
                            await _shutdown_abandoned_app(old_app)
                        except Exception:
                            pass
            await self._app.start()

            # Decide between webhook and polling mode
            webhook_url = os.getenv("TELEGRAM_WEBHOOK_URL", "").strip()

            if webhook_url:
                # ── Webhook mode ─────────────────────────────────────
                # Telegram pushes updates to our HTTP endpoint.  This
                # enables cloud platforms (Fly.io, Railway) to auto-wake
                # suspended machines on inbound HTTP traffic.
                #
                # SECURITY: TELEGRAM_WEBHOOK_SECRET is REQUIRED. Without it,
                # python-telegram-bot passes secret_token=None and the
                # webhook endpoint accepts any HTTP POST — attackers can
                # inject forged updates as if from Telegram. Refuse to
                # start rather than silently run in fail-open mode.
                # See GHSA-3vpc-7q5r-276h.
                webhook_port = env_int("TELEGRAM_WEBHOOK_PORT", 8443)
                # Bind host. Default "" → tornado bind_sockets opens one
                # listening socket per address family (IPv4 + IPv6). The old
                # hardcoded "0.0.0.0" bound IPv4 ONLY and was unreachable
                # over IPv6-only private networks (e.g. Fly.io 6PN) — same
                # bug as the LINE adapter (NS-603). Pin via
                # TELEGRAM_WEBHOOK_HOST or platforms.telegram.extra.webhook_host.
                webhook_host = (
                    os.getenv("TELEGRAM_WEBHOOK_HOST", "").strip()
                    or str((self.config.extra or {}).get("webhook_host") or "").strip()
                )
                # Profile-scoped read (adapter startup, Slack pattern
                # #59739): a scoped read honors the profile's own secret;
                # only an UNSCOPED read under multiplex (default-profile
                # startup loop) falls back to the process env, which is that
                # profile's own value.
                from agent.secret_scope import (
                    UnscopedSecretError,
                    get_secret,
                )

                try:
                    webhook_secret = (get_secret("TELEGRAM_WEBHOOK_SECRET") or "").strip()
                except UnscopedSecretError:
                    webhook_secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
                if not webhook_secret:
                    raise RuntimeError(
                        "TELEGRAM_WEBHOOK_SECRET is required when "
                        "TELEGRAM_WEBHOOK_URL is set. Without it, the "
                        "webhook endpoint accepts forged updates from "
                        "anyone who can reach it — see "
                        "https://github.com/NousResearch/hermes-agent/"
                        "security/advisories/GHSA-3vpc-7q5r-276h.\n\n"
                        "Generate a secret and set it in your .env:\n"
                        "  export TELEGRAM_WEBHOOK_SECRET=\"$(openssl rand -hex 32)\"\n\n"
                        "Then register it with Telegram when setting the "
                        "webhook via setWebhook's secret_token parameter."
                    )
                from urllib.parse import urlparse
                webhook_path = urlparse(webhook_url).path or "/telegram"

                await self._app.updater.start_webhook(
                    listen=webhook_host,
                    port=webhook_port,
                    url_path=webhook_path,
                    webhook_url=webhook_url,
                    secret_token=webhook_secret,
                    allowed_updates=Update.ALL_TYPES,
                    # Webhooks are push-based — Telegram does not hold a
                    # server-side getUpdates queue, so this flag is a no-op
                    # in practice. Mirror the polling path's reconnect
                    # semantics for consistency.
                    drop_pending_updates=not is_reconnect,
                )
                self._webhook_mode = True
                self._polling_progress_accepting = False
                self._send_path_degraded = False
                logger.info(
                    "[%s] Webhook server listening on %s:%d%s",
                    self.name,
                    webhook_host or "* (all interfaces, IPv4+IPv6)",
                    webhook_port,
                    webhook_path,
                )
            else:
                # ── Polling mode (default) ───────────────────────────
                # Clear any stale webhook first so polling doesn't inherit a
                # previous webhook registration and silently stop receiving
                # updates. Best-effort: a transient Bot API network error here
                # must not fail gateway startup — degrade to background polling
                # recovery instead.
                await self._delete_webhook_best_effort(
                    require_success=not is_reconnect
                )

                loop = asyncio.get_running_loop()

                def _polling_error_callback(error: Exception) -> None:
                    if getattr(self, "_polling_teardown_started", False):
                        return
                    if self._polling_error_task and not self._polling_error_task.done():
                        return
                    if self._looks_like_polling_conflict(error):
                        # Synchronously stop PTB's internal network_retry_loop
                        # BEFORE scheduling our async recovery task.  PTB calls
                        # this callback synchronously inside its loop and then
                        # keeps polling on its own; if we only schedule a task
                        # here, PTB's retry and our stop->restart overlap and
                        # produce a fresh 409.  Disarming the loop now makes it
                        # exit on its next tick so recovery owns polling alone.
                        self._disarm_ptb_retry_loop()
                        self._polling_error_task = loop.create_task(self._handle_polling_conflict(error))
                        self._background_tasks.add(self._polling_error_task)
                        self._polling_error_task.add_done_callback(self._background_tasks.discard)
                    elif self._looks_like_network_error(error):
                        logger.warning("[%s] Telegram network _redact_telegram_error_text(error), scheduling reconnect: %s", self.name, error)
                        self._polling_error_task = loop.create_task(self._handle_polling_network_error(error))
                        self._background_tasks.add(self._polling_error_task)
                        self._polling_error_task.add_done_callback(self._background_tasks.discard)
                    else:
                        logger.error("[%s] Telegram polling _redact_telegram_error_text(error): %s", self.name, error, exc_info=True)

                # Store reference for retry use in _handle_polling_conflict
                self._polling_error_callback_ref = _polling_error_callback

                polling_started = await self._start_polling_resilient(
                    # On a cold first boot drop the stale Bot API queue; on a
                    # watcher reconnect after an outage preserve it so messages
                    # sent while the bot was offline are delivered (#46621).
                    drop_pending_updates=not is_reconnect,
                    error_callback=_polling_error_callback,
                    require_progress=not is_reconnect,
                )
                if not polling_started:
                    logger.warning(
                        "[%s] Connected in degraded Telegram mode: gateway is alive, "
                        "polling will be retried in the background",
                        self.name,
                    )

            self._mark_connected()
            mode = "webhook" if self._webhook_mode else "polling"
            logger.info("[%s] Connected to Telegram (%s mode)", self.name, mode)

            # Start the persistent heartbeat loop in polling mode. Webhook mode
            # receives updates via incoming pushes — there is no long-poll
            # socket to wedge in CLOSE-WAIT, so the loop is not needed there.
            if not self._webhook_mode:
                if self._polling_heartbeat_task and not self._polling_heartbeat_task.done():
                    self._polling_heartbeat_task.cancel()
                self._polling_heartbeat_task = asyncio.ensure_future(
                    self._polling_heartbeat_loop()
                )

            # Seed the live identity from whatever PTB cached during
            # initialize(), then keep it fresh. Polling mode rides the
            # heartbeat's get_me() probe; webhook mode has no probe at all, so
            # it gets a dedicated low-frequency refresh loop — otherwise a
            # BotFather rename breaks mention routing until restart.
            self._note_bot_username(getattr(self._bot, "username", None))
            self._bot_identity_checked_at = time.monotonic()
            if self._webhook_mode:
                identity_task = getattr(self, "_bot_identity_refresh_task", None)
                if identity_task and not identity_task.done():
                    identity_task.cancel()
                self._bot_identity_refresh_task = asyncio.ensure_future(
                    self._bot_identity_refresh_loop()
                )

            # Command-menu registration, DM-topic setup, and the status
            # indicator each make Bot API calls that can stall for certain
            # tokens. Running them here — inside the connect() coroutine that
            # the gateway wraps in a connect timeout — means one slow call
            # blows the whole connect and the adapter never comes up, even
            # though polling/webhook is already live (#46298). Defer them to a
            # cancellable background task so connect() returns as soon as the
            # transport is up.
            self._start_post_connect_housekeeping()

            return True

        except Exception as e:
            self._release_platform_lock()
            safe_error = _redact_telegram_error_text(e)
            message = f"Telegram startup failed: {safe_error}"
            self._set_fatal_error("telegram_connect_error", message, retryable=True)
            logger.error("[%s] Failed to connect to Telegram: %s", self.name, safe_error)
            return False

    async def disconnect(self) -> None:
        """Stop polling/webhook, cancel pending delayed deliveries, and disconnect."""
        # Mark disconnected first so the drop guard short-circuits any flush
        # that wins the race against teardown and prevents new delayed tasks
        # from being scheduled by late update handlers.
        self._mark_disconnected()
        self._polling_teardown_started = True
        self._polling_progress_accepting = False
        self._polling_generation = getattr(self, "_polling_generation", 0) + 1
        self._polling_progress_event = asyncio.Event()
        self._send_path_degraded = True

        # Recovery can be suspended in stop/drain/start while disconnect begins.
        # Cancel and await both polling lifecycle owners immediately after the
        # fence, before any other teardown await lets them start a new generation.
        current_task = asyncio.current_task()
        lifecycle_tasks: list[asyncio.Task] = []
        lifecycle_seen: set[int] = set()
        for task in (
            getattr(self, "_polling_error_task", None),
            getattr(self, "_polling_progress_verifier_task", None),
        ):
            if not task or task.done() or task is current_task:
                continue
            marker = id(task)
            if marker in lifecycle_seen:
                continue
            lifecycle_seen.add(marker)
            task.cancel()
            if asyncio.isfuture(task) or asyncio.iscoroutine(task):
                lifecycle_tasks.append(task)
        if lifecycle_tasks:
            await asyncio.gather(*lifecycle_tasks, return_exceptions=True)
        if getattr(self, "_polling_error_task", None) is not current_task:
            self._polling_error_task = None
        if getattr(self, "_polling_progress_verifier_task", None) is not current_task:
            self._polling_progress_verifier_task = None

        # Cancellation callbacks may have run while awaited; the teardown fence
        # remains authoritative regardless of their finalizers.
        self._polling_progress_accepting = False
        self._send_path_degraded = True

        # Cancel deferred post-connect housekeeping (command-menu / DM-topic /
        # status-indicator Bot API calls) so it cannot fire into a half-torn-down
        # bot client (#46298). getattr guards the object.__new__ test pattern
        # where __init__ (which sets this attr) is never called.
        post_connect_task = getattr(self, "_post_connect_task", None)
        if post_connect_task and not post_connect_task.done():
            post_connect_task.cancel()
            await asyncio.gather(post_connect_task, return_exceptions=True)
        self._post_connect_task = None

        # Cancel the heartbeat before tearing down the app so the probe task
        # cannot fire get_me() into a half-shutdown bot client.
        polling_heartbeat_task = getattr(self, "_polling_heartbeat_task", None)
        if polling_heartbeat_task and not polling_heartbeat_task.done():
            polling_heartbeat_task.cancel()
            try:
                await polling_heartbeat_task
            except asyncio.CancelledError:
                pass
        self._polling_heartbeat_task = None

        # Cancel the webhook-mode identity refresh loop on the same fence as
        # the heartbeat so it cannot fire get_me() into a torn-down client.
        identity_task = getattr(self, "_bot_identity_refresh_task", None)
        if identity_task and not identity_task.done():
            identity_task.cancel()
            try:
                await identity_task
            except asyncio.CancelledError:
                pass
        self._bot_identity_refresh_task = None

        # Mark the bot "Offline" in its short description while the bot's HTTP
        # client is still alive (before app shutdown closes it). Opt-in via
        # extra.status_indicator. Non-fatal. This is the clean-shutdown path;
        # a hard crash leaves the last-known status, which is the expected
        # limitation of a profile-text indicator.
        try:
            await self._set_status_indicator(online=False)
        except Exception:
            pass

        await self._cancel_pending_delivery_tasks()

        if self._app:
            try:
                # Only stop the updater if it's running.  Bounded with a
                # timeout: a CLOSE-WAIT socket can wedge stop() on epoll
                # indefinitely, which would hang disconnect() (and any
                # gateway shutdown/restart waiting on it) forever.  On timeout
                # we fall through to app.stop()/shutdown() to force teardown.
                if self._app.updater and self._app.updater.running:
                    try:
                        await asyncio.wait_for(self._app.updater.stop(), timeout=_UPDATER_STOP_TIMEOUT)
                    except asyncio.TimeoutError:
                        logger.warning(
                            "[%s] updater.stop() timed out during disconnect "
                            "(likely CLOSE-WAIT socket); forcing app shutdown",
                            self.name,
                        )
                if self._app.running:
                    await self._app.stop()
                await self._app.shutdown()
            except Exception as e:
                logger.warning(
                    "[%s] Error during Telegram disconnect: %s",
                    self.name, _redact_telegram_error_text(e),
                )
        self._release_platform_lock()

        self._app = None
        self._bot = None
        logger.info("[%s] Disconnected from Telegram", self.name)

    async def _cancel_pending_delivery_tasks(self) -> None:
        """Cancel every delayed-delivery task family before disconnect completes.

        Covers media-group, photo-batch and text-batch flush tasks plus the
        polling-error recovery task. Each sits behind an ``asyncio.sleep()``;
        if teardown leaves them running they dispatch ``handle_message`` into a
        torn-down session. Skips the current task so the coroutine driving
        teardown does not cancel itself.
        """
        current_task = asyncio.current_task()
        pending_tasks: list[asyncio.Task] = []
        awaitable_tasks: list[asyncio.Task] = []
        seen: set[int] = set()

        def collect(task: Optional[asyncio.Task]) -> None:
            if not task or task.done() or task is current_task:
                return
            marker = id(task)
            if marker in seen:
                return
            seen.add(marker)
            pending_tasks.append(task)
            if asyncio.isfuture(task) or asyncio.iscoroutine(task):
                awaitable_tasks.append(task)

        for task in list(self._media_group_tasks.values()):
            collect(task)
        for task in list(self._pending_photo_batch_tasks.values()):
            collect(task)
        for task in list(self._pending_text_batch_tasks.values()):
            collect(task)
        collect(getattr(self, "_polling_error_task", None))
        collect(getattr(self, "_polling_progress_verifier_task", None))

        for task in pending_tasks:
            task.cancel()
        if awaitable_tasks:
            await asyncio.gather(*awaitable_tasks, return_exceptions=True)

        self._media_group_tasks.clear()
        self._media_group_events.clear()
        self._pending_photo_batch_tasks.clear()
        self._pending_photo_batches.clear()
        self._pending_text_batch_tasks.clear()
        self._pending_text_batches.clear()
        if getattr(self, "_polling_error_task", None) is not current_task:
            self._polling_error_task = None
        if getattr(self, "_polling_progress_verifier_task", None) is not current_task:
            self._polling_progress_verifier_task = None

    async def _set_status_indicator(self, online: bool) -> None:
        """Set the bot's short description to the online/offline status text.

        The short description is the line shown under the bot's name in its
        profile. It is the closest Bot API surface to a presence indicator —
        bots have no real online/offline dot (that's a user-account feature).

        No-op unless ``extra.status_indicator`` is enabled. Best-effort: any
        failure is logged at debug and swallowed so it never blocks connect or
        disconnect. The default (no language_code) description applies to every
        user who doesn't have a language-specific one set.
        """
        if not getattr(self, "_status_indicator_enabled", False):
            return
        bot = self._bot
        if bot is None:
            return
        text = self._status_online_text if online else self._status_offline_text
        # Telegram caps short_description at 120 chars.
        text = text[:120]
        try:
            await bot.set_my_short_description(short_description=text)
            logger.info("[%s] Set bot status indicator to %r", self.name, text)
        except Exception as e:
            logger.debug(
                "[%s] Failed to set bot status indicator to %r: %s",
                self.name, text, _redact_telegram_error_text(e),
            )

    def _start_post_connect_housekeeping(self) -> None:
        """Kick off deferred post-connect housekeeping in the background.

        Idempotent: if a previous housekeeping task is still running (e.g. a
        rapid reconnect), it is left in place rather than double-scheduled.
        """
        task = self._post_connect_task
        if task and not task.done():
            return
        self._post_connect_task = asyncio.ensure_future(
            self._run_post_connect_housekeeping()
        )

    async def _run_post_connect_housekeeping(self) -> None:
        """Register the command menu, surface the status indicator, and set up
        DM topics — all off the connect path so a slow Bot API call cannot blow
        the gateway connect timeout (#46298). Every step is non-fatal."""
        try:
            # Register bot commands so Telegram shows a hint menu when users type /
            # List is derived from the central COMMAND_REGISTRY — adding a new
            # gateway command there automatically adds it to the Telegram menu.
            try:
                from telegram import (
                    BotCommand,
                    BotCommandScopeAllPrivateChats,
                    BotCommandScopeAllGroupChats,
                    BotCommandScopeDefault,
                )
                from hermes_cli.commands import telegram_menu_commands, telegram_menu_max_commands
                if not self._bot:
                    return
                # Telegram allows up to 100 commands but has an undocumented
                # payload size limit (~4KB total).  Hermes defaults to 60 to
                # keep built-ins plus common skill commands visible while
                # staying under the threshold; users can tune the cap via
                # platforms.telegram.extra.command_menu.
                max_commands = telegram_menu_max_commands()
                menu_commands, hidden_count = telegram_menu_commands(max_commands=max_commands)
                bot_commands = [BotCommand(name, desc) for name, desc in menu_commands]
                # Register for all scopes independently — Telegram picks the
                # narrowest matching scope per chat type (forum topics fall
                # through to AllGroupChats or Default).
                for scope_cls in (BotCommandScopeDefault, BotCommandScopeAllPrivateChats, BotCommandScopeAllGroupChats):
                    scope_name = getattr(scope_cls, "__name__", str(scope_cls))
                    try:
                        await self._bot.set_my_commands(bot_commands, scope=scope_cls())
                        logger.info("[%s] set_my_commands OK for scope %s (%d cmds)", self.name, scope_name, len(bot_commands))
                    except Exception as scope_err:
                        logger.warning("[%s] set_my_commands FAILED for scope %s: %s", self.name, scope_name, scope_err)
                # Forum topics don't inherit AllGroupChats — Telegram resolves
                # commands via BotCommandScopeChat(chat_id) for forum groups.
                # Lazy registration happens in _ensure_forum_commands on first
                # message from a forum topic (see _handle_text_message).
                if hidden_count:
                    logger.info(
                        "[%s] Telegram menu: %d commands registered, %d hidden (over %d limit). Use /commands for full list.",
                        self.name, len(menu_commands), hidden_count, max_commands,
                    )
            except Exception as e:
                logger.warning(
                    "[%s] Could not register Telegram command menu: %s",
                    self.name,
                    _redact_telegram_error_text(e),
                    exc_info=True,
                )

            # Surface the gateway as "Online" in the bot's short description
            # (opt-in via extra.status_indicator). Non-fatal.
            try:
                await self._set_status_indicator(online=True)
            except Exception:
                pass

            # Set up DM topics (Bot API 9.4 — Private Chat Topics)
            # Runs after connection is established so the bot can call createForumTopic.
            # Failures here are non-fatal — the bot works fine without topics.
            try:
                await self._setup_dm_topics()
            except Exception as topics_err:
                logger.warning(
                    "[%s] DM topics setup failed (non-fatal): %s",
                    self.name, topics_err, exc_info=True,
                )
        except asyncio.CancelledError:
            raise
        finally:
            if self._post_connect_task is asyncio.current_task():
                self._post_connect_task = None

    async def _bot_identity_refresh_loop(self) -> None:
        """Keep the cached @username fresh when no heartbeat is running.

        Polling mode re-reads identity via the heartbeat's ``get_me()`` probe.
        Webhook mode has no such probe — nothing calls ``get_me()`` again after
        ``initialize()`` — so without this loop a BotFather rename breaks
        mention routing until the gateway restarts.
        """
        while True:
            try:
                await asyncio.sleep(self._BOT_IDENTITY_TTL_SECONDS)
                if getattr(self, "_polling_teardown_started", False):
                    return
                if self.has_fatal_error:
                    return
                await self._refresh_bot_identity(force=True)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.debug(
                    "[%s] Telegram identity refresh loop iteration failed",
                    self.name, exc_info=True,
                )
