"""S1 cluster mixin: TelegramInteractiveMixin."""

from __future__ import annotations

import asyncio
import html as _html
from typing import Dict, List, Optional, Set, Any
from pathlib import Path as _Path
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
    ChatType,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ParseMode,
    _redact_telegram_error_text,
    logger,
)

class TelegramInteractiveMixin:
    async def send_update_prompt(
        self, chat_id: str, prompt: str, default: str = "",
        session_key: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an inline-keyboard update prompt (Yes / No buttons).

        Used by the gateway ``/update`` watcher when ``hermes update --gateway``
        needs user input (stash restore, config migration).
        """
        if not self._bot:
            return SendResult(success=False, error="Not connected")
        try:
            default_hint = f" (default: {default})" if default else ""
            text = self.format_message(f"⚕ *Update needs your input:*\n\n{prompt}{default_hint}")
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✓ Yes", callback_data="update_prompt:y"),
                    InlineKeyboardButton("✗ No", callback_data="update_prompt:n"),
                ]
            ])
            thread_id = self._metadata_thread_id(metadata)
            reply_to_id = self._reply_to_message_id_for_send(None, metadata, reply_to_mode=self._reply_to_mode)
            msg = await self._send_message_with_thread_fallback(
                chat_id=normalize_telegram_chat_id(chat_id),
                text=text,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard,
                reply_to_message_id=reply_to_id,
                **self._thread_kwargs_for_send(
                    chat_id,
                    thread_id,
                    metadata,
                    reply_to_message_id=reply_to_id,
                    reply_to_mode=self._reply_to_mode
                ),
                **self._link_preview_kwargs(),
            )
            return SendResult(success=True, message_id=str(msg.message_id))
        except Exception as e:
            logger.warning("[%s] send_update_prompt failed: %s", self.name, _redact_telegram_error_text(e))
            return SendResult(success=False, error=_redact_telegram_error_text(e))

    def _ea_escape(self, text: str) -> str:
        return _html.escape(text)

    async def send_exec_approval(
        self, chat_id: str, command: str, session_key: str,
        description: str = "dangerous command",
        metadata: Optional[Dict[str, Any]] = None,
        allow_permanent: bool = True,
        allow_session: bool = True,
        smart_denied: bool = False,
    ) -> SendResult:
        """Send an inline-keyboard approval prompt with interactive buttons.

        The buttons call ``resolve_gateway_approval()`` to unblock the waiting
        agent thread — same mechanism as the text ``/approve`` flow.
        """
        if not self._bot:
            return SendResult(success=False, error="Not connected")

        try:
            text = self._format_exec_approval(command, description, smart_denied)

            # Resolve thread context for thread replies
            thread_id = self._metadata_thread_id(metadata)

            # We'll use the message_id as part of callback_data to look up session_key
            # Send a placeholder first, then update — or use a counter.
            # Simpler: use a monotonic counter to generate short IDs.
            import itertools
            if not hasattr(self, "_approval_counter"):
                self._approval_counter = itertools.count(1)
            approval_id = next(self._approval_counter)

            buttons = [
                InlineKeyboardButton("✅ Allow Once", callback_data=f"ea:once:{approval_id}")
            ]
            if not smart_denied and allow_session:
                buttons.append(
                    InlineKeyboardButton("✅ Session", callback_data=f"ea:session:{approval_id}")
                )
                if allow_permanent:
                    buttons.append(
                        InlineKeyboardButton("✅ Always", callback_data=f"ea:always:{approval_id}")
                    )
            buttons.append(InlineKeyboardButton("❌ Deny", callback_data=f"ea:deny:{approval_id}"))
            # Pair into rows (2x2 for the full set) so labels stay readable on
            # mobile — a single 4-button row truncates to "Allo… / Ses… / …".
            rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
            keyboard = InlineKeyboardMarkup(rows)

            kwargs: Dict[str, Any] = {
                "chat_id": normalize_telegram_chat_id(chat_id),
                "text": text,
                "parse_mode": ParseMode.HTML,
                "reply_markup": keyboard,
                **self._link_preview_kwargs(),
            }
            reply_to_id = self._reply_to_message_id_for_send(None, metadata, reply_to_mode=self._reply_to_mode)
            kwargs["reply_to_message_id"] = reply_to_id
            kwargs.update(
                self._thread_kwargs_for_send(
                    chat_id,
                    thread_id,
                    metadata,
                    reply_to_message_id=reply_to_id,
                    reply_to_mode=self._reply_to_mode
                )
            )

            msg = await self._send_message_with_thread_fallback(**kwargs)

            # Store session_key keyed by approval_id for the callback handler
            self._approval_state[approval_id] = session_key

            return SendResult(success=True, message_id=str(msg.message_id))
        except Exception as e:
            logger.warning("[%s] send_exec_approval failed: %s", self.name, _redact_telegram_error_text(e))
            return SendResult(success=False, error=_redact_telegram_error_text(e))

    async def send_slash_confirm(
        self, chat_id: str, title: str, message: str, session_key: str,
        confirm_id: str, metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Render a three-button slash-command confirmation prompt."""
        if not self._bot:
            return SendResult(success=False, error="Not connected")

        try:
            preview = self.format_message(self._truncate_preview(message, 3800))

            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Approve Once", callback_data=f"sc:once:{confirm_id}"),
                    InlineKeyboardButton("🔒 Always Approve", callback_data=f"sc:always:{confirm_id}"),
                ],
                [
                    InlineKeyboardButton("❌ Cancel", callback_data=f"sc:cancel:{confirm_id}"),
                ],
            ])

            thread_id = self._metadata_thread_id(metadata)
            kwargs: Dict[str, Any] = {
                "chat_id": normalize_telegram_chat_id(chat_id),
                "text": preview,
                "parse_mode": ParseMode.MARKDOWN_V2,
                "reply_markup": keyboard,
                **self._link_preview_kwargs(),
            }
            reply_to_id = self._reply_to_message_id_for_send(None, metadata, reply_to_mode=self._reply_to_mode)
            kwargs["reply_to_message_id"] = reply_to_id
            kwargs.update(
                self._thread_kwargs_for_send(
                    chat_id,
                    thread_id,
                    metadata,
                    reply_to_message_id=reply_to_id,
                    reply_to_mode=self._reply_to_mode
                )
            )

            msg = await self._send_message_with_thread_fallback(**kwargs)
            self._slash_confirm_state[confirm_id] = session_key
            return SendResult(success=True, message_id=str(msg.message_id))
        except Exception as e:
            logger.warning("[%s] send_slash_confirm failed: %s", self.name, _redact_telegram_error_text(e))
            return SendResult(success=False, error=_redact_telegram_error_text(e))

    async def send_clarify(
        self,
        chat_id: str,
        question: str,
        choices: Optional[list],
        clarify_id: str,
        session_key: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Render a clarify prompt with one inline button per choice.

        Multi-choice mode (``choices`` non-empty): renders one button per
        option plus a final "✏️ Other (type answer)" button.  Picking the
        "Other" button flips the entry into text-capture mode so the next
        message becomes the response.

        Open-ended mode (``choices`` empty): renders the question as plain
        text — no buttons.  The next message in the session is captured by
        the gateway's text-intercept and resolves the clarify.
        """
        if not self._bot:
            return SendResult(success=False, error="Not connected")

        try:
            text = f"❓ {_html.escape(question)}"
            thread_id = self._metadata_thread_id(metadata)

            if choices:
                # Render full option text in the message body so mobile
                # users can read long choices that would be truncated in
                # inline button labels.  Buttons keep short numeric labels
                # (1, 2, …, Other) to avoid Telegram truncation.
                option_lines = "\n".join(
                    f"{i + 1}. {_html.escape(str(c))}"
                    for i, c in enumerate(choices)
                )
                text += f"\n\n{option_lines}"

            kwargs: Dict[str, Any] = {
                "chat_id": normalize_telegram_chat_id(chat_id),
                "text": text,
                "parse_mode": ParseMode.HTML,
                **self._link_preview_kwargs(),
            }

            if choices:
                # Telegram caps callback_data at 64 bytes; keep "cl:<id>:<idx>"
                # short.
                rows = []
                for idx in range(len(choices)):
                    rows.append([
                        InlineKeyboardButton(
                            str(idx + 1),
                            callback_data=f"cl:{clarify_id}:{idx}",
                        )
                    ])
                rows.append([
                    InlineKeyboardButton(
                        "✏️ Other (type answer)",
                        callback_data=f"cl:{clarify_id}:other",
                    )
                ])
                kwargs["reply_markup"] = InlineKeyboardMarkup(rows)

            reply_to_id = self._reply_to_message_id_for_send(None, metadata)
            kwargs["reply_to_message_id"] = reply_to_id
            kwargs.update(
                self._thread_kwargs_for_send(
                    chat_id,
                    thread_id,
                    metadata,
                    reply_to_message_id=reply_to_id,
                )
            )

            msg = await self._send_message_with_thread_fallback(**kwargs)
            self._clarify_state[clarify_id] = session_key
            return SendResult(success=True, message_id=str(msg.message_id))
        except Exception as e:
            logger.warning("[%s] send_clarify failed: %s", self.name, _redact_telegram_error_text(e))
            return SendResult(success=False, error=_redact_telegram_error_text(e))

    async def send_model_picker(
        self,
        chat_id: str,
        providers: list,
        current_model: str,
        current_provider: str,
        session_key: str,
        on_model_selected,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an interactive inline-keyboard model picker.

        Two-step drill-down: provider selection → model selection.
        Edits the same message in-place as the user navigates.
        """
        if not self._bot:
            return SendResult(success=False, error="Not connected")

        try:
            from hermes_cli.providers import get_label
        except ImportError:
            def get_label(slug):
                return slug

        try:
            # Build provider buttons — folds provider groups (display only).
            keyboard, provider_page_info = self._build_provider_keyboard(providers, 0)

            provider_label = get_label(current_provider)
            text = self.format_message(
                (
                    f"⚙ *Model Configuration*\n\n"
                    f"Current model: `{current_model or 'unknown'}`\n"
                    f"Provider: {provider_label}\n\n"
                    f"Select a provider:{provider_page_info}"
                )
            )

            thread_id = metadata.get("thread_id") if metadata else None
            reply_to_id = self._reply_to_message_id_for_send(None, metadata, reply_to_mode=self._reply_to_mode)
            msg = await self._send_message_with_thread_fallback(
                chat_id=normalize_telegram_chat_id(chat_id),
                text=text,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard,
                reply_to_message_id=reply_to_id,
                **self._thread_kwargs_for_send(
                    chat_id,
                    thread_id,
                    metadata,
                    reply_to_message_id=reply_to_id,
                    reply_to_mode=self._reply_to_mode
                ),
                **self._link_preview_kwargs(),
            )

            # Store picker state keyed by chat_id
            self._model_picker_state[str(chat_id)] = {
                "msg_id": msg.message_id,
                "providers": providers,
                "session_key": session_key,
                "on_model_selected": on_model_selected,
                "current_model": current_model,
                "current_provider": current_provider,
                "provider_page": 0,
            }

            return SendResult(success=True, message_id=str(msg.message_id))
        except Exception as e:
            logger.warning("[%s] send_model_picker failed: %s", self.name, _redact_telegram_error_text(e))
            return SendResult(success=False, error=_redact_telegram_error_text(e))

    async def send_choice_picker(
        self,
        chat_id: str,
        title: str,
        choices: list,
        session_key: str,
        on_choice_selected,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a flat inline-keyboard choice picker (one tap → one value).

        Generic single-level companion to ``send_model_picker`` used by
        `/reasoning`, `/fast`, and any future finite-choice command. Each
        choice dict: ``{"value": str, "label": str, "is_current": bool}``.
        """
        if not self._bot:
            return SendResult(success=False, error="Not connected")

        try:
            buttons = []
            for i, choice in enumerate(choices):
                label = str(choice.get("label") or choice.get("value") or "")
                if choice.get("is_current"):
                    label = f"✓ {label}"
                buttons.append(
                    InlineKeyboardButton(label, callback_data=f"cp:{i}")
                )
            if not buttons:
                return SendResult(success=False, error="No choices")
            # Two buttons per row keeps labels readable on mobile.
            keyboard = InlineKeyboardMarkup(
                [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
            )

            thread_id = metadata.get("thread_id") if metadata else None
            reply_to_id = self._reply_to_message_id_for_send(None, metadata, reply_to_mode=self._reply_to_mode)
            msg = await self._send_message_with_thread_fallback(
                chat_id=normalize_telegram_chat_id(chat_id),
                text=self.format_message(title),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard,
                reply_to_message_id=reply_to_id,
                **self._thread_kwargs_for_send(
                    chat_id,
                    thread_id,
                    metadata,
                    reply_to_message_id=reply_to_id,
                    reply_to_mode=self._reply_to_mode
                ),
                **self._link_preview_kwargs(),
            )

            self._choice_picker_state[str(chat_id)] = {
                "msg_id": msg.message_id,
                "choices": choices,
                "session_key": session_key,
                "on_choice_selected": on_choice_selected,
            }
            return SendResult(success=True, message_id=str(msg.message_id))
        except Exception as e:
            logger.warning("[%s] send_choice_picker failed: %s", self.name, _redact_telegram_error_text(e))
            return SendResult(success=False, error=_redact_telegram_error_text(e))

    async def _handle_choice_picker_callback(
        self, query, data: str, chat_id: str
    ) -> None:
        """Handle choice picker button taps (cp:<index>)."""
        state = self._choice_picker_state.get(chat_id)
        if not state:
            await query.answer(text="Picker expired — run the command again.")
            return

        # Same authorization gate as approval buttons: unauthorized users in a
        # shared group must not flip session/config state via someone else's
        # picker message.
        query_message = getattr(query, "message", None)
        query_chat = getattr(query_message, "chat", None)
        if not self._is_callback_user_authorized(
            str(getattr(query.from_user, "id", "")),
            chat_id=getattr(query_message, "chat_id", None),
            chat_type=str(getattr(query_chat, "type", None)) if getattr(query_chat, "type", None) is not None else None,
            thread_id=str(getattr(query_message, "message_thread_id", None)) if getattr(query_message, "message_thread_id", None) is not None else None,
            user_name=getattr(query.from_user, "first_name", None),
        ):
            await query.answer(text="⛔ You are not authorized to change this setting.")
            return

        try:
            idx = int(data[3:])
            choice = state["choices"][idx]
        except (ValueError, IndexError):
            await query.answer(text="Invalid selection.")
            return

        callback = state.get("on_choice_selected")
        if not callback:
            await query.answer(text="Picker expired.")
            return

        try:
            result_text = await callback(chat_id, str(choice.get("value") or ""))
        except Exception as exc:
            logger.error("Choice picker selection failed: %s", exc)
            result_text = f"Error applying selection: {exc}"

        try:
            await query.edit_message_text(
                text=self.format_message(result_text),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=None,
            )
        except Exception:
            try:
                await query.edit_message_text(
                    text=result_text, parse_mode=None, reply_markup=None,
                )
            except Exception:
                pass
        await query.answer()
        self._choice_picker_state.pop(chat_id, None)

    def _build_provider_keyboard(self, providers: list, page: int = 0) -> tuple:
        """Build the paginated top-level provider keyboard, folding groups.

        Provider families (Kimi/Moonshot, MiniMax, xAI Grok, ...) collapse to
        a single ``mpg:<gid>`` button; tapping it drills into a member
        sub-keyboard. Single providers (and groups with only one authenticated
        member) render as direct ``mp:<slug>`` buttons. Grouping mirrors the
        CLI ``hermes model`` picker via the shared ``group_providers`` fold,
        so all surfaces stay consistent.
        """
        try:
            from hermes_cli.models import group_providers
        except Exception:
            group_providers = None

        by_slug = {p.get("slug"): p for p in providers}

        def _provider_button(p):
            count = p.get("total_models", len(p.get("models", [])))
            label = f"{p['name']} ({count})"
            if p.get("is_current"):
                label = f"✓ {label}"
            return InlineKeyboardButton(label, callback_data=f"mp:{p['slug']}")

        buttons: list = []
        if group_providers is not None:
            for row in group_providers([p.get("slug") for p in providers]):
                if row["kind"] == "group":
                    members = [by_slug[m] for m in row["members"] if m in by_slug]
                    count = sum(
                        m.get("total_models", len(m.get("models", []))) for m in members
                    )
                    label = f"{row['label']} ▸ ({count})"
                    if any(m.get("is_current") for m in members):
                        label = f"✓ {label}"
                    buttons.append(
                        InlineKeyboardButton(label, callback_data=f"mpg:{row['group_id']}")
                    )
                else:
                    p = by_slug.get(row["slug"])
                    if p is not None:
                        buttons.append(_provider_button(p))
        else:
            for p in providers:
                buttons.append(_provider_button(p))

        page_buttons, page_meta = self._format_choice_page(
            buttons, page, self._PROVIDER_PAGE_SIZE
        )
        page = page_meta["page"]
        total_pages = page_meta["total_pages"]

        rows = [page_buttons[i : i + 2] for i in range(0, len(page_buttons), 2)]

        if total_pages > 1:
            nav: list = []
            if page > 0:
                nav.append(InlineKeyboardButton("◀ Prev", callback_data=f"mpv:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="mx:noop"))
            if page < total_pages - 1:
                nav.append(InlineKeyboardButton("Next ▶", callback_data=f"mpv:{page + 1}"))
            rows.append(nav)

        rows.append([InlineKeyboardButton("✗ Cancel", callback_data="mx")])

        return InlineKeyboardMarkup(rows), page_meta["page_info"]

    def _build_model_keyboard(self, models: list, page: int) -> tuple:
        """Build paginated model buttons. Returns (keyboard, page_info_text)."""
        page_models, page_meta = self._format_choice_page(
            models, page, self._MODEL_PAGE_SIZE
        )
        page = page_meta["page"]
        total_pages = page_meta["total_pages"]
        start = page_meta["start"]

        buttons: list = []
        for i, model_id in enumerate(page_models):
            abs_idx = start + i
            short = model_id.split("/")[-1] if "/" in model_id else model_id
            if len(short) > 38:
                short = short[:35] + "..."
            buttons.append(
                InlineKeyboardButton(short, callback_data=f"mm:{abs_idx}")
            )

        rows = [buttons[i : i + 2] for i in range(0, len(buttons), 2)]

        # Pagination row (if needed)
        if total_pages > 1:
            nav: list = []
            if page > 0:
                nav.append(InlineKeyboardButton("◀ Prev", callback_data=f"mg:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="mx:noop"))
            if page < total_pages - 1:
                nav.append(InlineKeyboardButton("Next ▶", callback_data=f"mg:{page + 1}"))
            rows.append(nav)

        rows.append([
            InlineKeyboardButton("◀ Back", callback_data="mb"),
            InlineKeyboardButton("✗ Cancel", callback_data="mx"),
        ])

        return InlineKeyboardMarkup(rows), page_meta["page_info"]

    async def _handle_model_picker_callback(
        self, query, data: str, chat_id: str
    ) -> None:
        """Handle model picker inline keyboard callbacks (mp:/mm:/mc:/mb:/mx:/mg:)."""
        state = self._model_picker_state.get(chat_id)
        if not state:
            await query.answer(text="Picker expired — use /model again.")
            return

        try:
            from hermes_cli.providers import get_label
        except ImportError:
            def get_label(slug):
                return slug

        if data.startswith("mp:"):
            # --- Provider selected: show model buttons (page 0) ---
            provider_slug = data[3:]
            provider = next(
                (p for p in state["providers"] if p["slug"] == provider_slug),
                None,
            )
            if not provider:
                await query.answer(text="Provider not found.")
                return

            models = provider.get("models", [])
            state["selected_provider"] = provider_slug
            state["selected_provider_name"] = provider.get("name", provider_slug)
            state["model_list"] = models
            state["model_page"] = 0

            keyboard, page_info = self._build_model_keyboard(models, 0)

            pname = provider.get("name", provider_slug)
            total = provider.get("total_models", len(models))
            shown = len(models)
            extra = f"\n_{total - shown} more available — type `/model <name>` directly_" if total > shown else ""

            await query.edit_message_text(
                text=self.format_message(
                    (
                        f"⚙ *Model Configuration*\n\n"
                        f"Provider: *{pname}*{page_info}\n"
                        f"Select a model:{extra}"
                    )
                ),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard,
            )
            await query.answer()

        elif data.startswith("mg:"):
            # --- Page navigation ---
            try:
                page = int(data[3:])
            except ValueError:
                await query.answer(text="Invalid page.")
                return

            models = state.get("model_list", [])
            state["model_page"] = page

            keyboard, page_info = self._build_model_keyboard(models, page)

            pname = state.get("selected_provider_name", "")
            provider_slug = state.get("selected_provider", "")
            provider = next(
                (p for p in state["providers"] if p["slug"] == provider_slug),
                None,
            )
            total = provider.get("total_models", len(models)) if provider else len(models)
            shown = len(models)
            extra = f"\n_{total - shown} more available — type `/model <name>` directly_" if total > shown else ""

            await query.edit_message_text(
                text=self.format_message(
                    (
                        f"⚙ *Model Configuration*\n\n"
                        f"Provider: *{pname}*{page_info}\n"
                        f"Select a model:{extra}"
                    )
                ),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard,
            )
            await query.answer()

        elif data.startswith("mpv:"):
            # --- Provider page navigation ---
            try:
                page = int(data[4:])
            except ValueError:
                await query.answer(text="Invalid page.")
                return

            state["provider_page"] = page
            keyboard, provider_page_info = self._build_provider_keyboard(
                state["providers"], page
            )

            try:
                provider_label = get_label(state["current_provider"])
            except Exception:
                provider_label = state["current_provider"]

            await query.edit_message_text(
                text=self.format_message(
                    (
                        f"⚙ *Model Configuration*\n\n"
                        f"Current model: `{state['current_model'] or 'unknown'}`\n"
                        f"Provider: {provider_label}\n\n"
                        f"Select a provider:{provider_page_info}"
                    )
                ),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard,
            )
            await query.answer()

        elif data.startswith("mc:"):
            # --- Expensive model confirmed: perform the switch ---
            try:
                idx = int(data[3:])
            except ValueError:
                await query.answer(text="Invalid selection.")
                return

            model_list = state.get("model_list", [])
            if idx < 0 or idx >= len(model_list):
                await query.answer(text="Invalid model index.")
                return

            model_id = model_list[idx]
            provider_slug = state.get("selected_provider", "")
            callback = state.get("on_model_selected")

            if not callback:
                await query.answer(text="Picker expired.")
                return

            switch_failed = False
            try:
                result_text = await callback(chat_id, model_id, provider_slug)
            except Exception as exc:
                logger.error("Model picker switch failed: %s", exc)
                result_text = f"Error switching model: {exc}"
                switch_failed = True

            try:
                await query.edit_message_text(
                    text=self.format_message(result_text),
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=None,
                )
            except Exception:
                try:
                    await query.edit_message_text(
                        text=result_text,
                        parse_mode=None,
                        reply_markup=None,
                    )
                except Exception:
                    pass
            await query.answer(
                text="Switch failed." if switch_failed else "Model switched!"
            )
            self._model_picker_state.pop(chat_id, None)

        elif data.startswith("mm:"):
            # --- Model selected: perform the switch ---
            try:
                idx = int(data[3:])
            except ValueError:
                await query.answer(text="Invalid selection.")
                return

            model_list = state.get("model_list", [])
            if idx < 0 or idx >= len(model_list):
                await query.answer(text="Invalid model index.")
                return

            model_id = model_list[idx]
            provider_slug = state.get("selected_provider", "")
            callback = state.get("on_model_selected")

            if not callback:
                await query.answer(text="Picker expired.")
                return

            try:
                from hermes_cli.model_cost_guard import expensive_model_warning

                # Pricing lookup can hit models.dev / a /models endpoint on a
                # cache miss — keep it off the event loop.
                warning = await asyncio.to_thread(
                    expensive_model_warning,
                    model_id,
                    provider=provider_slug,
                )
            except Exception:
                warning = None
            if warning is not None:
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Switch anyway", callback_data=f"mc:{idx}")],
                    [
                        InlineKeyboardButton("◀ Back", callback_data="mb"),
                        InlineKeyboardButton("✗ Cancel", callback_data="mx"),
                    ],
                ])
                await query.edit_message_text(
                    text=self.format_message(
                        f"⚠ *Expensive Model Warning*\n\n{warning.message}"
                    ),
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=keyboard,
                )
                await query.answer(text="Confirm expensive model")
                return

            switch_failed = False
            try:
                result_text = await callback(chat_id, model_id, provider_slug)
            except Exception as exc:
                logger.error("Model picker switch failed: %s", exc)
                result_text = f"Error switching model: {exc}"
                switch_failed = True

            # Edit message to show confirmation, remove buttons
            try:
                await query.edit_message_text(
                    text=self.format_message(result_text),
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=None,
                )
            except Exception:
                # Markdown parse failure — retry as plain text
                try:
                    await query.edit_message_text(
                        text=result_text,
                        parse_mode=None,
                        reply_markup=None,
                    )
                except Exception:
                    pass
            await query.answer(
                text="Switch failed." if switch_failed else "Model switched!"
            )

            # Clean up state
            self._model_picker_state.pop(chat_id, None)

        elif data.startswith("mpg:"):
            # --- Provider group selected: show member providers ---
            group_id = data[4:]
            try:
                from hermes_cli.models import PROVIDER_GROUPS
                _label, _desc, member_slugs = PROVIDER_GROUPS.get(group_id, ("", "", []))
            except Exception:
                _label, member_slugs = "", []

            by_slug = {p["slug"]: p for p in state["providers"]}
            members = [by_slug[m] for m in member_slugs if m in by_slug]
            if not members:
                await query.answer(text="Group not found.")
                return

            buttons = []
            for p in members:
                count = p.get("total_models", len(p.get("models", [])))
                label = f"{p['name']} ({count})"
                if p.get("is_current"):
                    label = f"✓ {label}"
                buttons.append(
                    InlineKeyboardButton(label, callback_data=f"mp:{p['slug']}")
                )
            rows = [buttons[i : i + 2] for i in range(0, len(buttons), 2)]
            rows.append([
                InlineKeyboardButton("◀ Back", callback_data="mb"),
                InlineKeyboardButton("✗ Cancel", callback_data="mx"),
            ])
            keyboard = InlineKeyboardMarkup(rows)

            await query.edit_message_text(
                text=self.format_message(
                    (
                        f"⚙ *Model Configuration*\n\n"
                        f"Provider family: *{_label or group_id}*\n\n"
                        f"Select a provider:"
                    )
                ),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard,
            )
            await query.answer()

        elif data == "mb":
            # --- Back to provider list (folds groups) ---
            page = int(state.get("provider_page", 0) or 0)
            keyboard, provider_page_info = self._build_provider_keyboard(
                state["providers"], page
            )

            try:
                provider_label = get_label(state["current_provider"])
            except Exception:
                provider_label = state["current_provider"]

            await query.edit_message_text(
                text=self.format_message(
                    (
                        f"⚙ *Model Configuration*\n\n"
                        f"Current model: `{state['current_model'] or 'unknown'}`\n"
                        f"Provider: {provider_label}\n\n"
                        f"Select a provider:{provider_page_info}"
                    )
                ),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard,
            )
            await query.answer()

        elif data == "mx":
            # --- Cancel ---
            self._model_picker_state.pop(chat_id, None)
            await query.edit_message_text(
                text="Model selection cancelled.",
                reply_markup=None,
            )
            await query.answer()

        else:
            # Catch-all (e.g. page counter button "mx:noop")
            await query.answer()

    async def _notify_clarify_expired(self, query, user_display: str) -> None:
        """Tell the user a clarify tap arrived too late to be delivered.

        Fires when the clarify entry was evicted by ``clarify_timeout`` or the
        gateway restarted between asking and the tap. In both cases the agent
        thread is no longer waiting, so the tap would otherwise leave a
        misleading ✓ (or an "awaiting typed response" prompt) on a button the
        agent never receives.
        """
        try:
            await query.answer(text="⚠️ This prompt expired — please /retry.")
        except Exception:
            pass
        try:
            await query.edit_message_text(
                text=(
                    f"❓ {_html.escape(query.message.text or '')}\n\n"
                    "<i>⚠️ This question expired or the session reset — please /retry.</i>"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=None,
            )
        except Exception:
            pass

    async def _handle_callback_query(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        """Handle inline keyboard button clicks."""
        query = update.callback_query
        if not query or not query.data:
            return
        data = query.data
        query_message = getattr(query, "message", None)
        query_chat_id = getattr(query_message, "chat_id", None)
        query_chat = getattr(query_message, "chat", None)
        query_chat_type = getattr(query_chat, "type", None)
        query_thread_id = getattr(query_message, "message_thread_id", None)
        query_user_name = getattr(query.from_user, "first_name", None)

        # --- Model picker callbacks ---
        if data.startswith(("mp:", "mpg:", "mpv:", "mm:", "mc:", "mb", "mx", "mg:")):
            chat_id = str(query.message.chat_id) if query.message else None
            if chat_id:
                await self._handle_model_picker_callback(query, data, chat_id)
            return

        # --- Generic choice picker callbacks (/reasoning, /fast) ---
        if data.startswith("cp:"):
            chat_id = str(query.message.chat_id) if query.message else None
            if chat_id:
                await self._handle_choice_picker_callback(query, data, chat_id)
            return

        # --- Gmail-triage callbacks (gt:verb:arg) ---
        if data.startswith("gt:"):
            await self._handle_gmail_triage_callback(
                query,
                data,
                query_chat_id=query_chat_id,
                query_chat_type=query_chat_type,
                query_thread_id=query_thread_id,
                query_user_name=query_user_name,
            )
            return

        # --- Exec approval callbacks (ea:choice:id) ---
        if data.startswith("ea:"):
            parts = data.split(":", 2)
            if len(parts) == 3:
                choice = parts[1]  # once, session, always, deny
                try:
                    approval_id = int(parts[2])
                except (ValueError, IndexError):
                    await query.answer(text="Invalid approval data.")
                    return

                # Only authorized users may click approval buttons.
                caller_id = str(getattr(query.from_user, "id", ""))
                if not self._is_callback_user_authorized(
                    caller_id,
                    chat_id=query_chat_id,
                    chat_type=str(query_chat_type) if query_chat_type is not None else None,
                    thread_id=str(query_thread_id) if query_thread_id is not None else None,
                    user_name=query_user_name,
                ):
                    await query.answer(text="⛔ You are not authorized to approve commands.")
                    return

                session_key = self._approval_state.pop(approval_id, None)
                if not session_key:
                    await query.answer(text="This approval has already been resolved.")
                    return

                user_display = getattr(query.from_user, "first_name", "User")

                # Resolve the approval FIRST — unblocks the agent thread.
                # Rendering happens after so the message reflects what
                # actually occurred: a tap that lands after the approval
                # wait timed out (count == 0) must NOT claim "Approved" —
                # the command was already denied and will not run (#63501
                # regression follow-up: 60s waits made stale taps common).
                try:
                    from tools.approval import resolve_gateway_approval
                    count = resolve_gateway_approval(session_key, choice)
                    logger.info(
                        "Telegram button resolved %d approval(s) for session %s (choice=%s, user=%s)",
                        count, session_key, choice, user_display,
                    )
                except Exception as exc:
                    logger.error("Failed to resolve gateway approval from Telegram button: %s", exc)
                    count = 0

                if count:
                    # Map choice to human-readable label
                    label_map = {
                        "once": "✅ Approved once",
                        "session": "✅ Approved for session",
                        "always": "✅ Approved permanently",
                        "deny": "❌ Denied",
                    }
                    label = label_map.get(choice, "Resolved")
                    edit_text = f"{label} by {user_display}"
                else:
                    label = "⌛ Approval expired"
                    edit_text = (
                        f"{label} — no command was waiting. "
                        f"It already timed out (and was denied) or was resolved elsewhere."
                    )

                await query.answer(text=label)

                # Edit message to show decision, remove buttons
                try:
                    await query.edit_message_text(
                        text=self.format_message(edit_text),
                        parse_mode=ParseMode.MARKDOWN_V2,
                        reply_markup=None,
                    )
                except Exception:
                    pass  # non-fatal if edit fails

                # Resume the typing indicator — paused when the approval was
                # sent (gateway/run.py).  The text /approve and /deny paths
                # call resume_typing_for_chat here too; without it, typing
                # stays paused for the rest of the turn after an inline
                # button click.
                if count and query_chat_id is not None:
                    self.resume_typing_for_chat(str(query_chat_id))
            return

        # --- Slash-confirm callbacks (sc:choice:confirm_id) ---
        if data.startswith("sc:"):
            parts = data.split(":", 2)
            if len(parts) == 3:
                choice = parts[1]  # once, always, cancel
                confirm_id = parts[2]

                caller_id = str(getattr(query.from_user, "id", ""))
                if not self._is_callback_user_authorized(
                    caller_id,
                    chat_id=query_chat_id,
                    chat_type=str(query_chat_type) if query_chat_type is not None else None,
                    thread_id=str(query_thread_id) if query_thread_id is not None else None,
                    user_name=query_user_name,
                ):
                    await query.answer(text="⛔ You are not authorized to answer this prompt.")
                    return

                session_key = self._slash_confirm_state.pop(confirm_id, None)
                if not session_key:
                    await query.answer(text="This prompt has already been resolved.")
                    return

                label_map = {
                    "once": "✅ Approved once",
                    "always": "🔒 Always approve",
                    "cancel": "❌ Cancelled",
                }
                user_display = getattr(query.from_user, "first_name", "User")
                label = label_map.get(choice, "Resolved")

                await query.answer(text=label)

                try:
                    await query.edit_message_text(
                        text=self.format_message(f"{label} by {user_display}"),
                        parse_mode=ParseMode.MARKDOWN_V2,
                        reply_markup=None,
                    )
                except Exception:
                    pass

                # Resolve via the module-level primitive.  The runner stored
                # a handler keyed by session_key; we run it on the event
                # loop and (if it returns a string) send it as a follow-up
                # message in the same chat.
                try:
                    from tools import slash_confirm as _slash_confirm_mod
                    result_text = await _slash_confirm_mod.resolve(
                        session_key, confirm_id, choice,
                    )
                    if result_text and query.message:
                        # Inherit the prompt message's topic. Supergroup forums
                        # use message_thread_id; Telegram private DM-topic lanes
                        # need both the private topic id and the prompt reply anchor.
                        thread_id = getattr(query.message, "message_thread_id", None)
                        chat = getattr(query.message, "chat", None)
                        chat_type = getattr(chat, "type", None)
                        prompt_message_id = getattr(query.message, "message_id", None)
                        send_kwargs: Dict[str, Any] = {
                            "chat_id": int(query.message.chat_id),
                            "text": self.format_message(result_text),
                            "parse_mode": ParseMode.MARKDOWN_V2,
                            **self._link_preview_kwargs(),
                        }
                        chat_type_value = getattr(chat_type, "value", chat_type)
                        is_private_chat = str(chat_type_value).lower() in {
                            "private",
                            str(ChatType.PRIVATE).lower(),
                            str(getattr(ChatType.PRIVATE, "value", ChatType.PRIVATE)).lower(),
                        }
                        if thread_id is not None and is_private_chat and prompt_message_id is not None:
                            reply_to_id = int(prompt_message_id)
                            send_kwargs["reply_to_message_id"] = reply_to_id
                            send_kwargs.update(
                                self._thread_kwargs_for_send(
                                    str(query.message.chat_id),
                                    str(thread_id),
                                    {
                                        "thread_id": str(thread_id),
                                        "telegram_dm_topic_reply_fallback": True,
                                    },
                                    reply_to_message_id=reply_to_id,
                                    reply_to_mode=self._reply_to_mode
                                )
                            )
                        elif thread_id is not None:
                            send_kwargs.update(
                                self._thread_kwargs_for_send(
                                    str(query.message.chat_id),
                                    str(thread_id),
                                    {"thread_id": str(thread_id)},
                                    reply_to_mode=self._reply_to_mode
                                )
                            )
                        await self._send_message_with_thread_fallback(**send_kwargs)
                except Exception as exc:
                    logger.error("[%s] slash-confirm callback failed: %s", self.name, exc, exc_info=True)
            return

        # --- Clarify callbacks (cl:clarify_id:idx | cl:clarify_id:other) ---
        if data.startswith("cl:"):
            parts = data.split(":", 2)
            if len(parts) == 3:
                clarify_id = parts[1]
                choice_token = parts[2]

                caller_id = str(getattr(query.from_user, "id", ""))
                if not self._is_callback_user_authorized(
                    caller_id,
                    chat_id=query_chat_id,
                    chat_type=str(query_chat_type) if query_chat_type is not None else None,
                    thread_id=str(query_thread_id) if query_thread_id is not None else None,
                    user_name=query_user_name,
                ):
                    await query.answer(text="⛔ You are not authorized to answer this prompt.")
                    return

                session_key = self._clarify_state.get(clarify_id)
                if not session_key:
                    await query.answer(text="This prompt has already been resolved.")
                    return

                user_display = getattr(query.from_user, "first_name", "User")

                if choice_token == "other":
                    # Flip into text-capture mode and tell the user to type
                    # their answer.  The gateway's text-intercept will pick
                    # up the next message in this session and resolve the
                    # clarify.  Do NOT pop _clarify_state yet — we still
                    # need it if the user is slow to respond and the entry
                    # is cleared by something else.
                    flipped = False
                    try:
                        from tools.clarify_gateway import mark_awaiting_text
                        flipped = mark_awaiting_text(clarify_id)
                    except Exception as exc:
                        logger.warning("[%s] mark_awaiting_text failed: %s", self.name, exc)

                    if not flipped:
                        # Entry evicted (clarify_timeout) or gateway restarted
                        # between ask and tap — a typed answer would go nowhere.
                        self._clarify_state.pop(clarify_id, None)
                        await self._notify_clarify_expired(query, user_display)
                        return

                    await query.answer(text="✏️ Type your answer in the chat.")
                    try:
                        await query.edit_message_text(
                            text=f"❓ {query.message.text or ''}\n\n<i>Awaiting typed response from {_html.escape(user_display)}…</i>",
                            parse_mode=ParseMode.HTML,
                            reply_markup=None,
                        )
                    except Exception:
                        pass
                    return

                # Numeric choice → resolve immediately with the chosen text
                try:
                    idx = int(choice_token)
                except (ValueError, TypeError):
                    await query.answer(text="Invalid choice.")
                    return

                # Look up the choice text from the entry registered in the
                # clarify primitive.  Fall back to the index if the entry
                # has been cleaned up (race with timeout / session reset).
                resolved_text: Optional[str] = None
                try:
                    from tools.clarify_gateway import _entries as _clarify_entries  # type: ignore
                    entry = _clarify_entries.get(clarify_id)
                    if entry and entry.choices and 0 <= idx < len(entry.choices):
                        resolved_text = entry.choices[idx]
                except Exception:
                    resolved_text = None

                if resolved_text is None:
                    # Race: entry vanished. Echo the index as a number so
                    # the agent at least sees an intentional response
                    # rather than nothing.
                    resolved_text = f"choice {idx + 1}"

                # Pop state and resolve
                self._clarify_state.pop(clarify_id, None)
                try:
                    from tools.clarify_gateway import resolve_gateway_clarify
                    resolved = resolve_gateway_clarify(clarify_id, resolved_text)
                except Exception as exc:
                    logger.error("[%s] resolve_gateway_clarify failed: %s", self.name, exc)
                    resolved = False

                if resolved:
                    await query.answer(text=f"✓ {resolved_text[:60]}")
                    try:
                        await query.edit_message_text(
                            text=f"❓ {_html.escape(query.message.text or '')}\n\n<b>{_html.escape(user_display)}:</b> {_html.escape(resolved_text)}",
                            parse_mode=ParseMode.HTML,
                            reply_markup=None,
                        )
                    except Exception:
                        pass
                    logger.info(
                        "Telegram clarify button resolved (id=%s, choice=%r, user=%s)",
                        clarify_id, resolved_text, user_display,
                    )
                else:
                    # Entry evicted (clarify_timeout) or gateway restarted
                    # between ask and tap — surface this instead of leaving a
                    # misleading ✓ on a button the agent will never receive.
                    await self._notify_clarify_expired(query, user_display)
                    logger.warning(
                        "Telegram clarify button: resolve_gateway_clarify returned False (id=%s)",
                        clarify_id,
                    )
            return

        # --- Update prompt callbacks ---
        if not data.startswith("update_prompt:"):
            return
        answer = data.split(":", 1)[1]  # "y" or "n"
        caller_id = str(getattr(query.from_user, "id", ""))
        if not self._is_callback_user_authorized(
            caller_id,
            chat_id=query_chat_id,
            chat_type=str(query_chat_type) if query_chat_type is not None else None,
            thread_id=str(query_thread_id) if query_thread_id is not None else None,
            user_name=query_user_name,
        ):
            await query.answer(text="⛔ You are not authorized to answer update prompts.")
            return
        await query.answer(text=f"Sent '{answer}' to the update process.")
        # Edit the message to show the choice and remove buttons
        label = "Yes" if answer == "y" else "No"
        try:
            await query.edit_message_text(
                text=self.format_message(f"⚕ Update prompt answered: *{label}*"),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=None,
            )
        except Exception:
            pass  # non-fatal if edit fails
        # Write the response file
        try:
            from hermes_constants import get_hermes_home
            home = get_hermes_home()
            response_path = home / ".update_response"
            tmp = response_path.with_suffix(".tmp")
            tmp.write_text(answer, encoding="utf-8")
            tmp.replace(response_path)
            logger.info("Telegram update prompt answered '%s' by user %s",
                        answer, getattr(query.from_user, "id", "unknown"))
        except Exception as exc:
            logger.error("Failed to write update response from callback: %s", exc)

    async def _handle_gmail_triage_callback(
        self,
        query,
        data: str,
        *,
        query_chat_id,
        query_chat_type,
        query_thread_id,
        query_user_name,
    ) -> None:
        """Dispatch a gmail-triage inline-button callback (gt:verb:arg)."""
        parts = data.split(":", 2)
        if len(parts) != 3:
            await query.answer(text="Invalid gmail-triage data.")
            return
        verb, arg = parts[1], parts[2]

        caller_id = str(getattr(query.from_user, "id", ""))
        if not self._is_callback_user_authorized(
            caller_id,
            chat_id=query_chat_id,
            chat_type=str(query_chat_type) if query_chat_type is not None else None,
            thread_id=str(query_thread_id) if query_thread_id is not None else None,
            user_name=query_user_name,
        ):
            await query.answer(text="⛔ You are not authorized to act on this email.")
            return

        entry = self._GT_VERB_DISPATCH.get(verb)
        if not entry:
            await query.answer(text=f"Unknown verb: {verb}")
            return
        script_name, extra_args, success_label, is_state_verb = entry

        script_path = _Path.home() / ".hermes" / "scripts" / "gmail-triage" / script_name
        if not script_path.exists():
            await query.answer(text=f"❌ {script_name} missing")
            logger.error("[%s] gmail-triage script missing: %s", self.name, script_path)
            return

        cmd = [str(script_path), arg, *extra_args]
        success = False
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(), timeout=60,
            )
            if proc.returncode == 0:
                label = success_label
                success = True
                logger.info(
                    "[%s] gmail-triage callback ok: verb=%s arg=%s",
                    self.name, verb, arg,
                )
            else:
                stderr_text = stderr_bytes.decode("utf-8", errors="replace").strip()
                last_line = stderr_text.splitlines()[-1] if stderr_text else f"exit {proc.returncode}"
                label = f"❌ {verb} failed: {last_line[:80]}"
                logger.error(
                    "[%s] gmail-triage callback failed: verb=%s arg=%s rc=%s stderr=%s",
                    self.name, verb, arg, proc.returncode, stderr_text,
                )
        except asyncio.TimeoutError:
            label = f"❌ {verb} timed out"
            logger.error("[%s] gmail-triage callback timed out: verb=%s arg=%s", self.name, verb, arg)
        except Exception as exc:
            label = f"❌ {verb} error: {exc}"
            logger.error(
                "[%s] gmail-triage callback exception: verb=%s arg=%s err=%s",
                self.name, verb, arg, exc, exc_info=True,
            )

        await query.answer(text=label)
        if not success:
            return

        user_display = getattr(query.from_user, "first_name", "User")
        original_text = (query.message.text or "") if query.message else ""
        appended = f"{original_text}\n— {label} by {user_display}"
        try:
            if is_state_verb:
                # Sticky state change: append confirmation, KEEP keyboard so
                # the user can stack further actions on this email.
                await query.edit_message_text(text=appended)
            else:
                # Per-email one-shot: strip keyboard so the action can't fire twice.
                await query.edit_message_text(text=appended, reply_markup=None)
        except Exception:
            pass
