"""S1 cluster mixin: TelegramAuthzMixin."""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Any
from gateway.authz_mixin import _coerce_allow_set
from gateway.config import Platform, PlatformConfig

from ..adapter import (
    Message,
    _scoped_gate_env,
    logger,
)

class TelegramAuthzMixin:
    def _is_callback_user_authorized(
        self,
        user_id: str,
        *,
        chat_id: Optional[str] = None,
        chat_type: Optional[str] = None,
        thread_id: Optional[str] = None,
        user_name: Optional[str] = None,
    ) -> bool:
        """Return whether a Telegram inline-button caller may perform gated actions."""
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            return False

        runner = getattr(getattr(self, "_message_handler", None), "__self__", None)
        auth_fn = getattr(runner, "_is_user_authorized", None)
        if callable(auth_fn):
            try:
                from gateway.session import SessionSource

                normalized_chat_type = str(chat_type or "dm").strip().lower() or "dm"
                if normalized_chat_type == "private":
                    normalized_chat_type = "dm"
                elif normalized_chat_type == "supergroup":
                    normalized_chat_type = "forum" if thread_id is not None else "group"

                source = SessionSource(
                    platform=Platform.TELEGRAM,
                    chat_id=str(chat_id or normalized_user_id),
                    chat_type=normalized_chat_type,
                    user_id=normalized_user_id,
                    user_name=str(user_name).strip() if user_name else None,
                    thread_id=str(thread_id) if thread_id is not None else None,
                )
                return bool(auth_fn(source))
            except Exception:
                logger.debug(
                    "[Telegram] Falling back to env-only callback auth for user %s",
                    normalized_user_id,
                    exc_info=True,
                )

        allowed_csv = _scoped_gate_env("TELEGRAM_ALLOWED_USERS").strip()
        if not allowed_csv:
            # Fail-closed: no allowlist means deny by default.
            # The runner auth path in _is_user_authorized() handles
            # GATEWAY_ALLOW_ALL_USERS; this fallback must not silently
            # allow everyone (fixes #24457).
            return _scoped_gate_env("GATEWAY_ALLOW_ALL_USERS").lower() in {"true", "1", "yes"}
        allowed_ids = {uid.strip() for uid in allowed_csv.split(",") if uid.strip()}
        return "*" in allowed_ids or normalized_user_id in allowed_ids

    def _source_from_message_for_auth(self, message: Message):
        """Build the same Telegram source shape the gateway auth path expects.

        Resolves the identity to authorize from ``from_user`` for normal
        messages, falling back to ``sender_chat`` for channel posts (which
        carry no ``from_user``) so a removed/unauthorized channel cannot
        inject content via the broadcast path either.
        """
        from gateway.session import SessionSource

        user = getattr(message, "from_user", None)
        chat = getattr(message, "chat", None)
        user_id = str(getattr(user, "id", "")).strip() or None
        user_name = (
            str(getattr(user, "username", "") or getattr(user, "full_name", "") or "").strip()
            or None
        )
        # Channel posts have no from_user — authorize the sender chat instead.
        if not user_id:
            sender_chat = getattr(message, "sender_chat", None)
            if sender_chat is not None:
                user_id = str(getattr(sender_chat, "id", "")).strip() or None
                if not user_name:
                    user_name = (
                        str(getattr(sender_chat, "title", "") or "").strip() or None
                    )

        chat_id = str(getattr(chat, "id", "")).strip() or user_id
        chat_type = str(getattr(chat, "type", "dm")).strip().lower() or "dm"
        if chat_type == "private":
            chat_type = "dm"
        elif chat_type == "supergroup":
            thread_id_raw = getattr(message, "message_thread_id", None)
            is_topic_message = bool(getattr(message, "is_topic_message", False))
            is_forum_group = getattr(chat, "is_forum", False) is True
            chat_type = (
                "forum"
                if thread_id_raw is not None and (is_topic_message or is_forum_group)
                else "group"
            )

        thread_id = None
        thread_id_raw = getattr(message, "message_thread_id", None)
        if thread_id_raw is not None:
            is_topic_message = bool(getattr(message, "is_topic_message", False))
            is_forum_group = getattr(chat, "is_forum", False) is True
            if chat_type == "forum" and (is_topic_message or is_forum_group):
                thread_id = str(thread_id_raw)
            elif chat_type == "dm" and is_topic_message:
                thread_id = str(thread_id_raw)

        return SessionSource(
            platform=Platform.TELEGRAM,
            chat_id=chat_id or "",
            chat_type=chat_type,
            user_id=user_id,
            user_name=user_name,
            thread_id=thread_id,
        )

    def _telegram_auth_env_configured(self) -> bool:
        """Return True when Telegram auth env vars make an early decision safe."""
        keys = (
            "TELEGRAM_ALLOWED_USERS",
            "TELEGRAM_GROUP_ALLOWED_USERS",
            "TELEGRAM_GROUP_ALLOWED_CHATS",
            "TELEGRAM_ALLOW_ALL_USERS",
            "GATEWAY_ALLOWED_USERS",
            "GATEWAY_ALLOW_ALL_USERS",
        )
        return any(_scoped_gate_env(key).strip() for key in keys)

    def _should_pass_unauthorized_dm_for_pairing(self, source) -> bool:
        """Return True when an unauthorized DM must still reach gateway pairing.

        Early auth (#40863) rejects before event construction. That is correct
        when unauthorized DMs are ignored, but it must not short-circuit the
        gateway pairing handshake when ``unauthorized_dm_behavior`` resolves
        to ``pair`` — including the case where an allowlist is set and the
        operator explicitly opted back into pairing via a platform override
        (resolution rule 1 in ``_get_unauthorized_dm_behavior``).
        """
        if source.chat_type != "dm":
            return False

        runner = getattr(getattr(self, "_message_handler", None), "__self__", None)
        behavior_fn = getattr(runner, "_get_unauthorized_dm_behavior", None)
        if callable(behavior_fn):
            try:
                return (
                    behavior_fn(
                        Platform.TELEGRAM,
                        profile=getattr(source, "profile", None),
                    )
                    == "pair"
                )
            except Exception:
                logger.debug(
                    "[Telegram] Failed to resolve unauthorized DM behavior; "
                    "falling back to adapter-local override",
                    exc_info=True,
                )

        extra = getattr(getattr(self, "config", None), "extra", None) or {}
        return str(extra.get("unauthorized_dm_behavior", "")).strip().lower() == "pair"

    def _is_user_authorized_from_message(self, message: Message) -> bool:
        """Check if the sender of a Telegram message is authorized.

        Intake prefilter that runs BEFORE text batching, event construction,
        and unmentioned-group observation, so a removed/unauthorized user
        cannot inject prompt content into the agent path or the observed
        transcript (fixes #40863). It only rejects when it can make the same
        context-aware decision the runner would make. Unknown DMs with no
        allowlist still pass through so the normal pairing flow can run.
        Unknown DMs with an allowlist still pass through when pairing is the
        effective unauthorized-DM behavior (explicit platform override).
        """
        source = self._source_from_message_for_auth(message)
        user_id = source.user_id
        # No identity at all → genuine group service message (pin, delete,
        # new_chat_members, etc.). Defer to the cold path. Channel posts
        # without sender_chat already resolved to None above and fall here;
        # they carry no authorizable identity, so let the normal
        # _should_process_message gating handle them.
        if not user_id:
            return True

        authorized: Optional[bool] = None

        # Adapter-level allow_from / group_allow_from: when set, they are the
        # sole authority.  Group chats use group_allow_from; DMs use allow_from.
        chat_type = source.chat_type or ""
        if chat_type in ("group", "forum", "channel"):
            adapter_allow_from = self.config.extra.get("group_allow_from")
        else:
            adapter_allow_from = self.config.extra.get("allow_from")
        if adapter_allow_from is not None:
            allowed = _coerce_allow_set(adapter_allow_from)
            authorized = user_id in allowed or "*" in allowed

        # Test/custom injection only. The class method named
        # _is_callback_user_authorized is for inline button callbacks and must
        # not be treated as a user-id-only shortcut for real messages — only
        # honor an instance-level override (set in tests).
        if authorized is None:
            callback_auth = self.__dict__.get("_is_callback_user_authorized")
            if callable(callback_auth):
                try:
                    authorized = bool(
                        callback_auth(
                            user_id,
                            chat_id=source.chat_id,
                            chat_type=source.chat_type,
                            thread_id=source.thread_id,
                            user_name=source.user_name,
                        )
                    )
                except Exception:
                    pass

        if authorized is None:
            runner = getattr(getattr(self, "_message_handler", None), "__self__", None)
            auth_fn = getattr(runner, "_is_user_authorized", None)
            if callable(auth_fn):
                # Only make an early decision via the runner when an allowlist
                # actually exists; otherwise unknown DMs must reach the pairing
                # flow rather than being default-denied here.
                if not self._telegram_auth_env_configured():
                    return True
                try:
                    authorized = bool(auth_fn(source))
                except Exception:
                    logger.debug(
                        "[Telegram] Falling back to env-only auth for user %s",
                        user_id,
                        exc_info=True,
                    )

        if authorized is None:
            allowed_csv = _scoped_gate_env("TELEGRAM_ALLOWED_USERS").strip()
            if not allowed_csv:
                return True
            allowed_ids = {uid.strip() for uid in allowed_csv.split(",") if uid.strip()}
            authorized = "*" in allowed_ids or user_id in allowed_ids

        if authorized:
            return True
        # Unauthorized DM that the gateway would pair: forward so pairing can run.
        return self._should_pass_unauthorized_dm_for_pairing(source)
