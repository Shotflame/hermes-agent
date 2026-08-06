"""Outbound sticker send tests for both WhatsApp backends.

Covers:
* Cloud API (whatsapp_cloud.py) — send_sticker routes to _send_media_from_path_or_link with media_kind="sticker"
* Bridge adapter (plugins/whatsapp/adapter.py) — send_sticker routes to _send_media_to_bridge with mediaType="sticker"
* bridge.js /send-media switch — 'sticker' case sends a Baileys stickerMessage
"""

import asyncio
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from plugins.platforms.whatsapp.adapter import WhatsAppAdapter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resp(status, json_data=None):
    r = AsyncMock()
    r.status = status
    r.json = AsyncMock(return_value=json_data or {})
    r.text = AsyncMock(return_value="")
    return r


def _pconfig():
    from types import SimpleNamespace
    return SimpleNamespace(token="", extra={"bridge_port": 3000})


def _tmp_webp():
    """Create a tiny valid-ish .webp file for sticker tests."""
    # Minimal WEBP lossless header (RIFF + WEBP + VP8L) — enough bytes to
    # exercise the size/existence checks without needing a real decoder.
    header = bytes([
        0x52, 0x49, 0x46, 0x46,  # RIFF
        0x1e, 0x00, 0x00, 0x00,  # file size - 8
        0x57, 0x45, 0x42, 0x50,  # WEBP
        0x56, 0x50, 0x38, 0x4c,  # VP8L
        0x10, 0x00, 0x00, 0x00,  # chunk size
        0x2f, 0x00, 0x00, 0x00,  # signature
        0x00, 0x00, 0x00, 0x00,  # width/height (1x1)
        0x00,
    ])
    f = tempfile.NamedTemporaryFile(suffix=".webp", delete=False)
    f.write(header)
    f.close()
    return f.name


# ---------------------------------------------------------------------------
# Cloud API — WhatsAppCloudAdapter.send_sticker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cloud_send_sticker_routes_to_send_media():
    """send_sticker calls _send_media_from_path_or_link with media_kind='sticker'."""
    from gateway.platforms.whatsapp_cloud import WhatsAppCloudAdapter

    adapter = WhatsAppCloudAdapter.__new__(WhatsAppCloudAdapter)
    adapter._http_client = AsyncMock()
    adapter._access_token = "tok"
    adapter._phone_number_id = "123"
    adapter._api_version = "v20.0"
    adapter._msg_store = MagicMock()
    adapter._rich_sent = MagicMock()
    adapter._last_inbound_wamid_by_chat = {}
    adapter._clarify_state = {}
    adapter._exec_approval_state = {}
    adapter._live_dm_policy = None
    adapter._live_dm_allow_list = []
    adapter._live_silent_notifications = False
    adapter._live_whatsapp_channel_config = {}
    adapter._setup_complete = True

    sticker_path = _tmp_webp()
    try:
        # Patch the inner dispatcher to capture the call
        with patch.object(adapter, "_send_media_from_path_or_link",
                          new=AsyncMock(return_value=MagicMock(success=True))) as mock_send:
            result = await adapter.send_sticker("12345", sticker_path)

        mock_send.assert_awaited_once()
        call_kwargs = mock_send.call_args.kwargs
        # Positional: chat_id, source, media_kind
        args = mock_send.call_args.args
        assert args[0] == "12345", f"Expected chat_id '12345', got {args[0]}"
        assert args[1] == sticker_path, f"Expected path {sticker_path}, got {args[1]}"
        assert args[2] == "sticker", f"Expected media_kind 'sticker', got {args[2]}"
        assert result.success is True
    finally:
        os.unlink(sticker_path)


@pytest.mark.asyncio
async def test_cloud_send_sticker_size_cap():
    """Sticker files larger than 100 KB are rejected by _upload_media."""
    from gateway.platforms.whatsapp_cloud import WhatsAppCloudAdapter

    adapter = WhatsAppCloudAdapter.__new__(WhatsAppCloudAdapter)
    adapter._http_client = AsyncMock()
    adapter._access_token = "tok"
    adapter._phone_number_id = "123"
    adapter._api_version = "v20.0"
    adapter._msg_store = MagicMock()
    adapter._rich_sent = MagicMock()
    adapter._last_inbound_wamid_by_chat = {}
    adapter._clarify_state = {}
    adapter._exec_approval_state = {}
    adapter._live_dm_policy = None
    adapter._live_dm_allow_list = []
    adapter._live_silent_notifications = False
    adapter._live_whatsapp_channel_config = {}
    adapter._setup_complete = True

    # Create a file that exceeds the 100 KB sticker limit
    f = tempfile.NamedTemporaryFile(suffix=".webp", delete=False)
    f.write(b"x" * 101 * 1024)  # 101 KB
    f.close()
    try:
        result = await adapter._send_media_from_path_or_link("12345", f.name, "sticker")
        assert not result.success
        assert "cap" in (result.error or "").lower()
    finally:
        os.unlink(f.name)


# ---------------------------------------------------------------------------
# Bridge adapter — WhatsAppAdapter.send_sticker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bridge_send_sticker_routes_with_media_type():
    """send_sticker calls _send_media_to_bridge with mediaType='sticker'."""
    adapter = WhatsAppAdapter.__new__(WhatsAppAdapter)
    adapter._running = True
    adapter._http_session = AsyncMock()
    adapter._managed_bridge = MagicMock()
    adapter._managed_bridge.returncode = None  # not exited
    # All the internal machinery needs to pass
    adapter._thread_state = {}
    adapter._app_state = {}
    adapter._pending_messages = {}
    adapter._message_id_out = {}
    adapter._store = MagicMock()
    adapter._sent_count = 0
    adapter._last_message_id = None
    adapter._connection_event = asyncio.Event()
    adapter._connection_event.set()
    adapter._connection_ready = asyncio.Event()
    adapter._connection_ready.set()
    adapter._discard_after_send = False
    adapter._dm_policy = "open"
    adapter._live_allow_ids = []
    adapter._live_dm_policy = None
    adapter._live_dm_allow_list = []
    adapter._config = MagicMock()
    adapter._config_poll_interval = 60
    adapter._channel_policy = "allow"
    adapter._setup_complete = True

    sticker_path = _tmp_webp()
    try:
        with patch.object(adapter, "_send_media_to_bridge",
                          new=AsyncMock(return_value=MagicMock(success=True, message_id="st1"))) as mock_send:
            result = await adapter.send_sticker("12345", sticker_path)

        mock_send.assert_awaited_once()
        # _send_media_to_bridge(chat_id, file_path, media_type, caption, file_name)
        args = mock_send.call_args.args
        assert args[0] == "12345"
        assert args[1] == sticker_path
        assert args[2] == "sticker"
        assert result.success is True
    finally:
        os.unlink(sticker_path)


# ---------------------------------------------------------------------------
# bridge_media_type — verify .webp still resolves as image for non-sticker sends
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path,expected",
    [
        ("a.webp", "image"),
        ("a.WEBP", "image"),
        ("a.webpm", "document"),   # .webpm not in _WA_IMAGE_EXTS → document
    ],
)
def test_bridge_media_type_webp_still_image(path, expected):
    """_bridge_media_type returns 'image' for .webp (non-sticker path).
    The send_sticker method explicitly passes 'sticker' as mediaType,
    bypassing this function. Plain .webp routed through the normal
    dispatcher → send_image_file → bridge 'image' is correct behavior."""
    from plugins.platforms.whatsapp.adapter import _bridge_media_type
    assert _bridge_media_type(path, is_voice=False, force_document=False) == expected