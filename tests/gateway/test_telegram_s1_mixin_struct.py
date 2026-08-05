"""Structural regression tests for the s1 cluster mixin extraction (#78791).

After the Telegram adapter god-file was split into 13 S1 mixin modules, the
behavioral tests in this directory exercise the moved methods end-to-end.
These tests pin the *structure* so the extraction cannot silently regress:

- all 13 S1 mixins must stay on ``TelegramAdapter.__mro__`` ahead of
  ``BasePlatformAdapter``;
- every method a mixin defines must remain resolvable on ``TelegramAdapter``
  (nothing lost in the move);
- no mixin method may be re-added (shadowed) back onto the adapter class
  itself — that would defeat the extraction and let the two copies drift.
"""

import sys
from unittest.mock import MagicMock

import pytest

from gateway.config import PlatformConfig


def _ensure_telegram_mock():
    if "telegram" in sys.modules and hasattr(sys.modules["telegram"], "__file__"):
        return
    mod = MagicMock()
    mod.error.NetworkError = type("NetworkError", (OSError,), {})
    mod.error.TimedOut = type("TimedOut", (OSError,), {})
    mod.error.BadRequest = type("BadRequest", (Exception,), {})
    for name in ("telegram", "telegram.ext", "telegram.constants", "telegram.request"):
        sys.modules.setdefault(name, mod)
    sys.modules.setdefault("telegram.error", mod.error)


_ensure_telegram_mock()

from gateway.platforms.base import BasePlatformAdapter  # noqa: E402
from plugins.platforms.telegram import mixins as _mixins  # noqa: E402
from plugins.platforms.telegram.adapter import TelegramAdapter  # noqa: E402

MIXIN_CLASS_NAMES = [
    "TelegramInitGuardsMixin",
    "TelegramConfigMixin",
    "TelegramAuthzMixin",
    "TelegramTopicsMixin",
    "TelegramNetworkMixin",
    "TelegramRichMixin",
    "TelegramTextFormatMixin",
    "TelegramMessagingMixin",
    "TelegramInteractiveMixin",
    "TelegramMediaMixin",
    "TelegramTypingMixin",
    "TelegramIngestMixin",
    "TelegramReactionsMixin",
]


def _mixin_classes():
    return {name: getattr(_mixins, name) for name in MIXIN_CLASS_NAMES}


def _is_mixin_own_method(value):
    """A member counts as a moved method if it is callable or a descriptor."""
    return callable(value) or isinstance(value, (property, staticmethod, classmethod))


def test_all_13_s1_mixins_in_mro_before_base():
    """Every S1 mixin must be in the MRO and sit ahead of the base adapter."""
    mro_names = [c.__name__ for c in TelegramAdapter.__mro__]
    assert BasePlatformAdapter in TelegramAdapter.__mro__, (
        "BasePlatformAdapter missing from TelegramAdapter MRO"
    )
    base_index = TelegramAdapter.__mro__.index(BasePlatformAdapter)
    for name in MIXIN_CLASS_NAMES:
        assert name in mro_names, f"{name} missing from TelegramAdapter.__mro__"
        idx = TelegramAdapter.__mro__.index(getattr(_mixins, name))
        assert idx < base_index, (
            f"{name} must sit before BasePlatformAdapter in the MRO "
            f"(found at index {idx}, base at {base_index})"
        )


def test_every_mixin_method_still_resolvable_and_not_shadowed():
    """All mixin-provided members resolve on the adapter and are not re-copied.

    This is the invariant that guards against someone "simplifying" the
    refactor by pasting methods back onto TelegramAdapter — a re-copied method
    would shadow the mixin version and the two copies could silently drift.
    """
    adapter_own = set(TelegramAdapter.__dict__.keys())
    for mname, mcls in _mixin_classes().items():
        for attr, value in mcls.__dict__.items():
            if attr.startswith("__") or not _is_mixin_own_method(value):
                continue
            assert hasattr(TelegramAdapter, attr), (
                f"{mname}.{attr} is no longer resolvable on TelegramAdapter "
                f"after the mixin extraction"
            )
            assert attr not in adapter_own, (
                f"{attr} was re-added to TelegramAdapter.__dict__, shadowing "
                f"the copy in {mname}"
            )


@pytest.mark.parametrize(
    "method_name,expected_mixin",
    [
        # connect() / lifecycle → s1_network
        ("connect", "TelegramNetworkMixin"),
        ("disconnect", "TelegramNetworkMixin"),
        ("_handle_polling_conflict", "TelegramNetworkMixin"),
        # outbound media → s1_media
        ("send_voice", "TelegramMediaMixin"),
        # interactive callbacks → s1_interactive
        ("send_exec_approval", "TelegramInteractiveMixin"),
        # inbound media handling → s1_ingest
        ("_handle_media_message", "TelegramIngestMixin"),
    ],
)
def test_representative_moved_method_resolves_through_mixin(method_name, expected_mixin):
    """Spot-check that key moved methods resolve from the owning mixin, not a
    shadowed adapter copy or the base class."""
    fn = getattr(TelegramAdapter, method_name)
    qualname = fn.__qualname__
    owner = qualname.split(".")[0]
    assert owner == expected_mixin, (
        f"{method_name} resolves from {owner!r}, expected {expected_mixin!r} "
        f"({qualname})"
    )
    # The method must be reachable on a live adapter instance too.
    adapter = TelegramAdapter(PlatformConfig(enabled=True, token="***"))
    assert callable(getattr(adapter, method_name)), method_name
