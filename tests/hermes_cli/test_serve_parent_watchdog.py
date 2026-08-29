"""Regression tests for Desktop-owned ``hermes serve`` lifecycle tracking."""

from hermes_cli.web_server import _is_serve_orphaned, _valid_parent_start_marker


def test_parent_watchdog_tracks_recorded_desktop_pid_not_immediate_ppid():
    """Windows venv launch shims must not make a live Desktop look orphaned."""

    assert _is_serve_orphaned(4242, pid_exists=lambda pid: pid == 4242) is False
    assert _is_serve_orphaned(4242, pid_exists=lambda _pid: False) is True


def test_parent_watchdog_fails_safe_when_liveness_probe_errors():
    def broken_probe(_pid: int) -> bool:
        raise OSError("process table temporarily unavailable")

    assert _is_serve_orphaned(4242, pid_exists=broken_probe) is False


def test_parent_watchdog_accepts_electron_windows_creation_time_marker():
    unix_ms = 1_723_456_789_123
    dotnet_ticks = 621_355_968_000_000_000 + unix_ms * 10_000 + 9_999

    assert _valid_parent_start_marker(f"winms:{unix_ms}") is True
    assert (
        _is_serve_orphaned(
            4242,
            f"winms:{unix_ms}",
            process_start_marker=lambda _pid: f"win:{dotnet_ticks}",
        )
        is False
    )


def test_parent_watchdog_rejects_reused_pid_with_different_windows_creation_time():
    unix_ms = 1_723_456_789_123
    next_process_ticks = 621_355_968_000_000_000 + (unix_ms + 1) * 10_000

    assert (
        _is_serve_orphaned(
            4242,
            f"winms:{unix_ms}",
            process_start_marker=lambda _pid: f"win:{next_process_ticks}",
        )
        is True
    )


def test_parent_watchdog_preserves_legacy_exact_windows_marker():
    marker = "win:638908765432109876"

    assert (
        _is_serve_orphaned(
            4242,
            marker,
            process_start_marker=lambda _pid: marker,
        )
        is False
    )


def test_process_start_marker_pins_utc_locale_for_macos_lstart(monkeypatch):
    """Regression for #93705: the macOS `ps:<lstart>` marker must not drift
    when the host timezone or locale changes between the Desktop's spawn-time
    stamp and the backend's poll-time re-derivation, or a HEALTHY backend is
    misread as orphaned and killed.

    We simulate a timezone change by driving the real ``_process_start_marker``
    under two different host ``TZ`` values and standing in for the ``ps``
    binary with a fake that renders ``lstart`` using the environment it is
    given (exactly what macOS ``ps`` does with localtime+strftime). With the
    fix the production code pins ``TZ=UTC``/``LC_ALL=C`` when it runs ``ps``,
    so both probes yield the identical marker and the live parent is not
    treated as orphaned. Without the pin the two renders differ and the same
    healthy process looks dead.
    """
    import os
    from types import SimpleNamespace

    from hermes_cli import web_server

    fake_epoch_s = 1_726_000_000

    def fake_ps_lstart(args, **kwargs):
        # Faithful macOS `ps`: render the fixed start moment as a naive local
        # wall-clock using the TZ + LC_ALL in the env passed to the child.
        # Without the fix the production code calls `ps` with NO env override,
        # so the child inherits the host TZ — reproducing the original drift.
        env = kwargs.get("env") or os.environ
        old_tz = os.environ.get("TZ")
        old_lc = os.environ.get("LC_ALL")
        os.environ["TZ"] = env.get("TZ", old_tz or "UTC")
        os.environ["LC_ALL"] = env.get("LC_ALL", old_lc or "C")
        try:
            import time

            try:
                time.tzset()
            except AttributeError:
                pass
            rendered = time.strftime("%a %b %d %H:%M:%S %Y", time.localtime(fake_epoch_s))
        finally:
            if old_tz is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = old_tz
            if old_lc is None:
                os.environ.pop("LC_ALL", None)
            else:
                os.environ["LC_ALL"] = old_lc
        return SimpleNamespace(returncode=0, stdout=rendered, stderr="")

    monkeypatch.setattr(web_server, "sys", SimpleNamespace(platform="darwin"))
    monkeypatch.setattr(web_server.subprocess, "run", fake_ps_lstart)

    # Same process start moment rendered under two host timezones produces
    # different naive strings on a non-pinned ps; the marker must not drift.
    def marker_under_tz(tz: str) -> str:
        os.environ["TZ"] = tz
        try:
            return web_server._process_start_marker(4242)
        finally:
            del os.environ["TZ"]

    spawn_marker = marker_under_tz("Asia/Tokyo")  # what Desktop stamped at spawn
    poll_marker = marker_under_tz("America/New_York")  # host TZ changed by the poll

    # The real assertion: the marker is timezone-invariant, so the decision
    # must keep the live parent alive.
    assert spawn_marker == poll_marker
    assert (
        _is_serve_orphaned(
            4242,
            spawn_marker,
            process_start_marker=lambda _pid: poll_marker,
        )
        is False
    )
