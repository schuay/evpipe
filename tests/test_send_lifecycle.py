"""Tests for the sender's self-termination on host suspend and for honest
grab reporting. No real devices: sources are stand-ins."""
from __future__ import annotations

import asyncio
import errno
import io
import logging

import pytest

from evpipe import wire
from evpipe.send import SenderApp, SuspendDetector


class _Clocks:
    def __init__(self) -> None:
        self.mono = 100.0
        self.boot = 500.0

    def tick(self, awake: float, asleep: float = 0.0) -> None:
        self.mono += awake
        self.boot += awake + asleep


def test_suspend_detector_measures_only_sleep():
    c = _Clocks()
    d = SuspendDetector(monotonic=lambda: c.mono, boottime=lambda: c.boot)
    c.tick(awake=0.5)
    assert d.slept() == pytest.approx(0.0)
    c.tick(awake=0.5, asleep=42.0)
    assert d.slept() == pytest.approx(42.0)
    c.tick(awake=0.5)
    assert d.slept() == pytest.approx(0.0)


def _run_resync(app: SenderApp, ticks: int) -> None:
    async def go():
        task = asyncio.create_task(app._resync_loop())
        await asyncio.sleep(app.resync_interval_s * ticks)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    asyncio.run(go())


def _app(clocks: _Clocks) -> SenderApp:
    return SenderApp(
        [], toggle_chord_evdev=[], stdout=io.BytesIO(), resync_interval_s=0.01,
        suspend_exit_threshold_s=10.0,
        suspend_detector=SuspendDetector(monotonic=lambda: clocks.mono,
                                         boottime=lambda: clocks.boot),
    )


def test_resync_loop_exits_after_long_suspend(caplog):
    c = _Clocks()
    app = _app(c)
    c.tick(awake=0.01, asleep=30.0)
    with caplog.at_level(logging.WARNING, logger="evpipe-send"):
        _run_resync(app, ticks=3)
    assert app.shutting_down.is_set()
    assert "host suspended for ~30s" in caplog.text


def test_resync_loop_survives_short_suspend():
    c = _Clocks()
    app = _app(c)
    c.tick(awake=0.01, asleep=3.0)
    _run_resync(app, ticks=3)
    assert not app.shutting_down.is_set()


class _FakeDev:
    def __init__(self, fail_errno=None) -> None:
        self.fail_errno = fail_errno
        self.grabbed = False

    def grab(self) -> None:
        if self.fail_errno is not None:
            raise OSError(self.fail_errno, "boom")
        self.grabbed = True


class _FakeSource:
    def __init__(self, path: str, dev: _FakeDev) -> None:
        self.path = path
        self.dev = dev
        self.grabbed = False
        self.role = wire.DEV_KB


def test_grab_all_reports_partial_failure(caplog):
    app = SenderApp([], toggle_chord_evdev=[], stdout=io.BytesIO())
    ok_src = _FakeSource("/dev/input/event1", _FakeDev())
    busy_src = _FakeSource("/dev/input/event2", _FakeDev(errno.EBUSY))
    app.sources = [ok_src, busy_src]
    with caplog.at_level(logging.ERROR, logger="evpipe-send"):
        assert app._grab_all() is False
    assert ok_src.grabbed and not busy_src.grabbed
    assert "another process holds the grab" in caplog.text


def test_grab_all_succeeds_quietly(caplog):
    app = SenderApp([], toggle_chord_evdev=[], stdout=io.BytesIO())
    app.sources = [_FakeSource("/dev/input/event1", _FakeDev())]
    with caplog.at_level(logging.WARNING, logger="evpipe-send"):
        assert app._grab_all() is True
    assert caplog.text == ""
