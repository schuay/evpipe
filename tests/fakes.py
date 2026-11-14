"""In-process stand-ins for evdev devices, so the sender and receiver logic
runs without /dev/input or /dev/uinput.

The fakes model only what evpipe relies on. Kernel behaviour itself (who
sees an event across EVIOCGRAB, what uinput does with a redundant press)
stays the business of the uinput-backed tests in test_loopback.py.
"""
from __future__ import annotations

import asyncio
import functools
import io
from types import SimpleNamespace

import evdev
import evdev.ecodes as e

from evpipe import hid_map, wire
from evpipe.send import SenderApp

TEST_TIMEOUT_S = 5.0


def async_test(fn):
    """Run an async test function to completion, bounded so a sender that
    fails to exit shows up as a failure instead of a hung suite."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(asyncio.wait_for(fn(*args, **kwargs), TEST_TIMEOUT_S))
    return wrapper


class FakeInputDevice:
    """Scripted evdev.InputDevice.

    `active_keys()` is the kernel's view: it reflects every event generated
    so far, including ones still queued for the sender. `seen_by_others`
    records the key events a second, non-grabbing reader of the device (the
    local compositor) receives, i.e. those generated while ungrabbed.
    """

    def __init__(self, path: str = "/dev/input/fake0", keys=(), rels=(),
                 held=(), name: str = "fake") -> None:
        self.path = path
        self.name = name
        self.info = SimpleNamespace(vendor=0xCAFE, product=0xBABE)
        self._caps = {e.EV_KEY: list(keys)}
        if rels:
            self._caps[e.EV_REL] = list(rels)
        self._active = set(held)
        self._queue: asyncio.Queue = asyncio.Queue()
        self.grabbed = False
        self.closed = False
        self.seen_by_others: list[tuple[int, int]] = []

    # The evdev.InputDevice surface the sender uses.

    def capabilities(self) -> dict[int, list[int]]:
        return self._caps

    def active_keys(self) -> list[int]:
        return sorted(self._active)

    def grab(self) -> None:
        self.grabbed = True

    def ungrab(self) -> None:
        self.grabbed = False

    def close(self) -> None:
        self.closed = True

    async def async_read_loop(self):
        while True:
            item = await self._queue.get()
            try:
                if isinstance(item, BaseException):
                    raise item
                yield item
            finally:
                # Reached when the consumer asks for the next event, i.e.
                # once it has fully dispatched this one. settle() waits on it.
                self._queue.task_done()

    # Test controls.

    def press(self, code: int) -> None:
        self._key(code, 1)

    def release(self, code: int) -> None:
        self._key(code, 0)

    def tap(self, code: int) -> None:
        self.press(code)
        self.release(code)

    def lose_release(self, code: int) -> None:
        """The key goes up but its event never reaches the sender, as after
        SYN_DROPPED."""
        self._active.discard(code)
        if not self.grabbed:
            self.seen_by_others.append((code, 0))

    def rel(self, code: int, value: int) -> None:
        self._emit(e.EV_REL, code, value)

    def fail(self, exc: BaseException) -> None:
        """Make the sender's read loop raise `exc` at this point in the stream."""
        self._queue.put_nowait(exc)

    async def settle(self) -> None:
        """Wait until the sender has dispatched every event generated so far."""
        await self._queue.join()

    def _key(self, code: int, value: int) -> None:
        if value:
            self._active.add(code)
        else:
            self._active.discard(code)
        if not self.grabbed:
            self.seen_by_others.append((code, value))
        self._emit(e.EV_KEY, code, value)

    def _emit(self, ev_type: int, code: int, value: int) -> None:
        self._queue.put_nowait(evdev.InputEvent(0, 0, ev_type, code, value))
        self._queue.put_nowait(evdev.InputEvent(0, 0, e.EV_SYN, e.SYN_REPORT, 0))


class FakeUInput:
    """Records what the receiver writes to a virtual device."""

    def __init__(self, events=None, name: str = "", **_kwargs) -> None:
        self.caps = events or {}
        self.name = name
        self.writes: list[tuple[int, int, int]] = []
        self.syns = 0
        self.closed = False

    def write(self, ev_type: int, code: int, value: int) -> None:
        assert not self.closed, "write to a destroyed uinput device"
        self.writes.append((ev_type, code, value))

    def syn(self) -> None:
        assert not self.closed, "syn on a destroyed uinput device"
        self.syns += 1

    def close(self) -> None:
        self.closed = True

    def key_events(self) -> list[tuple[int, int]]:
        return [(code, value) for t, code, value in self.writes if t == e.EV_KEY]


class SenderHarness:
    """A SenderApp reading FakeInputDevices and writing to memory.

    The periodic resync is parked by default; tests drive `app._resync()`
    themselves so nothing depends on wall-clock time.
    """

    def __init__(self, *devs: FakeInputDevice, stdout=None, **app_kwargs) -> None:
        self.devs = devs
        self.out = stdout if stdout is not None else io.BytesIO()
        by_path = {d.path: d for d in devs}
        assert len(by_path) == len(devs), "fake devices need distinct paths"
        app_kwargs.setdefault("toggle_chord_evdev", [])
        app_kwargs.setdefault("resync_interval_s", 3600.0)
        self.app = SenderApp(
            [(d.path, wire.DEV_MOUSE if e.EV_REL in d.capabilities() else wire.DEV_KB)
             for d in devs],
            stdout=self.out,
            open_device=by_path.__getitem__,
            **app_kwargs,
        )
        self.task: asyncio.Task | None = None

    async def start(self) -> None:
        self.task = asyncio.create_task(self.app.run())
        await asyncio.sleep(0)

    async def __aenter__(self) -> "SenderHarness":
        await self.start()
        return self

    async def __aexit__(self, *exc_info) -> None:
        self.app.shutdown()
        await self.task

    async def settle(self) -> None:
        for d in self.devs:
            await d.settle()

    def packets(self) -> list[tuple]:
        """Everything sent after session-open, decoded back to evdev codes:
        ("key", code, value), ("rel", code, value), ("syn",) or
        ("fs", device_id, [held codes])."""
        buf = io.BytesIO(self.out.getvalue())
        wire.read_session_open(buf)
        out: list[tuple] = []
        while True:
            try:
                kind, payload = wire.read_packet(buf)
            except EOFError:
                return out
            if kind == wire.PACKET_FULL_STATE:
                dev_id, held = wire.decode_full_state(payload)
                out.append(("fs", dev_id, sorted(hid_map.decode_key(h) for h in held)))
                continue
            ev = wire.decode_event(payload)
            if ev.ev_kind == wire.EV_KEY:
                out.append(("key", hid_map.decode_key(ev.code), ev.value))
            elif ev.ev_kind == wire.EV_REL:
                out.append(("rel", hid_map.decode_rel(ev.code), ev.value))
            elif ev.ev_kind == wire.EV_SYN:
                out.append(("syn",))

    def key_events(self) -> list[tuple[int, int]]:
        return [(p[1], p[2]) for p in self.packets() if p[0] == "key"]

    def full_states(self) -> list[list[int]]:
        return [p[2] for p in self.packets() if p[0] == "fs"]
