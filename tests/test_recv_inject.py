"""Receiver injection, FULL_STATE convergence and held-key release, against
a recording uinput stand-in."""
from __future__ import annotations

import asyncio

import evdev.ecodes as e

from evpipe import hid_map, recv, wire
from evpipe.recv import ReceiverApp

from .fakes import FakeInputDevice, FakeUInput, SenderHarness, async_test

A, SHIFT = e.KEY_A, e.KEY_LEFTSHIFT
HID_A = hid_map.encode_key(A)
HID_SHIFT = hid_map.encode_key(SHIFT)


def _event(ev_kind: int, code: int = 0, value: int = 0, device_id: int = 0) -> bytes:
    return wire.encode_packet(
        wire.PACKET_EVENT, wire.encode_event(wire.Event(ev_kind, code, value, 0, device_id)))


def _full_state(held: list[int], device_id: int = 0) -> bytes:
    return wire.encode_packet(
        wire.PACKET_FULL_STATE, wire.encode_full_state(device_id, held))


class _Receiver:
    """A running ReceiverApp fed from memory, with one keyboard device."""

    def __init__(self) -> None:
        self.app = ReceiverApp(make_uinput=FakeUInput)
        self.reader = asyncio.StreamReader()
        desc = wire.DeviceDescriptor(
            name="kb", vendor_id=1, product_id=2, kind=wire.DEV_KB,
            keys=[HID_A, HID_SHIFT])
        self.reader.feed_data(wire.encode_session_open([desc]))
        self.task = asyncio.create_task(self.app.run(self.reader))

    async def feed(self, *packets: bytes) -> None:
        self.reader.feed_data(b"".join(packets))
        # The read loop consumes everything buffered in one step once woken.
        for _ in range(3):
            await asyncio.sleep(0)

    @property
    def ui(self) -> FakeUInput:
        return self.app.uinputs[0]


@async_test
async def test_events_are_injected_and_held_keys_released_on_eof():
    r = _Receiver()
    await r.feed(
        _event(wire.EV_KEY, HID_SHIFT, 1), _event(wire.EV_SYN),
        _event(wire.EV_KEY, HID_A, 1), _event(wire.EV_SYN),
        _event(wire.EV_KEY, HID_A, 0), _event(wire.EV_SYN),
    )
    ui = r.ui
    assert ui.name == "evpipe: kb"
    assert ui.caps == {e.EV_KEY: [A, SHIFT]}
    assert ui.key_events() == [(SHIFT, 1), (A, 1), (A, 0)]
    assert ui.syns == 3
    assert r.app.held_per_dev[0] == {HID_SHIFT}

    r.reader.feed_eof()
    assert await r.task == 0
    assert ui.key_events()[-1] == (SHIFT, 0)
    assert ui.closed


@async_test
async def test_full_state_converges_both_ways():
    r = _Receiver()
    await r.feed(_event(wire.EV_KEY, HID_A, 1), _event(wire.EV_SYN))
    # The sender says A is up and shift is down: the receiver follows.
    await r.feed(_full_state([HID_SHIFT]))
    assert r.ui.key_events() == [(A, 1), (A, 0), (SHIFT, 1)]
    assert r.app.held_per_dev[0] == {HID_SHIFT}
    # A matching FULL_STATE is a pure heartbeat.
    await r.feed(_full_state([HID_SHIFT]))
    assert r.ui.key_events() == [(A, 1), (A, 0), (SHIFT, 1)]
    r.app.shutdown()
    assert await r.task == 0


@async_test
async def test_packets_for_unknown_devices_and_kinds_are_ignored():
    r = _Receiver()
    await r.feed(
        _event(wire.EV_KEY, HID_A, 1, device_id=7),
        _full_state([HID_A], device_id=7),
        wire.encode_packet(99, b"junk"),
        _event(wire.EV_KEY, HID_A, 1),
    )
    assert r.ui.key_events() == [(A, 1)]
    r.app.shutdown()
    assert await r.task == 0


@async_test
async def test_silence_releases_held_keys_but_keeps_the_session(monkeypatch):
    monkeypatch.setattr(recv, "HEARTBEAT_SAFETY_RELEASE_S", 0.05)
    monkeypatch.setattr(recv, "HEARTBEAT_CHECK_S", 0.01)
    r = _Receiver()
    await r.feed(_event(wire.EV_KEY, HID_A, 1), _event(wire.EV_SYN))
    await asyncio.sleep(0.2)
    assert r.ui.key_events() == [(A, 1), (A, 0)]
    assert not r.app.shutting_down.is_set()
    assert not r.ui.closed
    # Traffic resumes: the sender still reports A held, so it is re-pressed.
    await r.feed(_full_state([HID_A]))
    assert r.ui.key_events() == [(A, 1), (A, 0), (A, 1)]
    r.app.shutdown()
    assert await r.task == 0


@async_test
async def test_sender_output_replays_on_the_receiver():
    kb = FakeInputDevice(keys=[A, SHIFT], name="src kb")
    async with SenderHarness(kb) as h:
        kb.press(SHIFT)
        kb.tap(A)
        await h.settle()
        await h.app._resync()

    made: list[FakeUInput] = []

    def make_uinput(*args, **kwargs) -> FakeUInput:
        made.append(FakeUInput(*args, **kwargs))
        return made[-1]

    app = ReceiverApp(make_uinput=make_uinput)
    reader = asyncio.StreamReader()
    reader.feed_data(h.out.getvalue())
    reader.feed_eof()
    assert await app.run(reader) == 0
    (ui,) = made
    assert ui.name == "evpipe: src kb"
    # Shift was still down when the stream ended: teardown releases it.
    assert ui.key_events() == [(SHIFT, 1), (A, 1), (A, 0), (SHIFT, 0)]
    assert ui.closed
