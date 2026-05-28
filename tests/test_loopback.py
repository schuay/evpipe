"""End-to-end loopback: synthetic uinput source -> sender -> pipe -> receiver
-> synthetic uinput sink. Verifies a kb chord (shift+a press/release)
survives the full pipeline byte-for-byte.

Mouse devices are intentionally not exercised here: the host's
qmk-mouse-bridge daemon auto-grabs anything that looks like a mouse
(REL_X + a button), which races our sender's grab. Mouse forwarding
is covered by the manual smoke test in README.

Needs /dev/uinput write access (membership in the `input` group on
most distros, or root). Skipped silently otherwise."""
from __future__ import annotations

import asyncio
import os

import evdev
import evdev.ecodes as e
import pytest

from evpipe import hid_map, wire
from evpipe.recv import ReceiverApp
from evpipe.send import SenderApp


def _can_uinput() -> bool:
    return os.access("/dev/uinput", os.W_OK)


pytestmark = pytest.mark.skipif(
    not _can_uinput(), reason="needs /dev/uinput write access"
)


def test_loopback_kb_chord():
    asyncio.run(_loopback())


async def _loopback() -> None:
    src_kb = evdev.UInput(
        {e.EV_KEY: [e.KEY_A, e.KEY_B, e.KEY_LEFTSHIFT]},
        name="evpipe-test-src-kb",
        vendor=0xCAFE, product=0xBAB1,
    )
    await asyncio.sleep(0.2)

    src_kb_path = src_kb.device.path

    rfd, wfd = os.pipe()
    sender_out = os.fdopen(wfd, "wb", buffering=0)
    recv_in = os.fdopen(rfd, "rb", buffering=0)

    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    proto = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: proto, recv_in)

    sender = SenderApp(
        [(src_kb_path, wire.DEV_KB)],
        toggle_chord_evdev=[],
        stdout=sender_out,
        resync_interval_s=0.2,
    )
    receiver = ReceiverApp()

    sender_task = asyncio.create_task(sender.run(), name="sender")
    receiver_task = asyncio.create_task(receiver.run(reader), name="receiver")

    for _ in range(40):
        if receiver.uinputs:
            break
        await asyncio.sleep(0.05)
    assert receiver.uinputs, "receiver never built uinput device"
    recv_kb = evdev.InputDevice(receiver.uinputs[0].device.path)
    await asyncio.sleep(0.1)

    src_kb.write(e.EV_KEY, e.KEY_LEFTSHIFT, 1); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_A, 1); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_A, 0); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_LEFTSHIFT, 0); src_kb.syn()

    kb_events = await _drain(recv_kb, 0.8)
    key_seq = [(ev.code, ev.value) for ev in kb_events if ev.type == e.EV_KEY]

    assert key_seq == [
        (e.KEY_LEFTSHIFT, 1),
        (e.KEY_A, 1),
        (e.KEY_A, 0),
        (e.KEY_LEFTSHIFT, 0),
    ], f"unexpected sequence: {key_seq}"

    # SYN_REPORTs should be interleaved.
    syn_count = sum(1 for ev in kb_events if ev.type == e.EV_SYN)
    assert syn_count >= 4, f"expected >=4 SYN_REPORTs, got {syn_count}"

    sender.shutdown()
    receiver.shutdown()
    for t in (sender_task, receiver_task):
        try:
            await asyncio.wait_for(t, timeout=1.0)
        except (asyncio.TimeoutError, Exception):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    src_kb.close()
    recv_kb.close()
    sender_out.close()


def test_loopback_state_resync_releases_stuck_key():
    """If the sender's resync says no keys are held but the receiver thinks
    one is, the receiver must release it on its own."""
    asyncio.run(_resync_releases_stuck())


async def _resync_releases_stuck() -> None:
    src_kb = evdev.UInput(
        {e.EV_KEY: [e.KEY_A]},
        name="evpipe-test-resync-src",
        vendor=0xCAFE, product=0xBAB3,
    )
    await asyncio.sleep(0.2)

    rfd, wfd = os.pipe()
    sender_out = os.fdopen(wfd, "wb", buffering=0)
    recv_in = os.fdopen(rfd, "rb", buffering=0)

    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    proto = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: proto, recv_in)

    sender = SenderApp(
        [(src_kb.device.path, wire.DEV_KB)],
        toggle_chord_evdev=[],
        stdout=sender_out,
        resync_interval_s=0.1,
    )
    receiver = ReceiverApp()
    sender_task = asyncio.create_task(sender.run())
    receiver_task = asyncio.create_task(receiver.run(reader))

    for _ in range(40):
        if receiver.uinputs:
            break
        await asyncio.sleep(0.05)
    assert receiver.uinputs

    # Forge a wedged "key A held" in the receiver's tracking. Then wait for
    # the sender's next resync to fire and force a release.
    hid_a = (0x07 << 8) | 0x04
    receiver.held_per_dev[0].add(hid_a)
    receiver.uinputs[0].write(e.EV_KEY, e.KEY_A, 1)
    receiver.uinputs[0].syn()

    recv_kb = evdev.InputDevice(receiver.uinputs[0].device.path)
    events = await _drain(recv_kb, 0.6)

    key_values = [(ev.code, ev.value) for ev in events if ev.type == e.EV_KEY]
    # The very first event is the forged press we wrote ourselves. The
    # resync converge must follow with a release.
    assert (e.KEY_A, 0) in key_values, f"resync did not release stuck key: {key_values}"
    assert hid_a not in receiver.held_per_dev[0]

    sender.shutdown()
    receiver.shutdown()
    for t in (sender_task, receiver_task):
        try:
            await asyncio.wait_for(t, timeout=1.0)
        except (asyncio.TimeoutError, Exception):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
    src_kb.close()
    recv_kb.close()
    sender_out.close()


def test_eof_releases_held_keys():
    asyncio.run(_eof_releases())


async def _eof_releases() -> None:
    src_kb = evdev.UInput(
        {e.EV_KEY: [e.KEY_A]},
        name="evpipe-test-eof-src",
        vendor=0xCAFE, product=0xBAB4,
    )
    await asyncio.sleep(0.2)

    rfd, wfd = os.pipe()
    sender_out = os.fdopen(wfd, "wb", buffering=0)
    recv_in = os.fdopen(rfd, "rb", buffering=0)
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    proto = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: proto, recv_in)

    sender = SenderApp(
        [(src_kb.device.path, wire.DEV_KB)],
        toggle_chord_evdev=[],
        stdout=sender_out,
        resync_interval_s=5.0,  # long, so EOF triggers teardown not resync
    )
    receiver = ReceiverApp()
    sender_task = asyncio.create_task(sender.run())
    receiver_task = asyncio.create_task(receiver.run(reader))

    for _ in range(40):
        if receiver.uinputs:
            break
        await asyncio.sleep(0.05)
    assert receiver.uinputs

    # Open the receiver's virtual device BEFORE injecting the press so its
    # async_read_loop has a queue to push into.
    recv_kb = evdev.InputDevice(receiver.uinputs[0].device.path)
    drain_task_out: list[evdev.InputEvent] = []

    async def collect() -> None:
        async for ev in recv_kb.async_read_loop():
            drain_task_out.append(ev)

    collector = asyncio.create_task(collect())

    src_kb.write(e.EV_KEY, e.KEY_A, 1); src_kb.syn()
    await asyncio.sleep(0.2)
    assert receiver.held_per_dev[0], "receiver should be tracking the held key"

    # Close sender's write end -> receiver EOF -> teardown -> all-up.
    sender_out.close()
    sender.shutdown()
    await asyncio.sleep(1.0)
    collector.cancel()
    try:
        await collector
    except (asyncio.CancelledError, Exception):
        pass

    key_values = [(ev.code, ev.value) for ev in drain_task_out if ev.type == e.EV_KEY]
    assert (e.KEY_A, 1) in key_values, f"press never reached recv: {key_values}"
    assert (e.KEY_A, 0) in key_values, f"all-up not emitted on EOF: {key_values}"

    for t in (sender_task, receiver_task):
        try:
            await asyncio.wait_for(t, timeout=1.5)
        except (asyncio.TimeoutError, Exception):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
    src_kb.close()
    recv_kb.close()


def test_toggle_chord_eats_trigger_and_flips_state():
    """Pressing the configured chord must (a) flip forwarding, (b) not
    leak the trigger key to the receiver."""
    asyncio.run(_chord_toggle())


async def _chord_toggle() -> None:
    src_kb = evdev.UInput(
        {e.EV_KEY: [
            e.KEY_A, e.KEY_T,
            e.KEY_LEFTCTRL, e.KEY_LEFTALT, e.KEY_LEFTSHIFT, e.KEY_LEFTMETA,
        ]},
        name="evpipe-test-chord-src",
        vendor=0xCAFE, product=0xBAB5,
    )
    await asyncio.sleep(0.2)

    rfd, wfd = os.pipe()
    sender_out = os.fdopen(wfd, "wb", buffering=0)
    recv_in = os.fdopen(rfd, "rb", buffering=0)
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    proto = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: proto, recv_in)

    chord = [e.KEY_LEFTCTRL, e.KEY_LEFTALT, e.KEY_LEFTSHIFT, e.KEY_LEFTMETA, e.KEY_T]
    sender = SenderApp(
        [(src_kb.device.path, wire.DEV_KB)],
        toggle_chord_evdev=chord,
        stdout=sender_out,
        resync_interval_s=5.0,
        start_forwarding=True,
    )
    receiver = ReceiverApp()
    sender_task = asyncio.create_task(sender.run())
    receiver_task = asyncio.create_task(receiver.run(reader))

    for _ in range(40):
        if receiver.uinputs:
            break
        await asyncio.sleep(0.05)
    assert receiver.uinputs

    recv_kb = evdev.InputDevice(receiver.uinputs[0].device.path)
    out: list[evdev.InputEvent] = []

    async def collect() -> None:
        async for ev in recv_kb.async_read_loop():
            out.append(ev)

    collector = asyncio.create_task(collect())

    # Phase 1: forwarding ON -- a plain KEY_A goes through.
    src_kb.write(e.EV_KEY, e.KEY_A, 1); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_A, 0); src_kb.syn()
    await asyncio.sleep(0.15)

    # Phase 2: press the chord -> forwarding flips OFF, trigger consumed.
    for code in (e.KEY_LEFTCTRL, e.KEY_LEFTALT, e.KEY_LEFTSHIFT, e.KEY_LEFTMETA):
        src_kb.write(e.EV_KEY, code, 1); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_T, 1); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_T, 0); src_kb.syn()
    for code in (e.KEY_LEFTMETA, e.KEY_LEFTSHIFT, e.KEY_LEFTALT, e.KEY_LEFTCTRL):
        src_kb.write(e.EV_KEY, code, 0); src_kb.syn()
    await asyncio.sleep(0.25)
    assert sender.forwarding_on is False, "chord did not flip forwarding off"

    # Phase 3: forwarding OFF -- plain KEY_A must NOT reach the receiver.
    src_kb.write(e.EV_KEY, e.KEY_A, 1); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_A, 0); src_kb.syn()
    await asyncio.sleep(0.15)

    # Phase 4: chord again -> forwarding ON.
    for code in (e.KEY_LEFTCTRL, e.KEY_LEFTALT, e.KEY_LEFTSHIFT, e.KEY_LEFTMETA):
        src_kb.write(e.EV_KEY, code, 1); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_T, 1); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_T, 0); src_kb.syn()
    for code in (e.KEY_LEFTMETA, e.KEY_LEFTSHIFT, e.KEY_LEFTALT, e.KEY_LEFTCTRL):
        src_kb.write(e.EV_KEY, code, 0); src_kb.syn()
    await asyncio.sleep(0.25)
    assert sender.forwarding_on is True, "chord did not flip forwarding back on"

    # Phase 5: forwarding ON -- KEY_A goes through again.
    src_kb.write(e.EV_KEY, e.KEY_A, 1); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_A, 0); src_kb.syn()
    await asyncio.sleep(0.25)

    collector.cancel()
    try:
        await collector
    except (asyncio.CancelledError, Exception):
        pass

    key_events = [(ev.code, ev.value) for ev in out if ev.type == e.EV_KEY]
    a_presses = [ev for ev in key_events if ev == (e.KEY_A, 1)]
    t_events = [ev for ev in key_events if ev[0] == e.KEY_T]

    assert len(a_presses) == 2, (
        f"expected 2 KEY_A presses (phase 1 + 5), got {len(a_presses)}: {key_events}"
    )
    assert not t_events, f"KEY_T leaked to receiver: {key_events}"

    sender.shutdown()
    receiver.shutdown()
    for t in (sender_task, receiver_task):
        try:
            await asyncio.wait_for(t, timeout=1.0)
        except (asyncio.TimeoutError, Exception):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
    src_kb.close()
    recv_kb.close()
    sender_out.close()


def test_chord_off_does_not_strand_trigger_on_receiver():
    """ON->OFF must release the still-held trigger on the receiver side,
    not leak it via active_keys-based resync that would leave A repeating."""
    asyncio.run(_chord_off_clears_trigger())


async def _chord_off_clears_trigger() -> None:
    src_kb = evdev.UInput(
        {e.EV_KEY: [e.KEY_A, e.KEY_F10]},
        name="evpipe-test-stuck-src",
        vendor=0xCAFE, product=0xBAB7,
    )
    await asyncio.sleep(0.2)

    rfd, wfd = os.pipe()
    sender_out = os.fdopen(wfd, "wb", buffering=0)
    recv_in = os.fdopen(rfd, "rb", buffering=0)
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    proto = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: proto, recv_in)

    sender = SenderApp(
        [(src_kb.device.path, wire.DEV_KB)],
        toggle_chord_evdev=[e.KEY_F10],
        stdout=sender_out,
        resync_interval_s=5.0,
        start_forwarding=True,
    )
    receiver = ReceiverApp()
    sender_task = asyncio.create_task(sender.run())
    receiver_task = asyncio.create_task(receiver.run(reader))

    for _ in range(40):
        if receiver.uinputs:
            break
        await asyncio.sleep(0.05)
    assert receiver.uinputs

    # Simulate a hold-then-release pattern with the trigger still held at
    # the moment ON->OFF runs (which is what the real-world bug needs).
    src_kb.write(e.EV_KEY, e.KEY_F10, 1); src_kb.syn()
    await asyncio.sleep(0.2)
    assert sender.forwarding_on is False, "chord should have flipped to OFF"

    # The receiver must not be tracking the trigger key as held; otherwise
    # F10 sits down forever on A and autorepeats.
    f10_hid = (hid_map.HID_PAGE_KEYBOARD << 8) | 0x43  # KEY_F10 -> 0x43
    assert f10_hid not in receiver.held_per_dev[0], (
        f"trigger key stuck on receiver: {receiver.held_per_dev[0]}"
    )

    src_kb.write(e.EV_KEY, e.KEY_F10, 0); src_kb.syn()
    await asyncio.sleep(0.1)

    sender.shutdown()
    receiver.shutdown()
    for t in (sender_task, receiver_task):
        try:
            await asyncio.wait_for(t, timeout=1.0)
        except (asyncio.TimeoutError, Exception):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
    src_kb.close()
    sender_out.close()


def test_chord_off_delays_ungrab_until_trigger_released():
    """ON->OFF must keep the grab until the trigger key is physically released.

    The kernel synthesises a press event to the compositor for every key that
    is still held at ungrab time.  Releasing the grab while the trigger finger
    is still down causes a stuck key on B.  We delay _ungrab_all() until the
    trigger's key-up event arrives."""
    asyncio.run(_chord_off_delays_ungrab())


async def _chord_off_delays_ungrab() -> None:
    src_kb = evdev.UInput(
        {e.EV_KEY: [e.KEY_A, e.KEY_F10]},
        name="evpipe-test-delayed-ungrab",
        vendor=0xCAFE, product=0xBAB8,
    )
    await asyncio.sleep(0.2)

    rfd, wfd = os.pipe()
    sender_out = os.fdopen(wfd, "wb", buffering=0)
    recv_in = os.fdopen(rfd, "rb", buffering=0)
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    proto = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: proto, recv_in)

    sender = SenderApp(
        [(src_kb.device.path, wire.DEV_KB)],
        toggle_chord_evdev=[e.KEY_F10],
        stdout=sender_out,
        resync_interval_s=5.0,
        start_forwarding=True,
    )
    receiver = ReceiverApp()
    sender_task = asyncio.create_task(sender.run())
    receiver_task = asyncio.create_task(receiver.run(reader))

    for _ in range(40):
        if receiver.uinputs:
            break
        await asyncio.sleep(0.05)
    assert receiver.uinputs

    # Press trigger -- chord fires, forwarding flips OFF.
    src_kb.write(e.EV_KEY, e.KEY_F10, 1); src_kb.syn()
    await asyncio.sleep(0.15)
    assert sender.forwarding_on is False, "chord should have flipped OFF"

    # The grab must still be held (trigger finger is down) so that the
    # compositor on B does not see a synthetic press for the trigger.
    assert all(s.grabbed for s in sender.sources), (
        "ungrab fired while trigger still down -- compositor will see synthetic press"
    )
    assert sender._waiting_chord_release_code == e.KEY_F10, (
        "_waiting_chord_release_code should be set while trigger is held"
    )

    # Release the trigger -- ungrab must fire now.
    src_kb.write(e.EV_KEY, e.KEY_F10, 0); src_kb.syn()
    await asyncio.sleep(0.15)
    assert all(not s.grabbed for s in sender.sources), (
        "ungrab did not fire after trigger release"
    )
    assert sender._waiting_chord_release_code is None

    sender.shutdown()
    receiver.shutdown()
    for t in (sender_task, receiver_task):
        try:
            await asyncio.wait_for(t, timeout=1.0)
        except (asyncio.TimeoutError, Exception):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
    src_kb.close()
    sender_out.close()


def test_single_key_chord_flips_state():
    """A bare trigger key with no modifiers must still toggle and be eaten."""
    asyncio.run(_single_key_chord())


async def _single_key_chord() -> None:
    src_kb = evdev.UInput(
        {e.EV_KEY: [e.KEY_A, e.KEY_F10]},
        name="evpipe-test-f10-src",
        vendor=0xCAFE, product=0xBAB6,
    )
    await asyncio.sleep(0.2)

    rfd, wfd = os.pipe()
    sender_out = os.fdopen(wfd, "wb", buffering=0)
    recv_in = os.fdopen(rfd, "rb", buffering=0)
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    proto = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: proto, recv_in)

    sender = SenderApp(
        [(src_kb.device.path, wire.DEV_KB)],
        toggle_chord_evdev=[e.KEY_F10],
        stdout=sender_out,
        resync_interval_s=5.0,
        start_forwarding=True,
    )
    receiver = ReceiverApp()
    sender_task = asyncio.create_task(sender.run())
    receiver_task = asyncio.create_task(receiver.run(reader))

    for _ in range(40):
        if receiver.uinputs:
            break
        await asyncio.sleep(0.05)
    assert receiver.uinputs

    recv_kb = evdev.InputDevice(receiver.uinputs[0].device.path)
    out: list[evdev.InputEvent] = []

    async def collect() -> None:
        async for ev in recv_kb.async_read_loop():
            out.append(ev)

    collector = asyncio.create_task(collect())

    # ON: A reaches the receiver.
    src_kb.write(e.EV_KEY, e.KEY_A, 1); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_A, 0); src_kb.syn()
    await asyncio.sleep(0.15)
    # Tap F10: chord fires -> forwarding OFF, F10 consumed.
    src_kb.write(e.EV_KEY, e.KEY_F10, 1); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_F10, 0); src_kb.syn()
    await asyncio.sleep(0.2)
    assert sender.forwarding_on is False
    # OFF: A is dropped.
    src_kb.write(e.EV_KEY, e.KEY_A, 1); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_A, 0); src_kb.syn()
    await asyncio.sleep(0.15)
    # Tap F10 again: flips back ON.
    src_kb.write(e.EV_KEY, e.KEY_F10, 1); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_F10, 0); src_kb.syn()
    await asyncio.sleep(0.2)
    assert sender.forwarding_on is True
    # ON again: A reaches the receiver.
    src_kb.write(e.EV_KEY, e.KEY_A, 1); src_kb.syn()
    src_kb.write(e.EV_KEY, e.KEY_A, 0); src_kb.syn()
    await asyncio.sleep(0.2)

    collector.cancel()
    try:
        await collector
    except (asyncio.CancelledError, Exception):
        pass

    key_events = [(ev.code, ev.value) for ev in out if ev.type == e.EV_KEY]
    a_presses = [ev for ev in key_events if ev == (e.KEY_A, 1)]
    f10_events = [ev for ev in key_events if ev[0] == e.KEY_F10]
    assert len(a_presses) == 2, f"expected 2 KEY_A presses, got: {key_events}"
    assert not f10_events, f"KEY_F10 leaked to receiver: {key_events}"

    sender.shutdown()
    receiver.shutdown()
    for t in (sender_task, receiver_task):
        try:
            await asyncio.wait_for(t, timeout=1.0)
        except (asyncio.TimeoutError, Exception):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
    src_kb.close()
    recv_kb.close()
    sender_out.close()


async def _drain(dev: evdev.InputDevice, window_s: float) -> list[evdev.InputEvent]:
    out: list[evdev.InputEvent] = []

    async def consume() -> None:
        async for ev in dev.async_read_loop():
            out.append(ev)

    task = asyncio.create_task(consume())
    try:
        await asyncio.sleep(window_s)
    finally:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
    return out
