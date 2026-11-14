"""Sender forwarding, toggle chord, action chord and local-only keys, driven
through fake input devices and asserted on the wire output."""
from __future__ import annotations

import asyncio
import logging

import evdev.ecodes as e

from .fakes import FakeInputDevice, SenderHarness, async_test

A, D, ENTER = e.KEY_A, e.KEY_D, e.KEY_ENTER
SHIFT, CTRL, META = e.KEY_LEFTSHIFT, e.KEY_LEFTCTRL, e.KEY_LEFTMETA
F9, F10, F12 = e.KEY_F9, e.KEY_F10, e.KEY_F12
BTN = e.BTN_LEFT


def _kb(**kwargs) -> FakeInputDevice:
    return FakeInputDevice(
        keys=[A, D, ENTER, SHIFT, CTRL, META, F9, F10, F12, BTN], **kwargs)


@async_test
async def test_keys_are_forwarded_in_order_and_grab_is_released_on_exit():
    kb = _kb()
    async with SenderHarness(kb) as h:
        assert kb.grabbed
        kb.press(SHIFT)
        kb.tap(A)
        kb.release(SHIFT)
        await h.settle()
        assert await h.packets() == [
            ("key", SHIFT, 1), ("syn",),
            ("key", A, 1), ("syn",),
            ("key", A, 0), ("syn",),
            ("key", SHIFT, 0), ("syn",),
        ]
    assert not kb.grabbed
    assert kb.closed


@async_test
async def test_relative_motion_is_forwarded():
    mouse = FakeInputDevice(keys=[BTN], rels=[e.REL_X, e.REL_Y])
    async with SenderHarness(mouse) as h:
        mouse.rel(e.REL_X, -3)
        await h.settle()
        assert await h.packets() == [("rel", e.REL_X, -3), ("syn",)]


@async_test
async def test_unmapped_key_is_dropped_and_logged_at_debug(caplog):
    kb = FakeInputDevice(keys=[A, e.KEY_FN])
    async with SenderHarness(kb) as h:
        with caplog.at_level(logging.DEBUG, logger="evpipe-send"):
            kb.tap(e.KEY_FN)
            kb.tap(A)
            await h.settle()
        assert await h.key_events() == [(A, 1), (A, 0)]
        dropped = [r.getMessage() for r in caplog.records if "no HID mapping" in r.getMessage()]
        assert dropped == [f"key {e.KEY_FN} has no HID mapping; not forwarded"]


@async_test
async def test_resync_reports_held_keys():
    kb = _kb()
    async with SenderHarness(kb) as h:
        kb.press(A)
        await h.settle()
        await h.app._resync()
        kb.release(A)
        await h.settle()
        await h.app._resync()
        assert await h.full_states() == [[A], []]


@async_test
async def test_single_key_chord_toggles_and_is_never_forwarded():
    kb = _kb()
    async with SenderHarness(kb, toggle_chord_evdev=[F10]) as h:
        kb.tap(A)
        kb.tap(F10)
        await h.settle()
        assert h.app.forwarding_on is False
        assert not kb.grabbed
        kb.tap(A)  # OFF: stays local
        kb.tap(F10)
        await h.settle()
        assert h.app.forwarding_on is True
        assert kb.grabbed
        kb.tap(A)
        await h.settle()
        assert await h.key_events() == [(A, 1), (A, 0), (A, 1), (A, 0)]


@async_test
async def test_chord_needs_its_modifiers():
    kb = _kb()
    async with SenderHarness(kb, toggle_chord_evdev=[CTRL, F12]) as h:
        kb.tap(F12)  # no modifier: an ordinary key
        await h.settle()
        assert h.app.forwarding_on is True
        assert await h.key_events() == [(F12, 1), (F12, 0)]

        kb.press(CTRL)
        kb.tap(F12)
        kb.release(CTRL)
        await h.settle()
        assert h.app.forwarding_on is False
        # The modifier went out before the trigger revealed the chord; the
        # FULL_STATE sent at the flip releases it on the receiver.
        assert await h.key_events() == [(F12, 1), (F12, 0), (CTRL, 1)]
        assert await h.full_states() == [[]]


@async_test
async def test_chord_off_keeps_trigger_released_while_it_is_held():
    """A resync during the wait for the trigger's release must not sample
    the still-held trigger and press it on the receiver."""
    kb = _kb()
    async with SenderHarness(kb, toggle_chord_evdev=[F10]) as h:
        kb.press(F10)
        await h.settle()
        assert h.app.forwarding_on is False
        await h.app._resync()
        kb.release(F10)
        await h.settle()
        await h.app._resync()
        assert await h.key_events() == []
        assert all(held == [] for held in await h.full_states())
        assert len(await h.full_states()) == 3


@async_test
async def test_chord_off_delays_ungrab_until_trigger_released():
    kb = _kb()
    async with SenderHarness(kb, toggle_chord_evdev=[F10]) as h:
        kb.press(F10)
        await h.settle()
        assert h.app.forwarding_on is False
        assert kb.grabbed, "ungrabbed with the trigger still down"
        kb.release(F10)
        await h.settle()
        assert not kb.grabbed
        assert kb.seen_by_others == []


@async_test
async def test_chord_on_defers_grab_until_chord_released():
    """The chord's key-ups must reach the local compositor, which saw the
    presses; grabbing first would strand the keys pressed there."""
    kb = _kb()
    async with SenderHarness(kb, toggle_chord_evdev=[CTRL, F12],
                             start_forwarding=False) as h:
        kb.press(CTRL)
        kb.press(F12)
        await h.settle()
        assert not kb.grabbed
        assert h.app.forwarding_on is False
        kb.release(F12)
        await h.settle()
        assert not kb.grabbed, "grabbed with a chord modifier still down"
        kb.release(CTRL)
        await h.settle()
        assert kb.grabbed
        assert h.app.forwarding_on is True
        assert kb.seen_by_others == [(CTRL, 1), (F12, 1), (F12, 0), (CTRL, 0)]
        assert await h.key_events() == []


@async_test
async def test_chord_on_defers_grab_until_held_button_released():
    kb = _kb()
    async with SenderHarness(kb, toggle_chord_evdev=[F12],
                             start_forwarding=False) as h:
        kb.press(BTN)
        kb.tap(F12)
        await h.settle()
        assert not kb.grabbed, "grabbed with a button still down"
        assert h.app.forwarding_on is False
        kb.release(BTN)
        await h.settle()
        assert kb.grabbed
        assert h.app.forwarding_on is True
        assert (BTN, 0) in kb.seen_by_others
        assert await h.key_events() == []


@async_test
async def test_action_chord_fires_only_while_grabbed_and_eats_trigger(tmp_path):
    marker = tmp_path / "fired"
    kb = _kb()
    async with SenderHarness(
        kb, toggle_chord_evdev=[F10],
        action_chords=[([F9], f"echo fired >> {marker}")],
    ) as h:
        async def fired() -> int:
            await h.settle()
            await asyncio.gather(*h.app._background_tasks)
            return len(marker.read_text().splitlines()) if marker.exists() else 0

        kb.tap(F9)
        assert await fired() == 1
        kb.tap(F9)
        assert await fired() == 2

        kb.tap(F10)  # forwarding OFF: the key belongs to the compositor
        kb.tap(F9)
        assert await fired() == 2
        assert await h.key_events() == []


@async_test
async def test_local_only_key_matches_chords_but_never_reaches_the_wire(tmp_path):
    marker = tmp_path / "fired"
    kb = _kb()
    async with SenderHarness(
        kb, local_only_evdev=[META],
        action_chords=[([META, D], f"echo fired >> {marker}")],
    ) as h:
        kb.press(META)
        kb.tap(A)
        await h.settle()
        await h.app._resync()
        kb.tap(D)
        kb.release(META)
        await h.settle()
        await asyncio.gather(*h.app._background_tasks)
        assert marker.read_text() == "fired\n"
        assert await h.key_events() == [(A, 1), (A, 0)]
        assert await h.full_states() == [[]]


@async_test
async def test_lost_trigger_release_does_not_keep_the_grab():
    """ON->OFF holds the grab until the trigger's key-up. If that event is
    lost the wait must still end, or the keyboard stays grabbed with every
    event, the chord included, swallowed."""
    kb = _kb()
    async with SenderHarness(kb, toggle_chord_evdev=[F10]) as h:
        kb.press(F10)
        await h.settle()
        kb.lose_release(F10)
        await h.app._resync()
        # Up in the kernel on one tick only: the event may still be queued.
        assert kb.grabbed
        await h.app._resync()
        assert not kb.grabbed

        kb.tap(F10)
        await h.settle()
        assert h.app.forwarding_on is True
        assert kb.grabbed
        kb.tap(A)
        await h.settle()
        assert await h.key_events() == [(A, 1), (A, 0)]


@async_test
async def test_lost_release_does_not_block_switching_on():
    kb = _kb()
    async with SenderHarness(kb, toggle_chord_evdev=[CTRL, F12],
                             start_forwarding=False) as h:
        kb.press(CTRL)
        kb.tap(F12)
        await h.settle()
        kb.lose_release(CTRL)
        await h.app._resync()
        assert not kb.grabbed
        await h.app._resync()
        assert kb.grabbed
        assert h.app.forwarding_on is True

        # The lost modifier is not remembered as held: a bare F12 is a key.
        kb.tap(F12)
        await h.settle()
        assert h.app.forwarding_on is True
        assert await h.key_events() == [(F12, 1), (F12, 0)]


@async_test
async def test_lost_release_needs_two_consecutive_ticks():
    kb = _kb()
    async with SenderHarness(kb, toggle_chord_evdev=[F12],
                             start_forwarding=False) as h:
        kb.press(BTN)
        kb.tap(F12)
        await h.settle()
        kb.lose_release(BTN)
        await h.app._resync()   # up
        kb.press(BTN)
        await h.settle()
        await h.app._resync()   # down again
        kb.lose_release(BTN)
        await h.app._resync()   # up, but not twice in a row
        assert not kb.grabbed
        await h.app._resync()
        assert kb.grabbed


@async_test
async def test_action_trigger_released_during_a_toggle_fires_again(tmp_path):
    marker = tmp_path / "fired"
    kb = _kb()
    async with SenderHarness(
        kb, toggle_chord_evdev=[F10],
        action_chords=[([F9], f"echo fired >> {marker}")],
    ) as h:
        kb.press(F9)
        kb.press(F10)   # ON->OFF while the action trigger is still down
        kb.release(F9)
        kb.release(F10)
        kb.tap(F10)     # back ON
        kb.tap(F9)
        await h.settle()
        await asyncio.gather(*h.app._background_tasks)
        assert marker.read_text() == "fired\nfired\n"
        assert await h.key_events() == []


@async_test
async def test_each_deferred_toggle_judges_lost_releases_afresh():
    kb = _kb()
    async with SenderHarness(kb, toggle_chord_evdev=[F10]) as h:
        kb.press(F10)
        await h.settle()
        kb.release(F10)         # up in the kernel, event still queued
        await h.app._resync()
        await h.settle()        # delivered after all: OFF, ungrabbed
        assert not kb.grabbed

        kb.press(F10)
        await h.settle()
        kb.lose_release(F10)
        await h.app._resync()
        assert not kb.grabbed, "one tick must not be enough"
        await h.app._resync()
        assert kb.grabbed


@async_test
async def test_startup_defers_grab_while_a_key_is_held():
    """Starting ON is an OFF->ON flip: a key that is down belongs to the
    local compositor until it is released, and must not be pressed on the
    receiver by the first resync."""
    kb = _kb(held=[ENTER])
    async with SenderHarness(kb) as h:
        assert not kb.grabbed
        await h.app._resync()
        kb.release(ENTER)
        await h.settle()
        assert kb.grabbed
        assert h.app.forwarding_on is True
        assert kb.seen_by_others == [(ENTER, 0)]
        kb.tap(A)
        await h.settle()
        assert await h.key_events() == [(A, 1), (A, 0)]
        assert all(held == [] for held in await h.full_states())


@async_test
async def test_start_off_does_not_grab():
    kb = _kb(held=[ENTER])
    async with SenderHarness(kb, toggle_chord_evdev=[F10],
                             start_forwarding=False) as h:
        kb.release(ENTER)
        kb.tap(A)
        await h.settle()
        assert not kb.grabbed
        assert h.app.forwarding_on is False
        assert await h.key_events() == []
