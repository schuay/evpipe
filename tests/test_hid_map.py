"""Static table sanity: round-trip identity and a few spot-checks against
the published HID Usage Tables."""
from __future__ import annotations

import evdev.ecodes as e

from evpipe import hid_map


def test_letter_a_maps_to_keyboard_usage_4():
    assert hid_map.encode_key(e.KEY_A) == (hid_map.HID_PAGE_KEYBOARD << 8) | 0x04


def test_left_gui_maps_to_keyboard_usage_e3():
    assert hid_map.encode_key(e.KEY_LEFTMETA) == (hid_map.HID_PAGE_KEYBOARD << 8) | 0xE3


def test_left_button_maps_to_button_page_usage_1():
    assert hid_map.encode_key(e.BTN_LEFT) == (hid_map.HID_PAGE_BUTTON << 8) | 0x01


def test_play_pause_consumer():
    assert hid_map.encode_key(e.KEY_PLAYPAUSE) == (hid_map.HID_PAGE_CONSUMER << 8) | 0xCD


def test_key_roundtrip_identity():
    # Every evdev key in our table must round-trip through the wire and
    # come back identical: that's the entire correctness guarantee for
    # Linux-to-Linux forwarding.
    for ev_code, hid_wire in hid_map.KEY_TO_HID.items():
        assert hid_map.decode_key(hid_wire) == ev_code


def test_rel_roundtrip_identity():
    for ev_code, axis in hid_map.REL_TO_AXIS.items():
        assert hid_map.decode_rel(axis) == ev_code


def test_unknown_key_returns_none():
    assert hid_map.encode_key(0xDEAD) is None
    assert hid_map.decode_key(0xDEAD) is None


def test_unknown_rel_returns_none():
    assert hid_map.encode_rel(0xDEAD) is None
    assert hid_map.decode_rel(0xDEAD) is None
