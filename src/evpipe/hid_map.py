"""Static evdev KEY_*/BTN_*/REL_* <-> HID Usage ID translation tables.

Wire codes are packed as ``(page << 8) | usage`` so a single u16 covers
the three HID pages we forward: 0x07 Keyboard/Keypad, 0x09 Button (mouse),
0x0C Consumer (media keys).

REL axes use a private small enum -- the design picked plain X=1, Y=2,
WHEEL=3, ... rather than the HID Generic Desktop usages, because the
receiver always knows the axis context and the enum compresses.

Both halves of every table are checked in: regenerating from libevdev at
install time would couple us to host headers and isn't worth the build-time
dep. The static set covers every code a stock 104/105 kb, mouse, or
consumer-control keyboard emits in normal use; codes outside it are
dropped at the sender with a warning.
"""
from __future__ import annotations

import evdev.ecodes as e

# HID pages we encode in the high byte of the wire code.
HID_PAGE_KEYBOARD = 0x07
HID_PAGE_BUTTON = 0x09
HID_PAGE_CONSUMER = 0x0C


def _pack(page: int, usage: int) -> int:
    return (page << 8) | usage


# evdev KEY_* -> HID keyboard usage (page 0x07).
_KEYBOARD_PAIRS: list[tuple[int, int]] = [
    (e.KEY_A, 0x04), (e.KEY_B, 0x05), (e.KEY_C, 0x06), (e.KEY_D, 0x07),
    (e.KEY_E, 0x08), (e.KEY_F, 0x09), (e.KEY_G, 0x0A), (e.KEY_H, 0x0B),
    (e.KEY_I, 0x0C), (e.KEY_J, 0x0D), (e.KEY_K, 0x0E), (e.KEY_L, 0x0F),
    (e.KEY_M, 0x10), (e.KEY_N, 0x11), (e.KEY_O, 0x12), (e.KEY_P, 0x13),
    (e.KEY_Q, 0x14), (e.KEY_R, 0x15), (e.KEY_S, 0x16), (e.KEY_T, 0x17),
    (e.KEY_U, 0x18), (e.KEY_V, 0x19), (e.KEY_W, 0x1A), (e.KEY_X, 0x1B),
    (e.KEY_Y, 0x1C), (e.KEY_Z, 0x1D),
    (e.KEY_1, 0x1E), (e.KEY_2, 0x1F), (e.KEY_3, 0x20), (e.KEY_4, 0x21),
    (e.KEY_5, 0x22), (e.KEY_6, 0x23), (e.KEY_7, 0x24), (e.KEY_8, 0x25),
    (e.KEY_9, 0x26), (e.KEY_0, 0x27),
    (e.KEY_ENTER, 0x28), (e.KEY_ESC, 0x29), (e.KEY_BACKSPACE, 0x2A),
    (e.KEY_TAB, 0x2B), (e.KEY_SPACE, 0x2C),
    (e.KEY_MINUS, 0x2D), (e.KEY_EQUAL, 0x2E),
    (e.KEY_LEFTBRACE, 0x2F), (e.KEY_RIGHTBRACE, 0x30),
    (e.KEY_BACKSLASH, 0x31),
    (e.KEY_SEMICOLON, 0x33), (e.KEY_APOSTROPHE, 0x34),
    (e.KEY_GRAVE, 0x35),
    (e.KEY_COMMA, 0x36), (e.KEY_DOT, 0x37), (e.KEY_SLASH, 0x38),
    (e.KEY_CAPSLOCK, 0x39),
    (e.KEY_F1, 0x3A), (e.KEY_F2, 0x3B), (e.KEY_F3, 0x3C), (e.KEY_F4, 0x3D),
    (e.KEY_F5, 0x3E), (e.KEY_F6, 0x3F), (e.KEY_F7, 0x40), (e.KEY_F8, 0x41),
    (e.KEY_F9, 0x42), (e.KEY_F10, 0x43), (e.KEY_F11, 0x44), (e.KEY_F12, 0x45),
    (e.KEY_SYSRQ, 0x46), (e.KEY_SCROLLLOCK, 0x47), (e.KEY_PAUSE, 0x48),
    (e.KEY_INSERT, 0x49), (e.KEY_HOME, 0x4A), (e.KEY_PAGEUP, 0x4B),
    (e.KEY_DELETE, 0x4C), (e.KEY_END, 0x4D), (e.KEY_PAGEDOWN, 0x4E),
    (e.KEY_RIGHT, 0x4F), (e.KEY_LEFT, 0x50),
    (e.KEY_DOWN, 0x51), (e.KEY_UP, 0x52),
    (e.KEY_NUMLOCK, 0x53),
    (e.KEY_KPSLASH, 0x54), (e.KEY_KPASTERISK, 0x55),
    (e.KEY_KPMINUS, 0x56), (e.KEY_KPPLUS, 0x57),
    (e.KEY_KPENTER, 0x58),
    (e.KEY_KP1, 0x59), (e.KEY_KP2, 0x5A), (e.KEY_KP3, 0x5B),
    (e.KEY_KP4, 0x5C), (e.KEY_KP5, 0x5D), (e.KEY_KP6, 0x5E),
    (e.KEY_KP7, 0x5F), (e.KEY_KP8, 0x60), (e.KEY_KP9, 0x61),
    (e.KEY_KP0, 0x62), (e.KEY_KPDOT, 0x63),
    (e.KEY_102ND, 0x64),
    (e.KEY_COMPOSE, 0x65),
    (e.KEY_POWER, 0x66),
    (e.KEY_KPEQUAL, 0x67),
    (e.KEY_F13, 0x68), (e.KEY_F14, 0x69), (e.KEY_F15, 0x6A),
    (e.KEY_F16, 0x6B), (e.KEY_F17, 0x6C), (e.KEY_F18, 0x6D),
    (e.KEY_F19, 0x6E), (e.KEY_F20, 0x6F), (e.KEY_F21, 0x70),
    (e.KEY_F22, 0x71), (e.KEY_F23, 0x72), (e.KEY_F24, 0x73),
    (e.KEY_LEFTCTRL, 0xE0), (e.KEY_LEFTSHIFT, 0xE1),
    (e.KEY_LEFTALT, 0xE2), (e.KEY_LEFTMETA, 0xE3),
    (e.KEY_RIGHTCTRL, 0xE4), (e.KEY_RIGHTSHIFT, 0xE5),
    (e.KEY_RIGHTALT, 0xE6), (e.KEY_RIGHTMETA, 0xE7),
]

# evdev BTN_* -> HID button page usage. BTN_LEFT == button 1, etc.
_BUTTON_PAIRS: list[tuple[int, int]] = [
    (e.BTN_LEFT, 1),
    (e.BTN_RIGHT, 2),
    (e.BTN_MIDDLE, 3),
    (e.BTN_SIDE, 4),
    (e.BTN_EXTRA, 5),
    (e.BTN_FORWARD, 6),
    (e.BTN_BACK, 7),
    (e.BTN_TASK, 8),
]

# evdev KEY_* (consumer/media) -> HID consumer usage (page 0x0C).
_CONSUMER_PAIRS: list[tuple[int, int]] = [
    (e.KEY_PLAYPAUSE, 0xCD),
    (e.KEY_STOPCD, 0xB7),
    (e.KEY_PREVIOUSSONG, 0xB6),
    (e.KEY_NEXTSONG, 0xB5),
    (e.KEY_MUTE, 0xE2),
    (e.KEY_VOLUMEUP, 0xE9),
    (e.KEY_VOLUMEDOWN, 0xEA),
    (e.KEY_PLAY, 0xB0),
    (e.KEY_PAUSE, 0xB1),
    (e.KEY_REWIND, 0xB4),
    (e.KEY_FORWARD, 0xB3),
    (e.KEY_EJECTCD, 0xB8),
]

KEY_TO_HID: dict[int, int] = {}
for _ev, _u in _KEYBOARD_PAIRS:
    KEY_TO_HID[_ev] = _pack(HID_PAGE_KEYBOARD, _u)
for _ev, _u in _BUTTON_PAIRS:
    KEY_TO_HID[_ev] = _pack(HID_PAGE_BUTTON, _u)
for _ev, _u in _CONSUMER_PAIRS:
    # Only populate if not already a keyboard mapping (KEY_PAUSE is
    # ambiguous in evdev -- same code as the Pause key on the keyboard
    # page; let the keyboard mapping win for round-trip identity).
    KEY_TO_HID.setdefault(_ev, _pack(HID_PAGE_CONSUMER, _u))

HID_TO_KEY: dict[int, int] = {v: k for k, v in KEY_TO_HID.items()}

# REL axis small enum.
REL_X = 1
REL_Y = 2
REL_WHEEL = 3
REL_HWHEEL = 4
REL_WHEEL_HIRES = 5
REL_HWHEEL_HIRES = 6

REL_TO_AXIS: dict[int, int] = {
    e.REL_X: REL_X,
    e.REL_Y: REL_Y,
    e.REL_WHEEL: REL_WHEEL,
    e.REL_HWHEEL: REL_HWHEEL,
    e.REL_WHEEL_HI_RES: REL_WHEEL_HIRES,
    e.REL_HWHEEL_HI_RES: REL_HWHEEL_HIRES,
}

AXIS_TO_REL: dict[int, int] = {v: k for k, v in REL_TO_AXIS.items()}


def encode_key(evdev_code: int) -> int | None:
    """Translate an evdev KEY_*/BTN_* code to a packed HID wire code.

    Returns None for codes outside the static table. The sender should
    log + drop these rather than guess at a fallback encoding.
    """
    return KEY_TO_HID.get(evdev_code)


def decode_key(wire_code: int) -> int | None:
    return HID_TO_KEY.get(wire_code)


def encode_rel(evdev_code: int) -> int | None:
    return REL_TO_AXIS.get(evdev_code)


def decode_rel(axis: int) -> int | None:
    return AXIS_TO_REL.get(axis)
