"""Unit tests for the dictation control plane and action-chord parsing.

These exercise the sender's text-injection routing and CLI parsing without
needing /dev/uinput: the SenderApp is driven with a BytesIO stdout and no
real source devices, so we assert on the wire bytes it produces.
"""
from __future__ import annotations

import argparse
import asyncio
import io
import json
import struct

import evdev.ecodes as e
import pytest

from evpipe import hid_map, wire
from evpipe.send import SenderApp, _parse_action_chord


def _make_app(stdout, forwarding: bool) -> SenderApp:
    app = SenderApp(
        [],
        toggle_chord_evdev=[],
        stdout=stdout,
        dictation_socket=None,
        start_forwarding=forwarding,
    )
    return app


class _FakeSource:
    """Minimal stand-in for Source: only the fields _inject_text touches."""

    def __init__(self, device_id: int, role: int) -> None:
        self.device_id = device_id
        self.role = role


def _decode_key_events(data: bytes) -> list[tuple[int, int]]:
    """Pull (hid_code, value) pairs out of a sender byte stream, key events only."""
    out: list[tuple[int, int]] = []
    off = 0
    while off < len(data):
        (body_len,) = struct.unpack_from("<H", data, off)
        kind = data[off + 2]
        payload = data[off + 3 : off + 2 + body_len]
        off += 2 + body_len
        if kind == wire.PACKET_EVENT:
            ev = wire.decode_event(payload)
            if ev.ev_kind == wire.EV_KEY:
                out.append((ev.code, ev.value))
    return out


def test_parse_action_chord_splits_on_first_colon():
    codes, cmd = _parse_action_chord("KEY_LEFTMETA+KEY_D:echo x > /tmp/f:oo")
    assert codes == [e.KEY_LEFTMETA, e.KEY_D]
    assert cmd == "echo x > /tmp/f:oo"


def test_parse_action_chord_requires_command():
    with pytest.raises(argparse.ArgumentTypeError):
        _parse_action_chord("KEY_F9")
    with pytest.raises(argparse.ArgumentTypeError):
        _parse_action_chord("KEY_F9:")


def test_inject_text_emits_shifted_and_unshifted():
    out = io.BytesIO()
    app = _make_app(out, forwarding=True)
    app.sources = [_FakeSource(device_id=0, role=wire.DEV_KB)]

    asyncio.run(app._inject_text("aB", submit=False))

    shift = hid_map.encode_key(e.KEY_LEFTSHIFT)
    a = hid_map.encode_key(e.KEY_A)
    b = hid_map.encode_key(e.KEY_B)
    seq = _decode_key_events(out.getvalue())
    assert seq == [
        (a, 1), (a, 0),                 # 'a' -- no shift
        (shift, 1), (b, 1), (b, 0), (shift, 0),  # 'B' -- shift wrapper
    ]


def test_inject_text_submit_appends_enter():
    out = io.BytesIO()
    app = _make_app(out, forwarding=True)
    app.sources = [_FakeSource(device_id=0, role=wire.DEV_KB)]

    asyncio.run(app._inject_text("x", submit=True))

    enter = hid_map.encode_key(e.KEY_ENTER)
    seq = _decode_key_events(out.getvalue())
    assert seq[-2:] == [(enter, 1), (enter, 0)]


def test_inject_text_drops_unmapped_chars():
    out = io.BytesIO()
    app = _make_app(out, forwarding=True)
    app.sources = [_FakeSource(device_id=0, role=wire.DEV_KB)]

    asyncio.run(app._inject_text("aéb", submit=False))

    a = hid_map.encode_key(e.KEY_A)
    b = hid_map.encode_key(e.KEY_B)
    seq = _decode_key_events(out.getvalue())
    assert seq == [(a, 1), (a, 0), (b, 1), (b, 0)]


def test_dictation_target_prefers_keyboard():
    out = io.BytesIO()
    app = _make_app(out, forwarding=True)
    app.sources = [
        _FakeSource(device_id=0, role=wire.DEV_MOUSE),
        _FakeSource(device_id=1, role=wire.DEV_KB),
    ]
    assert app._dictation_target().device_id == 1


def _run_client(app: SenderApp, text: str, submit: bool = False):
    """Drive _handle_dictation_client with an in-memory reader/writer pair."""
    written = bytearray()

    class _W:
        def write(self, b):
            written.extend(b)

        async def drain(self):
            pass

        def close(self):
            pass

    async def go():
        reader = asyncio.StreamReader()
        reader.feed_data(
            (json.dumps({"text": text, "submit": submit}) + "\n").encode())
        reader.feed_eof()
        await app._handle_dictation_client(reader, _W())

    asyncio.run(go())
    return json.loads(bytes(written).decode())


def test_routing_remote_when_forwarding():
    out = io.BytesIO()
    app = _make_app(out, forwarding=True)
    app.sources = [_FakeSource(device_id=0, role=wire.DEV_KB)]
    reply = _run_client(app, "hi")
    assert reply == {"routed": "remote"}
    assert len(out.getvalue()) > 0  # keys were injected


def test_routing_local_when_not_forwarding():
    out = io.BytesIO()
    app = _make_app(out, forwarding=False)
    app.sources = [_FakeSource(device_id=0, role=wire.DEV_KB)]
    reply = _run_client(app, "hi")
    assert reply == {"routed": "local"}
    assert out.getvalue() == b""  # nothing injected locally
