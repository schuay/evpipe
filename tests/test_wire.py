"""Wire-format round-trip tests. Exercise every record + packet kind on
empty/non-empty payloads, then a full session encode/decode chain."""
from __future__ import annotations

import io

import pytest

from evpipe import wire
from evpipe.wire import (
    AbsAxis,
    DeviceDescriptor,
    EV_KEY,
    EV_REL,
    EV_SYN,
    Event,
    PACKET_EVENT,
    PACKET_FULL_STATE,
    PACKET_HEARTBEAT,
    ProtocolError,
)


def test_event_roundtrip():
    ev = Event(ev_kind=EV_KEY, code=0x0704, value=1, timestamp_us=12345, device_id=2)
    payload = wire.encode_event(ev)
    assert len(payload) == 17
    assert wire.decode_event(payload) == ev


def test_event_size_mismatch_raises():
    with pytest.raises(ProtocolError):
        wire.decode_event(b"\x00" * 16)


def test_full_state_empty():
    payload = wire.encode_full_state(0, [])
    dev, held = wire.decode_full_state(payload)
    assert dev == 0
    assert held == []


def test_full_state_with_keys():
    payload = wire.encode_full_state(1, [0x0704, 0x0901, 0x07E0])
    dev, held = wire.decode_full_state(payload)
    assert dev == 1
    assert held == [0x0704, 0x0901, 0x07E0]


def test_descriptor_minimal():
    d = DeviceDescriptor(name="kb", vendor_id=0x1234, product_id=0x5678, kind=wire.DEV_KB)
    blob = wire.encode_descriptor(d)
    decoded, off = wire.decode_descriptor(blob, 0)
    assert off == len(blob)
    assert decoded == d


def test_descriptor_with_keys_and_rels():
    d = DeviceDescriptor(
        name="combo",
        vendor_id=0xFEED,
        product_id=0xBEEF,
        kind=wire.DEV_COMBO,
        keys=[0x0704, 0x0705, 0x0901, 0x0902],
        rel_axes=[1, 2, 3],
    )
    blob = wire.encode_descriptor(d)
    decoded, off = wire.decode_descriptor(blob, 0)
    assert off == len(blob)
    assert decoded == d


def test_descriptor_with_abs_axes():
    d = DeviceDescriptor(
        name="tablet",
        vendor_id=0,
        product_id=0,
        kind=wire.DEV_TABLET,
        abs_axes=[AbsAxis(usage=0x30, min=0, max=32767, resolution=100)],
    )
    blob = wire.encode_descriptor(d)
    decoded, off = wire.decode_descriptor(blob, 0)
    assert off == len(blob)
    assert decoded == d


def test_session_open_roundtrip():
    devs = [
        DeviceDescriptor(name="kb", vendor_id=1, product_id=2, kind=wire.DEV_KB,
                         keys=[0x0704, 0x0705]),
        DeviceDescriptor(name="mouse", vendor_id=3, product_id=4, kind=wire.DEV_MOUSE,
                         keys=[0x0901, 0x0902], rel_axes=[1, 2, 3]),
    ]
    blob = wire.encode_session_open(devs)
    decoded, off = wire.decode_session_open(blob)
    assert off == len(blob)
    assert decoded == devs


def test_session_open_bad_magic():
    with pytest.raises(ProtocolError):
        wire.decode_session_open(b"BAAD\x01\x00\x00\x00")


def test_packet_event():
    ev = Event(ev_kind=EV_REL, code=1, value=-5, timestamp_us=99, device_id=0)
    pkt = wire.encode_packet(PACKET_EVENT, wire.encode_event(ev))
    assert pkt[0:2] == bytes([18, 0])
    assert pkt[2] == PACKET_EVENT
    kind, payload = _read_one(pkt)
    assert kind == PACKET_EVENT
    assert wire.decode_event(payload) == ev


def test_packet_heartbeat():
    pkt = wire.encode_packet(PACKET_HEARTBEAT, b"")
    kind, payload = _read_one(pkt)
    assert kind == PACKET_HEARTBEAT
    assert payload == b""


def test_packet_full_state():
    pkt = wire.encode_packet(PACKET_FULL_STATE, wire.encode_full_state(0, [0x0704]))
    kind, payload = _read_one(pkt)
    assert kind == PACKET_FULL_STATE
    dev, held = wire.decode_full_state(payload)
    assert dev == 0
    assert held == [0x0704]


def test_stream_session_and_packets():
    devs = [DeviceDescriptor(name="k", vendor_id=0, product_id=0, kind=wire.DEV_KB,
                             keys=[0x0704])]
    buf = io.BytesIO()
    wire.write_session_open(buf, devs)
    wire.write_event(buf, Event(EV_KEY, 0x0704, 1, 1, 0))
    wire.write_event(buf, Event(EV_SYN, 0, 0, 2, 0))
    wire.write_heartbeat(buf)
    wire.write_full_state(buf, 0, [0x0704])

    buf.seek(0)
    got_devs = wire.read_session_open(buf)
    assert got_devs == devs
    kinds = []
    while True:
        try:
            k, _ = wire.read_packet(buf)
        except EOFError:
            break
        kinds.append(k)
    assert kinds == [PACKET_EVENT, PACKET_EVENT, PACKET_HEARTBEAT, PACKET_FULL_STATE]


def test_short_read_raises_eof():
    buf = io.BytesIO(b"")
    with pytest.raises(EOFError):
        wire.read_session_open(buf)


def _read_one(blob: bytes) -> tuple[int, bytes]:
    buf = io.BytesIO(blob)
    return wire.read_packet(buf)
