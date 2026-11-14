"""Wire-format tests: payload round-trips, then whole streams through the
receiver's reader, including the ways a stream can be malformed."""
from __future__ import annotations

import struct

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

from .fakes import async_test, read_stream, stream_reader

SESSION_HEAD = wire.MAGIC + struct.pack("<BBH", wire.VERSION, 0, 1)


def test_event_roundtrip():
    ev = Event(ev_kind=EV_KEY, code=0x0704, value=1, timestamp_us=12345, device_id=2)
    payload = wire.encode_event(ev)
    assert len(payload) == 17
    assert wire.decode_event(payload) == ev


def test_event_size_mismatch_raises():
    with pytest.raises(ProtocolError):
        wire.decode_event(b"\x00" * 16)


@pytest.mark.parametrize("held", [[], [0x0704, 0x0901, 0x07E0]])
def test_full_state_roundtrip(held):
    assert wire.decode_full_state(wire.encode_full_state(1, held)) == (1, held)


@pytest.mark.parametrize("payload", [b"\x00\x00\x01", b"\x00\x00\x02\x00\x04\x07"])
def test_full_state_size_mismatch_raises(payload):
    with pytest.raises(ProtocolError):
        wire.decode_full_state(payload)


def test_packet_framing():
    ev = Event(ev_kind=EV_REL, code=1, value=-5, timestamp_us=99, device_id=0)
    pkt = wire.encode_packet(PACKET_EVENT, wire.encode_event(ev))
    assert pkt[0:2] == bytes([18, 0])
    assert pkt[2] == PACKET_EVENT


@pytest.mark.parametrize("descriptor", [
    DeviceDescriptor(name="kb", vendor_id=0x1234, product_id=0x5678, kind=wire.DEV_KB),
    DeviceDescriptor(name="combo", vendor_id=0xFEED, product_id=0xBEEF,
                     kind=wire.DEV_COMBO,
                     keys=[0x0704, 0x0705, 0x0901, 0x0902], rel_axes=[1, 2, 3]),
    DeviceDescriptor(name="tablet", vendor_id=0, product_id=0, kind=wire.DEV_TABLET,
                     abs_axes=[AbsAxis(usage=0x30, min=0, max=32767, resolution=100)]),
    DeviceDescriptor(name="tästätur", vendor_id=1, product_id=2,
                     kind=wire.DEV_KB, keys=[0x0704]),
], ids=lambda d: d.name)
@async_test
async def test_descriptor_roundtrip(descriptor):
    devices, packets = await read_stream(wire.encode_session_open([descriptor]))
    assert devices == [descriptor]
    assert packets == []


@async_test
async def test_session_and_packets_roundtrip():
    devs = [
        DeviceDescriptor(name="kb", vendor_id=1, product_id=2, kind=wire.DEV_KB,
                         keys=[0x0704, 0x0705]),
        DeviceDescriptor(name="mouse", vendor_id=3, product_id=4, kind=wire.DEV_MOUSE,
                         keys=[0x0901, 0x0902], rel_axes=[1, 2, 3]),
    ]
    events = [Event(EV_KEY, 0x0704, 1, 1, 0), Event(EV_SYN, 0, 0, 2, 0)]
    stream = b"".join([
        wire.encode_session_open(devs),
        *(wire.encode_packet(PACKET_EVENT, wire.encode_event(ev)) for ev in events),
        wire.encode_packet(PACKET_HEARTBEAT, b""),
        wire.encode_packet(PACKET_FULL_STATE, wire.encode_full_state(0, [0x0704])),
    ])
    got_devs, packets = await read_stream(stream)
    assert got_devs == devs
    assert [kind for kind, _ in packets] == [
        PACKET_EVENT, PACKET_EVENT, PACKET_HEARTBEAT, PACKET_FULL_STATE]
    assert [wire.decode_event(p) for _, p in packets[:2]] == events
    assert packets[2][1] == b""
    assert wire.decode_full_state(packets[3][1]) == (0, [0x0704])


@async_test
async def test_empty_stream_is_eof():
    with pytest.raises(EOFError):
        await wire.aread_session_open(stream_reader(b""))


@pytest.mark.parametrize("stream, match", [
    (b"Welcome to hostb\n", "bad magic"),
    (wire.MAGIC + struct.pack("<BBH", wire.VERSION + 1, 0, 0), "unsupported version"),
    (wire.MAGIC + b"\x01", "truncated"),
    (SESSION_HEAD + struct.pack("<H", 2) + b"kb" + b"\x00\x00", "truncated"),
    (SESSION_HEAD + struct.pack("<H", 2) + b"\xff\xfe" + b"\x00" * 9, "not UTF-8"),
], ids=["bad-magic", "version", "short-head", "short-descriptor", "name-not-utf8"])
@async_test
async def test_malformed_session_open_is_a_protocol_error(stream, match):
    with pytest.raises(ProtocolError, match=match):
        await wire.aread_session_open(stream_reader(stream))


@pytest.mark.parametrize("stream, match", [
    (b"\x00\x00", "zero-length"),
    (b"\x05", "truncated"),
    (b"\x05\x00\x01\x02", "truncated"),
], ids=["empty-body", "short-length", "short-body"])
@async_test
async def test_malformed_packet_is_a_protocol_error(stream, match):
    with pytest.raises(ProtocolError, match=match):
        await wire.aread_packet(stream_reader(stream))
