"""evpipe wire format: encode/decode for the byte stream over stdin/stdout.

All multi-byte fields are little-endian and fixed-width. The session
opens with a magic + version + device-descriptor list; every subsequent
packet is length-prefixed (u16) and discriminated by a 1-byte kind tag.

Two surfaces are exposed:
  * stream functions (``write_session_open``, ``write_event``, ...,
    ``read_session_open``, ``read_packet``) -- used by the sender/receiver
    against stdin/stdout.
  * pure-bytes helpers (``encode_event``, ``decode_event``, ...) -- used
    by the unit tests and by the sender to batch frames into a single
    write where useful.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import BinaryIO

MAGIC = b"EVPW"
VERSION = 1

# Packet kinds (after the 2-byte length prefix).
PACKET_EVENT = 1
PACKET_FULL_STATE = 2
PACKET_HEARTBEAT = 3

# Event sub-kinds (in the event payload's first byte).
EV_KEY = 1
EV_REL = 2
EV_ABS = 3
EV_SYN = 4

# Device kinds (in device descriptor).
DEV_KB = 1
DEV_MOUSE = 2
DEV_COMBO = 3
DEV_TABLET = 4


class ProtocolError(Exception):
    """Wire framing or session-open contract violation."""


@dataclass
class AbsAxis:
    usage: int
    min: int
    max: int
    resolution: int


@dataclass
class DeviceDescriptor:
    name: str
    vendor_id: int
    product_id: int
    kind: int
    keys: list[int] = field(default_factory=list)
    rel_axes: list[int] = field(default_factory=list)
    abs_axes: list[AbsAxis] = field(default_factory=list)


@dataclass
class Event:
    ev_kind: int
    code: int
    value: int
    timestamp_us: int
    device_id: int


# ---- bytes-level encoders -------------------------------------------------

_EVENT_STRUCT = struct.Struct("<BHiQH")  # ev_kind, code, value, ts, dev


def encode_event(ev: Event) -> bytes:
    return _EVENT_STRUCT.pack(
        ev.ev_kind, ev.code, ev.value, ev.timestamp_us, ev.device_id
    )


def decode_event(payload: bytes) -> Event:
    if len(payload) != _EVENT_STRUCT.size:
        raise ProtocolError(
            f"event payload size mismatch: {len(payload)} != {_EVENT_STRUCT.size}"
        )
    ev_kind, code, value, ts, dev = _EVENT_STRUCT.unpack(payload)
    return Event(ev_kind, code, value, ts, dev)


def encode_full_state(device_id: int, held: list[int]) -> bytes:
    return struct.pack("<HH", device_id, len(held)) + struct.pack(
        f"<{len(held)}H", *held
    )


def decode_full_state(payload: bytes) -> tuple[int, list[int]]:
    if len(payload) < 4:
        raise ProtocolError(f"full_state payload too short: {len(payload)}")
    device_id, nkeys = struct.unpack_from("<HH", payload, 0)
    if len(payload) != 4 + 2 * nkeys:
        raise ProtocolError(
            f"full_state size mismatch: got {len(payload)}, expected {4 + 2 * nkeys}"
        )
    held = list(struct.unpack_from(f"<{nkeys}H", payload, 4))
    return device_id, held


def encode_descriptor(d: DeviceDescriptor) -> bytes:
    name_bytes = d.name.encode("utf-8")
    if len(name_bytes) > 0xFFFF:
        raise ProtocolError("device name too long")
    if len(d.keys) > 0xFFFF:
        raise ProtocolError("too many keys in descriptor")
    if len(d.rel_axes) > 0xFF or len(d.abs_axes) > 0xFF:
        raise ProtocolError("too many axes in descriptor")
    parts: list[bytes] = [
        struct.pack("<H", len(name_bytes)),
        name_bytes,
        struct.pack("<HHB", d.vendor_id, d.product_id, d.kind),
        struct.pack("<H", len(d.keys)),
        struct.pack(f"<{len(d.keys)}H", *d.keys),
        struct.pack("<B", len(d.rel_axes)),
        struct.pack(f"<{len(d.rel_axes)}B", *d.rel_axes),
        struct.pack("<B", len(d.abs_axes)),
    ]
    for ax in d.abs_axes:
        parts.append(struct.pack("<Hiii", ax.usage, ax.min, ax.max, ax.resolution))
    return b"".join(parts)


def decode_descriptor(data: bytes, off: int) -> tuple[DeviceDescriptor, int]:
    (name_len,) = struct.unpack_from("<H", data, off)
    off += 2
    name = data[off : off + name_len].decode("utf-8")
    off += name_len
    vid, pid, kind = struct.unpack_from("<HHB", data, off)
    off += 5
    (nkeys,) = struct.unpack_from("<H", data, off)
    off += 2
    keys = list(struct.unpack_from(f"<{nkeys}H", data, off))
    off += 2 * nkeys
    (nrel,) = struct.unpack_from("<B", data, off)
    off += 1
    rel_axes = list(struct.unpack_from(f"<{nrel}B", data, off))
    off += nrel
    (nabs,) = struct.unpack_from("<B", data, off)
    off += 1
    abs_axes: list[AbsAxis] = []
    for _ in range(nabs):
        usage, mn, mx, res = struct.unpack_from("<Hiii", data, off)
        off += 14
        abs_axes.append(AbsAxis(usage, mn, mx, res))
    return (
        DeviceDescriptor(
            name=name,
            vendor_id=vid,
            product_id=pid,
            kind=kind,
            keys=keys,
            rel_axes=rel_axes,
            abs_axes=abs_axes,
        ),
        off,
    )


def encode_session_open(devices: list[DeviceDescriptor]) -> bytes:
    parts = [MAGIC, struct.pack("<BBH", VERSION, 0, len(devices))]
    for d in devices:
        parts.append(encode_descriptor(d))
    return b"".join(parts)


def decode_session_open(data: bytes) -> tuple[list[DeviceDescriptor], int]:
    if len(data) < 8 or data[:4] != MAGIC:
        raise ProtocolError(f"bad magic: {data[:4]!r}")
    version, _, ndev = struct.unpack_from("<BBH", data, 4)
    if version != VERSION:
        raise ProtocolError(f"unsupported version: {version}")
    off = 8
    devices: list[DeviceDescriptor] = []
    for _ in range(ndev):
        d, off = decode_descriptor(data, off)
        devices.append(d)
    return devices, off


def encode_packet(kind: int, payload: bytes) -> bytes:
    body_len = 1 + len(payload)
    if body_len > 0xFFFF:
        raise ProtocolError(f"packet body too large: {body_len}")
    return struct.pack("<HB", body_len, kind) + payload


# ---- stream readers/writers ----------------------------------------------


def _read_exact(stream: BinaryIO, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            raise EOFError("short read")
        buf.extend(chunk)
    return bytes(buf)


def write_session_open(stream: BinaryIO, devices: list[DeviceDescriptor]) -> None:
    stream.write(encode_session_open(devices))
    stream.flush()


def read_session_open(stream: BinaryIO) -> list[DeviceDescriptor]:
    head = _read_exact(stream, 8)
    if head[:4] != MAGIC:
        raise ProtocolError(f"bad magic: {head[:4]!r}")
    version, _, ndev = struct.unpack_from("<BBH", head, 4)
    if version != VERSION:
        raise ProtocolError(f"unsupported version: {version}")
    devices: list[DeviceDescriptor] = []
    for _ in range(ndev):
        devices.append(_read_descriptor_stream(stream))
    return devices


def _read_descriptor_stream(stream: BinaryIO) -> DeviceDescriptor:
    (name_len,) = struct.unpack("<H", _read_exact(stream, 2))
    name = _read_exact(stream, name_len).decode("utf-8")
    vid, pid, kind = struct.unpack("<HHB", _read_exact(stream, 5))
    (nkeys,) = struct.unpack("<H", _read_exact(stream, 2))
    keys = list(struct.unpack(f"<{nkeys}H", _read_exact(stream, 2 * nkeys))) if nkeys else []
    (nrel,) = struct.unpack("<B", _read_exact(stream, 1))
    rel_axes = list(struct.unpack(f"<{nrel}B", _read_exact(stream, nrel))) if nrel else []
    (nabs,) = struct.unpack("<B", _read_exact(stream, 1))
    abs_axes: list[AbsAxis] = []
    for _ in range(nabs):
        usage, mn, mx, res = struct.unpack("<Hiii", _read_exact(stream, 14))
        abs_axes.append(AbsAxis(usage, mn, mx, res))
    return DeviceDescriptor(
        name=name,
        vendor_id=vid,
        product_id=pid,
        kind=kind,
        keys=keys,
        rel_axes=rel_axes,
        abs_axes=abs_axes,
    )


def write_event(stream: BinaryIO, ev: Event) -> None:
    stream.write(encode_packet(PACKET_EVENT, encode_event(ev)))


def write_full_state(stream: BinaryIO, device_id: int, held: list[int]) -> None:
    stream.write(encode_packet(PACKET_FULL_STATE, encode_full_state(device_id, held)))


def write_heartbeat(stream: BinaryIO) -> None:
    stream.write(encode_packet(PACKET_HEARTBEAT, b""))


def read_packet(stream: BinaryIO) -> tuple[int, bytes]:
    """Read one packet: returns (kind, payload). Raises EOFError at clean EOF."""
    head = _read_exact(stream, 2)
    (body_len,) = struct.unpack("<H", head)
    if body_len < 1:
        raise ProtocolError(f"zero-length packet body")
    body = _read_exact(stream, body_len)
    return body[0], body[1:]


# ---- async stream readers (StreamReader-backed) --------------------------


async def _aread_exact(reader, n: int) -> bytes:
    import asyncio

    try:
        return await reader.readexactly(n)
    except asyncio.IncompleteReadError as exc:
        if exc.partial:
            raise ProtocolError(f"truncated read: got {len(exc.partial)}/{n}") from exc
        raise EOFError("clean EOF") from exc


async def aread_session_open(reader) -> list[DeviceDescriptor]:
    head = await _aread_exact(reader, 8)
    if head[:4] != MAGIC:
        raise ProtocolError(f"bad magic: {head[:4]!r}")
    version, _, ndev = struct.unpack_from("<BBH", head, 4)
    if version != VERSION:
        raise ProtocolError(f"unsupported version: {version}")
    devices: list[DeviceDescriptor] = []
    for _ in range(ndev):
        devices.append(await _aread_descriptor(reader))
    return devices


async def _aread_descriptor(reader) -> DeviceDescriptor:
    (name_len,) = struct.unpack("<H", await _aread_exact(reader, 2))
    name = (await _aread_exact(reader, name_len)).decode("utf-8")
    vid, pid, kind = struct.unpack("<HHB", await _aread_exact(reader, 5))
    (nkeys,) = struct.unpack("<H", await _aread_exact(reader, 2))
    keys = (
        list(struct.unpack(f"<{nkeys}H", await _aread_exact(reader, 2 * nkeys)))
        if nkeys
        else []
    )
    (nrel,) = struct.unpack("<B", await _aread_exact(reader, 1))
    rel_axes = (
        list(struct.unpack(f"<{nrel}B", await _aread_exact(reader, nrel))) if nrel else []
    )
    (nabs,) = struct.unpack("<B", await _aread_exact(reader, 1))
    abs_axes: list[AbsAxis] = []
    for _ in range(nabs):
        usage, mn, mx, res = struct.unpack("<Hiii", await _aread_exact(reader, 14))
        abs_axes.append(AbsAxis(usage, mn, mx, res))
    return DeviceDescriptor(
        name=name,
        vendor_id=vid,
        product_id=pid,
        kind=kind,
        keys=keys,
        rel_axes=rel_axes,
        abs_axes=abs_axes,
    )


async def aread_packet(reader) -> tuple[int, bytes]:
    head = await _aread_exact(reader, 2)
    (body_len,) = struct.unpack("<H", head)
    if body_len < 1:
        raise ProtocolError("zero-length packet body")
    body = await _aread_exact(reader, body_len)
    return body[0], body[1:]
