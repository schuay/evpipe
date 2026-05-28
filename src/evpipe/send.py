"""evpipe sender: capture local input devices and stream wire packets on stdout.

Architecture mirrors the QMK mouse bridge:

  * One read coroutine per grabbed evdev source, plus a non-grabbed
    monitor on the toggle device.
  * A periodic resync coroutine emits FULL_STATE for every source on a
    fixed interval. That same packet doubles as a heartbeat -- the
    receiver treats absence as link death.
  * `forwarding_on` is the master switch. While ON, source devices are
    `EVIOCGRAB`ed and events flow. While OFF, the grabs are released
    (events fall through to the local compositor as usual) and the read
    coroutines idle. Transitions are driven exclusively by the toggle
    monitor.

Teardown contract:
  every grabbed device gets ungrabbed before exit, no matter how we
  arrive there (signal, BrokenPipeError on stdout, source-device close,
  unhandled exception). The top-level `try/finally` in `run()` plus the
  signal handlers installed in `_amain()` are the only two paths into
  that finally block.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import BinaryIO, Optional

import evdev
import evdev.ecodes as e

from . import hid_map, wire

logger = logging.getLogger("evpipe-send")

DEFAULT_RESYNC_INTERVAL_S = 0.5
DEFAULT_TOGGLE_KEY_NAME = "KEY_SCROLLLOCK"


@dataclass
class Source:
    """One grabbed input device + its current held-keys snapshot.

    Held keys are tracked as HID wire codes (the post-translation form)
    so the resync packet can be built without a second round of map
    lookups. Each press/release event mutates the set inline.
    """

    path: str
    role: int  # wire.DEV_KB / DEV_MOUSE / DEV_COMBO
    device_id: int
    dev: evdev.InputDevice
    descriptor: wire.DeviceDescriptor
    held: set[int] = field(default_factory=set)
    grabbed: bool = False


def _build_descriptor(dev: evdev.InputDevice, role: int) -> wire.DeviceDescriptor:
    caps = dev.capabilities()
    keys_evdev = caps.get(e.EV_KEY, [])
    rels_evdev = caps.get(e.EV_REL, [])
    hid_keys: list[int] = []
    dropped_keys = 0
    for k in keys_evdev:
        u = hid_map.encode_key(k)
        if u is not None:
            hid_keys.append(u)
        else:
            dropped_keys += 1
    rel_axes: list[int] = []
    for r in rels_evdev:
        a = hid_map.encode_rel(r)
        if a is not None:
            rel_axes.append(a)
    if dropped_keys:
        logger.debug(
            "device %s: %d evdev keys not in HID table, dropped from descriptor",
            dev.path, dropped_keys,
        )
    return wire.DeviceDescriptor(
        name=dev.name,
        vendor_id=dev.info.vendor & 0xFFFF,
        product_id=dev.info.product & 0xFFFF,
        kind=role,
        keys=hid_keys,
        rel_axes=rel_axes,
        abs_axes=[],
    )


class SenderApp:
    def __init__(
        self,
        source_paths: list[tuple[str, int]],
        toggle_path: Optional[str],
        toggle_code: Optional[int],
        stdout: BinaryIO,
        resync_interval_s: float = DEFAULT_RESYNC_INTERVAL_S,
        start_forwarding: bool = True,
    ) -> None:
        self.source_paths = source_paths
        self.toggle_path = toggle_path
        self.toggle_code = toggle_code
        self.stdout = stdout
        self.resync_interval_s = resync_interval_s
        self.forwarding_on = start_forwarding
        self.sources: list[Source] = []
        self.toggle_dev: Optional[evdev.InputDevice] = None
        self.shutting_down = asyncio.Event()
        self._t0 = time.monotonic()
        self._write_lock = asyncio.Lock()
        self._toggle_pending = False  # debounce flag

    def shutdown(self) -> None:
        self.shutting_down.set()

    def _ts_us(self) -> int:
        return int((time.monotonic() - self._t0) * 1_000_000)

    async def run(self) -> None:
        self._open_sources()
        if self.toggle_path:
            self._open_toggle()
        try:
            self._write_session_open()
        except (BrokenPipeError, OSError) as exc:
            logger.error("downstream closed before session_open: %s", exc)
            self._teardown()
            return
        if self.forwarding_on:
            self._grab_all()
            logger.info("forwarding ON at startup")

        tasks: list[asyncio.Task] = []
        for src in self.sources:
            tasks.append(asyncio.create_task(self._read_source(src),
                                             name=f"src:{src.path}"))
        if self.toggle_dev is not None:
            tasks.append(asyncio.create_task(self._read_toggle(),
                                             name="toggle"))
        tasks.append(asyncio.create_task(self._resync_loop(), name="resync"))

        try:
            await self.shutting_down.wait()
        finally:
            for t in tasks:
                t.cancel()
            for t in tasks:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            self._teardown()

    def _open_sources(self) -> None:
        for idx, (path, role) in enumerate(self.source_paths):
            dev = evdev.InputDevice(path)
            desc = _build_descriptor(dev, role)
            self.sources.append(Source(
                path=path, role=role, device_id=idx, dev=dev, descriptor=desc,
            ))
            logger.info("source %d: %s (%s) keys=%d rels=%d",
                        idx, path, dev.name, len(desc.keys), len(desc.rel_axes))

    def _open_toggle(self) -> None:
        if self.toggle_path is None:
            return
        # The toggle device is intentionally not grabbed: events pass through
        # to the local compositor in addition to being seen by us.
        self.toggle_dev = evdev.InputDevice(self.toggle_path)
        logger.info("toggle device: %s (%s)", self.toggle_path, self.toggle_dev.name)

    def _write_session_open(self) -> None:
        descs = [s.descriptor for s in self.sources]
        wire.write_session_open(self.stdout, descs)

    def _grab_all(self) -> None:
        for src in self.sources:
            if src.grabbed:
                continue
            try:
                src.dev.grab()
                src.grabbed = True
            except OSError as exc:
                logger.warning("grab failed on %s: %s", src.path, exc)

    def _ungrab_all(self) -> None:
        for src in self.sources:
            if not src.grabbed:
                continue
            try:
                src.dev.ungrab()
            except OSError:
                pass
            src.grabbed = False

    def _teardown(self) -> None:
        self._ungrab_all()
        for src in self.sources:
            try:
                src.dev.close()
            except Exception:
                pass
        if self.toggle_dev is not None:
            try:
                self.toggle_dev.close()
            except Exception:
                pass

    def _write_bytes(self, data: bytes) -> bool:
        """Synchronous write to stdout; returns False on BrokenPipeError.

        The lock is held only during a single write so concurrent source
        readers can't interleave bytes mid-frame.
        """
        try:
            self.stdout.write(data)
            self.stdout.flush()
            return True
        except (BrokenPipeError, OSError) as exc:
            logger.info("downstream closed (%s); shutting down", exc)
            self.shutting_down.set()
            return False

    async def _emit_event(self, ev_kind: int, code: int, value: int, device_id: int) -> None:
        ev = wire.Event(ev_kind, code, value, self._ts_us(), device_id)
        data = wire.encode_packet(wire.PACKET_EVENT, wire.encode_event(ev))
        async with self._write_lock:
            self._write_bytes(data)

    async def _emit_full_state(self, src: Source) -> None:
        # While grabbed, sample the kernel's view of held keys for ground
        # truth -- our `held` set can drift across SYN_DROPPED etc.
        held_hid = set(src.held)
        if src.grabbed:
            try:
                active = src.dev.active_keys()
                held_hid = set()
                for code in active:
                    u = hid_map.encode_key(code)
                    if u is not None:
                        held_hid.add(u)
                src.held = set(held_hid)
            except OSError:
                pass
        data = wire.encode_packet(
            wire.PACKET_FULL_STATE,
            wire.encode_full_state(src.device_id, sorted(held_hid)),
        )
        async with self._write_lock:
            self._write_bytes(data)

    async def _read_source(self, src: Source) -> None:
        try:
            async for event in src.dev.async_read_loop():
                if not src.grabbed:
                    # Toggle flipped us off; drop the event. Held set will
                    # be reset by the FULL_STATE we sent at toggle-off
                    # transition.
                    continue
                await self._dispatch_event(src, event)
        except OSError as exc:
            logger.info("source %s closed: %s", src.path, exc)
            self.shutting_down.set()

    async def _dispatch_event(self, src: Source, event: evdev.InputEvent) -> None:
        if event.type == e.EV_KEY:
            u = hid_map.encode_key(event.code)
            if u is None:
                return
            if event.value == 1:
                src.held.add(u)
            elif event.value == 0:
                src.held.discard(u)
            await self._emit_event(wire.EV_KEY, u, event.value, src.device_id)
        elif event.type == e.EV_REL:
            axis = hid_map.encode_rel(event.code)
            if axis is None:
                return
            await self._emit_event(wire.EV_REL, axis, event.value, src.device_id)
        elif event.type == e.EV_SYN and event.code == e.SYN_REPORT:
            await self._emit_event(wire.EV_SYN, 0, 0, src.device_id)
        # EV_ABS, EV_MSC, EV_LED, etc. dropped for v1.

    async def _read_toggle(self) -> None:
        assert self.toggle_dev is not None
        try:
            async for event in self.toggle_dev.async_read_loop():
                if (
                    event.type == e.EV_KEY
                    and event.code == self.toggle_code
                    and event.value == 1
                ):
                    await self._toggle()
        except OSError as exc:
            logger.warning("toggle device closed: %s", exc)
            self.shutting_down.set()

    async def _toggle(self) -> None:
        if self._toggle_pending:
            return
        self._toggle_pending = True
        try:
            if self.forwarding_on:
                # Send all-up first so the receiver releases everything before
                # we lose our grip on the source devices.
                for src in self.sources:
                    src.held.clear()
                    await self._emit_full_state(src)
                self._ungrab_all()
                self.forwarding_on = False
                logger.info("forwarding OFF")
            else:
                self._grab_all()
                self.forwarding_on = True
                # Reseed receiver state with whatever was held at grab-time.
                for src in self.sources:
                    await self._emit_full_state(src)
                logger.info("forwarding ON")
        finally:
            self._toggle_pending = False

    async def _resync_loop(self) -> None:
        while not self.shutting_down.is_set():
            try:
                await asyncio.wait_for(
                    self.shutting_down.wait(), timeout=self.resync_interval_s,
                )
                return
            except asyncio.TimeoutError:
                pass
            for src in self.sources:
                await self._emit_full_state(src)


def _resolve_toggle_key(name: str) -> int:
    """Map an evdev key name (e.g. KEY_SCROLLLOCK) to its code."""
    code = getattr(e, name, None)
    if not isinstance(code, int):
        raise argparse.ArgumentTypeError(f"unknown evdev key: {name}")
    return code


def list_input_devices() -> int:
    """Print every readable evdev node with a one-line summary. Helper for
    figuring out which paths to pass to --kb / --mouse."""
    for path in sorted(evdev.list_devices()):
        try:
            d = evdev.InputDevice(path)
        except OSError as exc:
            print(f"{path}  <unreadable: {exc}>")
            continue
        caps = d.capabilities()
        flags = []
        if e.EV_KEY in caps:
            flags.append(f"keys={len(caps[e.EV_KEY])}")
        if e.EV_REL in caps:
            flags.append(f"rels={len(caps[e.EV_REL])}")
        if e.EV_ABS in caps:
            flags.append(f"abs={len(caps[e.EV_ABS])}")
        print(f"{path}  {d.name!r}  {' '.join(flags)}")
        d.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="evpipe-send",
        description="Capture local input devices and stream them on stdout. "
                    "Pipe through ssh to evpipe-recv on the destination host.",
    )
    parser.add_argument("--kb", action="append", default=[], metavar="PATH",
                        help="keyboard /dev/input/event* node. Repeatable.")
    parser.add_argument("--mouse", action="append", default=[], metavar="PATH",
                        help="mouse /dev/input/event* node. Repeatable.")
    parser.add_argument("--device", action="append", default=[], metavar="PATH",
                        help="combo (kb+mouse, tablet, ...) node. Repeatable. "
                             "Use when the host kb endpoint emits both keys and buttons.")
    parser.add_argument("--toggle-device", default=None, metavar="PATH",
                        help="non-grabbed monitor for the toggle chord (typically "
                             "the QMK kb endpoint). If omitted, forwarding is "
                             "always on.")
    parser.add_argument("--toggle-key", default=DEFAULT_TOGGLE_KEY_NAME,
                        help=f"evdev key name to flip forwarding "
                             f"(default: {DEFAULT_TOGGLE_KEY_NAME}).")
    parser.add_argument("--resync-interval", type=float,
                        default=DEFAULT_RESYNC_INTERVAL_S, metavar="SECONDS",
                        help="FULL_STATE / heartbeat cadence (default: "
                             f"{DEFAULT_RESYNC_INTERVAL_S}s).")
    parser.add_argument("--start-off", action="store_true",
                        help="start with forwarding OFF; toggle key to enable. "
                             "Useless without --toggle-device.")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--list-devices", action="store_true",
                        help="print every evdev node with a short summary and exit.")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    if args.list_devices:
        return list_input_devices()

    sources: list[tuple[str, int]] = []
    for p in args.kb:
        sources.append((p, wire.DEV_KB))
    for p in args.mouse:
        sources.append((p, wire.DEV_MOUSE))
    for p in args.device:
        sources.append((p, wire.DEV_COMBO))
    if not sources:
        parser.error("no source devices specified (--kb / --mouse / --device)")

    toggle_code: Optional[int] = None
    if args.toggle_device:
        toggle_code = _resolve_toggle_key(args.toggle_key)

    # Default: SIGPIPE delivers BrokenPipeError on writes (not termination)
    # so the try/finally in run() can ungrab everything before exit.
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)

    app = SenderApp(
        sources,
        toggle_path=args.toggle_device,
        toggle_code=toggle_code,
        stdout=sys.stdout.buffer,
        resync_interval_s=args.resync_interval,
        start_forwarding=not args.start_off,
    )
    asyncio.run(_amain(app))
    return 0


async def _amain(app: SenderApp) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        loop.add_signal_handler(sig, app.shutdown)
    await app.run()


if __name__ == "__main__":
    sys.exit(main())
