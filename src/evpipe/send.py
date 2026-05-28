"""evpipe sender: capture local input devices and stream wire packets on stdout.

  * One read coroutine per source device. The fd stays open across
    toggle transitions; the EVIOCGRAB is what flips. When ungrabbed,
    our fd still receives every event (alongside the compositor) so
    chord detection works in both states.
  * Toggle is a multi-key chord -- modifiers held + trigger pressed.
    Detection runs inside the source read path and consumes the
    trigger event (so the key combo never reaches the receiver). The
    chord is queried against the kernel's `active_keys()` rather than
    a tracked held set, so it works whether or not we're currently
    grabbed.
  * A periodic resync coroutine emits FULL_STATE for every source on a
    fixed interval. That same packet doubles as a heartbeat -- the
    receiver treats absence as link death.

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
DEFAULT_TOGGLE_CHORD = ["KEY_LEFTMETA", "KEY_T"]


@dataclass
class Source:
    """One input device + its held-keys snapshots.

    Two held sets, both updated event-by-event in the read loop:

      * ``held`` -- HID wire codes; only mutated while grabbed; what we
        send to the receiver as FULL_STATE.
      * ``held_evdev`` -- raw evdev codes; mutated unconditionally;
        used by the toggle chord check, which has to work both
        grabbed and ungrabbed and must reflect the state at the moment
        of the trigger event (not whatever has since arrived at the
        kernel, which `active_keys` would return).
    """

    path: str
    role: int  # wire.DEV_KB / DEV_MOUSE / DEV_COMBO
    device_id: int
    dev: evdev.InputDevice
    descriptor: wire.DeviceDescriptor
    held: set[int] = field(default_factory=set)
    held_evdev: set[int] = field(default_factory=set)
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
        toggle_chord_evdev: list[int],
        stdout: BinaryIO,
        resync_interval_s: float = DEFAULT_RESYNC_INTERVAL_S,
        start_forwarding: bool = True,
    ) -> None:
        self.source_paths = source_paths
        self.stdout = stdout
        self.resync_interval_s = resync_interval_s
        self.forwarding_on = start_forwarding
        self.sources: list[Source] = []
        self.shutting_down = asyncio.Event()
        self._t0 = time.monotonic()
        self._write_lock = asyncio.Lock()
        self._toggle_pending = False  # debounce flag
        # Chord state. Empty list disables toggling (always-on).
        self.toggle_chord_evdev = list(toggle_chord_evdev)
        self.toggle_modifiers_evdev: set[int] = (
            set(self.toggle_chord_evdev[:-1]) if self.toggle_chord_evdev else set()
        )
        self.toggle_trigger_evdev: Optional[int] = (
            self.toggle_chord_evdev[-1] if self.toggle_chord_evdev else None
        )
        self.toggle_chord_hid: set[int] = set()
        for code in self.toggle_chord_evdev:
            u = hid_map.encode_key(code)
            if u is not None:
                self.toggle_chord_hid.add(u)

    def shutdown(self) -> None:
        self.shutting_down.set()

    def _ts_us(self) -> int:
        return int((time.monotonic() - self._t0) * 1_000_000)

    async def run(self) -> None:
        self._open_sources()
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
            try:
                initial_held = set(dev.active_keys())
            except OSError:
                initial_held = set()
            self.sources.append(Source(
                path=path, role=role, device_id=idx, dev=dev, descriptor=desc,
                held_evdev=initial_held,
            ))
            logger.info("source %d: %s (%s) keys=%d rels=%d",
                        idx, path, dev.name, len(desc.keys), len(desc.rel_axes))

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
        # The fd is open at all times; the grab state flips. While
        # ungrabbed, our reader still receives every event (alongside
        # the compositor) so the chord check below works in both states.
        try:
            async for event in src.dev.async_read_loop():
                await self._dispatch_event(src, event)
        except OSError as exc:
            logger.info("source %s closed: %s", src.path, exc)
            self.shutting_down.set()

    def _chord_fires(self, src: Source, ev_code: int, ev_value: int) -> bool:
        """True iff this event is the toggle trigger press AND every
        configured modifier was held when the trigger arrived. Checked
        against our own ``held_evdev`` rather than `active_keys`, so a
        modifier release queued behind the trigger doesn't hide the
        chord."""
        if self.toggle_trigger_evdev is None:
            return False
        if ev_value != 1 or ev_code != self.toggle_trigger_evdev:
            return False
        if self.toggle_modifiers_evdev.issubset(src.held_evdev):
            return True
        missing = self.toggle_modifiers_evdev - src.held_evdev
        logger.debug(
            "toggle trigger pressed but chord incomplete: missing=%s held=%s",
            sorted(missing), sorted(src.held_evdev),
        )
        return False

    async def _dispatch_event(self, src: Source, event: evdev.InputEvent) -> None:
        if event.type == e.EV_KEY:
            # Chord must be checked against the pre-event held set --
            # before we record the trigger as held -- so the modifiers
            # alone are what's being matched.
            if self._chord_fires(src, event.code, event.value):
                # The trigger never reaches the receiver, and never makes
                # it into held_evdev either.
                await self._toggle(src)
                return
            if event.value == 1:
                src.held_evdev.add(event.code)
            elif event.value == 0:
                src.held_evdev.discard(event.code)
            u = hid_map.encode_key(event.code)
            if u is None:
                return
            if not src.grabbed:
                return
            if event.value == 1:
                src.held.add(u)
            elif event.value == 0:
                src.held.discard(u)
            await self._emit_event(wire.EV_KEY, u, event.value, src.device_id)
        elif event.type == e.EV_REL:
            if not src.grabbed:
                return
            axis = hid_map.encode_rel(event.code)
            if axis is None:
                return
            await self._emit_event(wire.EV_REL, axis, event.value, src.device_id)
        elif event.type == e.EV_SYN and event.code == e.SYN_REPORT:
            if not src.grabbed:
                return
            await self._emit_event(wire.EV_SYN, 0, 0, src.device_id)
        # EV_ABS, EV_MSC, EV_LED, etc. dropped for v1.

    async def _toggle(self, triggering_src: Source) -> None:
        if self._toggle_pending:
            return
        self._toggle_pending = True
        try:
            if self.forwarding_on:
                # ON -> OFF. Drop the held set and emit an empty FULL_STATE
                # so the receiver releases anything outstanding before we
                # ungrab. The user's actual key releases that follow happen
                # locally on B; the receiver never sees them.
                for src in self.sources:
                    src.held.clear()
                    await self._emit_full_state(src)
                self._ungrab_all()
                self.forwarding_on = False
                logger.info("forwarding OFF")
            else:
                # OFF -> ON. Grab, then reseed the receiver with whatever
                # the user is still holding -- minus the chord keys, which
                # we explicitly do not want to leak onto A.
                self._grab_all()
                self.forwarding_on = True
                for src in self.sources:
                    self._seed_held_from_kernel(src)
                    await self._emit_full_state(src)
                logger.info("forwarding ON")
        finally:
            self._toggle_pending = False

    def _seed_held_from_kernel(self, src: Source) -> None:
        """Repopulate `src.held` from the kernel's view, excluding any
        chord keys (the chord was just used to flip state, not as input)."""
        src.held.clear()
        if not src.grabbed:
            return
        try:
            active = src.dev.active_keys()
        except OSError:
            return
        for code in active:
            u = hid_map.encode_key(code)
            if u is None or u in self.toggle_chord_hid:
                continue
            src.held.add(u)

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


def _resolve_evdev_key(name: str) -> int:
    code = getattr(e, name, None)
    if not isinstance(code, int):
        raise argparse.ArgumentTypeError(f"unknown evdev key: {name}")
    return code


def _parse_chord(spec: str) -> list[int]:
    """Parse a comma- or plus-separated chord into evdev codes.

    Examples: ``KEY_LEFTCTRL,KEY_LEFTALT,KEY_T`` or
    ``KEY_LEFTCTRL+KEY_LEFTALT+KEY_T``. Last key is the trigger; the rest
    must be held when it transitions to pressed. Empty string disables
    toggling entirely.
    """
    spec = spec.strip()
    if not spec:
        return []
    sep = "," if "," in spec else "+"
    names = [p.strip() for p in spec.split(sep) if p.strip()]
    return [_resolve_evdev_key(n) for n in names]


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
    parser.add_argument("--toggle-chord",
                        default=",".join(DEFAULT_TOGGLE_CHORD),
                        metavar="K1,K2,...,TRIGGER",
                        help="comma- or plus-separated evdev key names. The "
                             "last key is the trigger; the rest must be held "
                             "when it presses. The trigger event is consumed "
                             "(never forwarded). Pass an empty string to "
                             "disable toggling entirely. Default: "
                             + ",".join(DEFAULT_TOGGLE_CHORD)
                             + ". Run with --log-level=DEBUG to see which "
                             "modifiers were missing when a chord fails to fire.")
    parser.add_argument("--resync-interval", type=float,
                        default=DEFAULT_RESYNC_INTERVAL_S, metavar="SECONDS",
                        help="FULL_STATE / heartbeat cadence (default: "
                             f"{DEFAULT_RESYNC_INTERVAL_S}s).")
    parser.add_argument("--start-off", action="store_true",
                        help="start with forwarding OFF; press the toggle chord "
                             "to enable. Pairs with --toggle-chord.")
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

    toggle_chord = _parse_chord(args.toggle_chord)
    if args.start_off and not toggle_chord:
        parser.error("--start-off requires a non-empty --toggle-chord")

    # Default: SIGPIPE delivers BrokenPipeError on writes (not termination)
    # so the try/finally in run() can ungrab everything before exit.
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)

    app = SenderApp(
        sources,
        toggle_chord_evdev=toggle_chord,
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
