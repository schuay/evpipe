"""evpipe receiver: read wire packets from stdin and inject them locally via uinput.

The receiver mirrors the sender's session-open / packet-stream layout
back into uinput: one virtual evdev device per source descriptor, with
its capabilities reconstructed by inverting the sender's translation
through `hid_map`.

Held-key tracking per device is the load-bearing piece for safety:
on EOF, heartbeat timeout, or signal we walk every tracked code and
emit a release event before destroying the virtual device, so stuck
modifiers on disconnect can't outlive the link.

FULL_STATE packets act both as the resync mechanism (sender's view of
what's held, receiver converges via diff) and as the heartbeat. Two
thresholds: at 1.5s without traffic we release every tracked-held
code (safety -- no stuck modifiers if the sender has actually died)
but keep the uinput devices alive; if traffic resumes the next
FULL_STATE diff re-presses anything still held. Only at 10s do we
declare the link dead and tear the session down. The 10s threshold
matches the recommended ssh `ServerAliveInterval=5 ServerAliveCountMax=2`
timeout, so sub-10s blips don't kill an otherwise-recoverable link.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time
from typing import Optional

import evdev
import evdev.ecodes as e

from . import hid_map, wire

logger = logging.getLogger("evpipe-recv")

HEARTBEAT_SAFETY_RELEASE_S = 1.5
HEARTBEAT_DEAD_S = 10.0
HEARTBEAT_CHECK_S = 0.25
UINPUT_NAME_PREFIX = "evpipe: "


class ReceiverApp:
    def __init__(self) -> None:
        self.uinputs: list[evdev.UInput] = []
        self.descriptors: list[wire.DeviceDescriptor] = []
        # Held HID wire codes per source device_id. Authoritative for the
        # all-up flush on shutdown.
        self.held_per_dev: list[set[int]] = []
        self.shutting_down = asyncio.Event()
        self._last_packet = time.monotonic()

    def shutdown(self) -> None:
        self.shutting_down.set()

    async def run(self, reader: asyncio.StreamReader) -> int:
        try:
            self.descriptors = await wire.aread_session_open(reader)
        except (EOFError, wire.ProtocolError) as exc:
            logger.error("session_open failed: %s", exc)
            return 1
        try:
            for d in self.descriptors:
                self._create_uinput(d)
        except (OSError, evdev.UInputError) as exc:
            logger.error("uinput create failed: %s", exc)
            self._teardown()
            return 1
        logger.info("session open: %d virtual device(s)", len(self.uinputs))

        self._last_packet = time.monotonic()
        try:
            await asyncio.gather(
                self._read_loop(reader),
                self._heartbeat_loop(),
            )
        finally:
            await self._teardown()
        return 0

    def _create_uinput(self, d: wire.DeviceDescriptor) -> None:
        ev_keys: list[int] = []
        for hid_u in d.keys:
            ec = hid_map.decode_key(hid_u)
            if ec is not None:
                ev_keys.append(ec)
        ev_rels: list[int] = []
        for axis in d.rel_axes:
            ec = hid_map.decode_rel(axis)
            if ec is not None:
                ev_rels.append(ec)
        caps: dict[int, list[int]] = {}
        if ev_keys:
            caps[e.EV_KEY] = ev_keys
        if ev_rels:
            caps[e.EV_REL] = ev_rels
        if not caps:
            raise wire.ProtocolError(
                f"descriptor for {d.name!r} has no usable keys/rels"
            )
        ui = evdev.UInput(
            caps,
            name=f"{UINPUT_NAME_PREFIX}{d.name}",
            vendor=d.vendor_id,
            product=d.product_id,
            phys="evpipe",
        )
        self.uinputs.append(ui)
        self.held_per_dev.append(set())
        logger.info(
            "uinput %d: %r keys=%d rels=%d",
            len(self.uinputs) - 1, ui.name, len(ev_keys), len(ev_rels),
        )

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        while not self.shutting_down.is_set():
            try:
                kind, payload = await wire.aread_packet(reader)
            except EOFError:
                logger.info("upstream EOF")
                self.shutting_down.set()
                return
            except wire.ProtocolError as exc:
                logger.error("protocol error: %s", exc)
                self.shutting_down.set()
                return
            self._last_packet = time.monotonic()
            self._handle_packet(kind, payload)

    def _handle_packet(self, kind: int, payload: bytes) -> None:
        if kind == wire.PACKET_EVENT:
            try:
                ev = wire.decode_event(payload)
            except wire.ProtocolError as exc:
                logger.warning("bad event packet: %s", exc)
                return
            self._inject(ev)
        elif kind == wire.PACKET_FULL_STATE:
            try:
                dev_id, held = wire.decode_full_state(payload)
            except wire.ProtocolError as exc:
                logger.warning("bad full_state packet: %s", exc)
                return
            self._converge(dev_id, set(held))
        elif kind == wire.PACKET_HEARTBEAT:
            pass
        else:
            logger.warning("unknown packet kind: %d", kind)

    def _inject(self, ev: wire.Event) -> None:
        if not 0 <= ev.device_id < len(self.uinputs):
            logger.warning("event device_id=%d out of range", ev.device_id)
            return
        ui = self.uinputs[ev.device_id]
        if ev.ev_kind == wire.EV_KEY:
            ec = hid_map.decode_key(ev.code)
            if ec is None:
                return
            ui.write(e.EV_KEY, ec, ev.value)
            held = self.held_per_dev[ev.device_id]
            if ev.value == 1:
                held.add(ev.code)
            elif ev.value == 0:
                held.discard(ev.code)
        elif ev.ev_kind == wire.EV_REL:
            ec = hid_map.decode_rel(ev.code)
            if ec is None:
                return
            ui.write(e.EV_REL, ec, ev.value)
        elif ev.ev_kind == wire.EV_SYN:
            ui.syn()
        # EV_ABS deferred.

    def _converge(self, dev_id: int, expected: set[int]) -> None:
        if not 0 <= dev_id < len(self.uinputs):
            logger.warning("full_state dev_id=%d out of range", dev_id)
            return
        ui = self.uinputs[dev_id]
        held = self.held_per_dev[dev_id]
        if held == expected:
            return
        # Release codes the sender no longer reports held.
        for code in held - expected:
            ec = hid_map.decode_key(code)
            if ec is not None:
                ui.write(e.EV_KEY, ec, 0)
        # Press codes the sender says are held that we don't have.
        for code in expected - held:
            ec = hid_map.decode_key(code)
            if ec is not None:
                ui.write(e.EV_KEY, ec, 1)
        ui.syn()
        self.held_per_dev[dev_id] = set(expected)

    async def _heartbeat_loop(self) -> None:
        released = False
        while not self.shutting_down.is_set():
            try:
                await asyncio.wait_for(
                    self.shutting_down.wait(), timeout=HEARTBEAT_CHECK_S,
                )
                return
            except asyncio.TimeoutError:
                pass
            elapsed = time.monotonic() - self._last_packet
            if elapsed > HEARTBEAT_DEAD_S:
                logger.warning(
                    "no traffic for %.1fs; treating link as dead",
                    elapsed,
                )
                self.shutting_down.set()
                return
            if elapsed > HEARTBEAT_SAFETY_RELEASE_S:
                if not released:
                    logger.warning(
                        "no traffic for %.1fs; releasing held keys "
                        "(link may still recover)",
                        elapsed,
                    )
                    self._release_held()
                    released = True
            else:
                released = False

    def _release_held(self) -> None:
        """Release every tracked-held code on every uinput. Idempotent.

        Used both by the safety branch in `_heartbeat_loop` (uinputs stay
        alive; recovery is via the next FULL_STATE diff) and by the final
        flush in `_teardown`.
        """
        for idx, ui in enumerate(self.uinputs):
            if idx >= len(self.held_per_dev):
                continue
            held = self.held_per_dev[idx]
            for code in list(held):
                ec = hid_map.decode_key(code)
                if ec is not None:
                    try:
                        ui.write(e.EV_KEY, ec, 0)
                    except Exception:
                        pass
            try:
                ui.syn()
            except Exception:
                pass
            held.clear()

    async def _teardown(self) -> None:
        """Final all-up flush, then destroy uinput devices.

        The small sleep between the release flush and `ui.close()` lets
        the kernel propagate the synthetic releases to any waiting
        readers (compositor, plus our integration tests) before the
        uinput device is torn down.
        """
        self._release_held()
        await asyncio.sleep(0.05)
        for ui in self.uinputs:
            try:
                ui.close()
            except Exception:
                pass
        self.uinputs.clear()


async def _stdin_reader() -> asyncio.StreamReader:
    """Wrap sys.stdin.buffer as an asyncio.StreamReader.

    asyncio.connect_read_pipe expects a pipe-style fd; stdin satisfies
    that on Linux. The protocol does nothing beyond proxying to the
    reader -- we don't need any callback hooks here.
    """
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin.buffer)
    return reader


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="evpipe-recv",
        description="Read evpipe packets from stdin and inject them locally "
                    "via uinput.",
    )
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
    app = ReceiverApp()
    return asyncio.run(_amain(app))


async def _amain(app: ReceiverApp) -> int:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        loop.add_signal_handler(sig, app.shutdown)
    reader = await _stdin_reader()
    return await app.run(reader)


if __name__ == "__main__":
    sys.exit(main())
