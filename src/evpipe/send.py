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
import json
import logging
import os
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
DEFAULT_TOGGLE_CHORD = ["KEY_F10"]


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
    # evdev codes whose next release must be swallowed (not forwarded). An
    # action chord fires on the trigger press and consumes it; the matching
    # release would otherwise leak to the receiver as a lone key-up.
    swallow_release: set[int] = field(default_factory=set)


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
        action_chords: Optional[list[tuple[list[int], str]]] = None,
        dictation_socket: Optional[str] = None,
        local_only_evdev: Optional[list[int]] = None,
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
        # Action chords: (chord_evdev_codes, shell_command). Each fires like the
        # toggle chord (modifiers held + trigger press) but runs a shell command
        # instead of flipping forwarding, and ONLY while grabbed -- ungrabbed,
        # the local compositor still sees the key and runs its own binding, so
        # firing here too would double-trigger. Indexed by trigger code for the
        # dispatch-path lookup; a trigger may not be shared across chords.
        self.action_chords: list[tuple[list[int], str]] = list(action_chords or [])
        self._action_by_trigger: dict[int, tuple[set[int], str]] = {}
        for codes, cmd in self.action_chords:
            if not codes:
                continue
            self._action_by_trigger[codes[-1]] = (set(codes[:-1]), cmd)
        self.dictation_socket_path = dictation_socket
        # Keys that update chord state but are never forwarded to the receiver,
        # nor included in the FULL_STATE resync snapshot. Used to keep a
        # modifier (e.g. LEFTMETA) out of a bare-modifier chord like Super+D
        # from tapping the remote compositor's overview: the modifier reaches
        # the receiver before the trigger reveals the chord, and the resync
        # would re-send it even if that eager press were suppressed.
        # TODO(jgruber): this drops the key on the remote entirely (no
        # Super+Tab, no intentional overview on A). If we ever want the key
        # usable there, replace this with a bare-key action chord (no modifier)
        # or buffer the modifier press until chord resolution -- see the design
        # discussion; the latter must also gate the resync path.
        self._local_only_hid: set[int] = set()
        for code in (local_only_evdev or []):
            u = hid_map.encode_key(code)
            if u is not None:
                self._local_only_hid.add(u)
        # Strong references to fire-and-forget tasks (subprocess reapers).
        # asyncio only holds a weak reference to running tasks, so without
        # this a bare create_task() can be GC'd mid-flight; the done-callback
        # drops the entry once the task finishes.
        self._background_tasks: set[asyncio.Task] = set()
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
        # Deferred toggle state. While `_pending_release` is non-empty we have
        # decided to flip forwarding but are holding the grab boundary until
        # these evdev key-ups arrive, so neither B's compositor nor A is left
        # with a key stuck pressed. `_pending_grab` selects the boundary:
        # True -> grab on completion (OFF->ON), False -> ungrab (ON->OFF).
        self._pending_release: set[int] = set()
        self._pending_grab = False

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

        server = None
        try:
            # Inside the try so a socket-bind failure still hits teardown:
            # by now the sources may be grabbed, and the teardown contract
            # requires ungrabbing on every exit path.
            server = await self._start_dictation_server()
            await self.shutting_down.wait()
        finally:
            for t in tasks:
                t.cancel()
            for t in tasks:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            if server is not None:
                server.close()
                try:
                    await server.wait_closed()
                except Exception:
                    pass
                try:
                    os.unlink(self.dictation_socket_path)
                except OSError:
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
        data = self._event_bytes(ev_kind, code, value, device_id)
        async with self._write_lock:
            self._write_bytes(data)

    async def _emit_full_state(self, src: Source) -> None:
        """Tell the receiver what should be held.

        While forwarding, kernel `active_keys()` is ground truth -- our
        own `src.held` set can drift across SYN_DROPPED etc.

        While not forwarding, the answer is unconditionally nothing.
        Crucially we do *not* resample active_keys in that branch even
        though `src.grabbed` may still be True (toggle ON->OFF holds the
        grab until trigger release): the user's finger is on the trigger
        right now, and an active_keys snapshot would re-press it on the
        receiver -- then when the user does release it, the deferred-
        release branch consumes the release without forwarding, leaving
        the key stuck on A.
        """
        if not self.forwarding_on:
            src.held.clear()
            await self._send_full_state_packet(src.device_id, [])
            return
        held_hid = set(src.held)
        if src.grabbed:
            try:
                active = src.dev.active_keys()
                held_hid = set()
                for code in active:
                    u = hid_map.encode_key(code)
                    if u is not None and u not in self._local_only_hid:
                        held_hid.add(u)
                src.held = set(held_hid)
            except OSError:
                pass
        await self._send_full_state_packet(src.device_id, sorted(held_hid))

    async def _send_full_state_packet(self, device_id: int, held: list[int]) -> None:
        data = wire.encode_packet(
            wire.PACKET_FULL_STATE,
            wire.encode_full_state(device_id, held),
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

    def _action_command(self, src: Source, ev_code: int, ev_value: int) -> Optional[str]:
        """The shell command for an action chord firing on this event, or None.

        Fires only while grabbed: ungrabbed, the local compositor still sees
        the key and runs its own binding, so acting here too would double-
        trigger. Same modifier match as the toggle chord, against held_evdev."""
        if not src.grabbed or ev_value != 1:
            return None
        entry = self._action_by_trigger.get(ev_code)
        if entry is None:
            return None
        modifiers, cmd = entry
        if modifiers.issubset(src.held_evdev):
            return cmd
        logger.debug(
            "action trigger pressed but chord incomplete: missing=%s held=%s",
            sorted(modifiers - src.held_evdev), sorted(src.held_evdev),
        )
        return None

    async def _run_action(self, cmd: str) -> None:
        """Fire-and-forget a chord's shell command; never blocks dispatch."""
        try:
            proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except OSError as exc:
            logger.warning("action command failed to spawn: %s", exc)
            return
        # Reap it in the background so it can't become a zombie, without
        # holding up the read loop.
        self._spawn_background(proc.wait())

    def _spawn_background(self, coro) -> None:
        """Run a fire-and-forget coroutine, keeping a strong reference until
        it completes so the event loop can't GC it mid-flight."""
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _dispatch_event(self, src: Source, event: evdev.InputEvent) -> None:
        # A toggle is mid-flight: we have decided to flip but are holding the
        # grab boundary until the awaited chord keys are released (see
        # _begin_deferred_toggle). Swallow every event until then -- nothing is
        # forwarded, and while ungrabbed (OFF->ON) the key-ups still reach B's
        # compositor, so they can't strand pressed there.
        if self._pending_release:
            if event.type == e.EV_KEY:
                if event.value == 1:
                    src.held_evdev.add(event.code)
                elif event.value == 0:
                    src.held_evdev.discard(event.code)
                    self._pending_release.discard(event.code)
                    if not self._pending_release:
                        await self._complete_deferred_toggle()
            return
        if event.type == e.EV_KEY:
            # An action chord swallows its trigger key entirely: the press
            # fired the command, and the release (and any autorepeat in
            # between) must not leak to the receiver as a lone key event.
            if event.code in src.swallow_release:
                if event.value == 0:
                    src.swallow_release.discard(event.code)
                return
            # Chord must be checked against the pre-event held set --
            # before we record the trigger as held -- so the modifiers
            # alone are what's being matched.
            if self._chord_fires(src, event.code, event.value):
                # The trigger never reaches the receiver, and never makes
                # it into held_evdev either.
                await self._toggle()
                return
            cmd = self._action_command(src, event.code, event.value)
            if cmd is not None:
                # Consume the trigger: run the command, and swallow this
                # press plus the matching release. The trigger never enters
                # held_evdev, mirroring the toggle chord.
                src.swallow_release.add(event.code)
                await self._run_action(cmd)
                return
            if event.value == 1:
                src.held_evdev.add(event.code)
            elif event.value == 0:
                src.held_evdev.discard(event.code)
            u = hid_map.encode_key(event.code)
            if u is None:
                return
            # Local-only key: recorded in held_evdev above (so chords still
            # match on it) but never forwarded and never added to the resync
            # set. See _local_only_hid.
            if u in self._local_only_hid:
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

    async def _toggle(self) -> None:
        if self._toggle_pending:
            return
        self._toggle_pending = True
        try:
            if self.forwarding_on:
                # ON -> OFF. Flip the flag first so _emit_full_state takes
                # the empty-FS branch -- both for this manual emit and for
                # any concurrent _resync_loop firing during the waiting-
                # chord-release window that follows. Sampling active_keys
                # while the trigger is still held would re-press it on
                # the receiver.
                self.forwarding_on = False
                for src in self.sources:
                    await self._emit_full_state(src)
                if self.toggle_trigger_evdev is not None:
                    # Hold the grab until the trigger is released; ungrabbing
                    # with it still down makes the kernel synthesise a held-
                    # trigger press to B's compositor. We wait on the trigger
                    # alone -- a still-held modifier self-heals at ungrab (the
                    # compositor is live again to receive its release), and
                    # waiting on it could keep B's keyboard grabbed for as long
                    # as the user chooses to hold it.
                    self._begin_deferred_toggle(
                        grab_on_complete=False,
                        pending={self.toggle_trigger_evdev},
                    )
                    logger.info("forwarding OFF (waiting for trigger release to ungrab)")
                else:
                    self._ungrab_all()
                    logger.info("forwarding OFF")
            else:
                # OFF -> ON. Defer the grab until every key/button currently
                # held is released -- not just the chord. While off we are
                # ungrabbed, so B's compositor saw all of them pressed; grabbing
                # with any still down would capture its key-up and strand it
                # pressed on B (the kernel sends no synthetic release to other
                # readers on grab). active_keys() across the sources is exactly
                # that held set, and already covers the chord modifiers and any
                # held mouse button. By release time it is empty, so the
                # seed/emit at completion cannot leak anything onto A either.
                # Cost: holding the chord (or a drag) delays the grab until you
                # let go, but B stays usable locally in the meantime.
                pending: set[int] = set()
                for src in self.sources:
                    try:
                        pending |= set(src.dev.active_keys())
                    except OSError:
                        pass
                pending.add(self.toggle_trigger_evdev)
                self._begin_deferred_toggle(grab_on_complete=True, pending=pending)
                logger.info("forwarding ON (waiting for held keys to release to grab)")
        finally:
            self._toggle_pending = False

    def _begin_deferred_toggle(self, *, grab_on_complete: bool,
                               pending: set[int]) -> None:
        """Arm a deferred grab/ungrab gated on `pending` keys releasing.

        While `_pending_release` is non-empty, _dispatch_event swallows every
        source event (nothing is forwarded) and watches for the awaited
        key-ups; the last one drains the set and runs _complete_deferred_toggle.
        """
        self._pending_grab = grab_on_complete
        self._pending_release = set(pending)

    async def _complete_deferred_toggle(self) -> None:
        """Cross the grab boundary now that the awaited chord keys have released.

        Mirror images. A pending grab finishes the OFF->ON flip: the chord has
        reached B's compositor as releases and active_keys no longer reports it,
        so neither B nor A is left holding it. A pending ungrab finishes ON->OFF:
        the trigger is up, so releasing the grab won't make the kernel
        synthesise a held-trigger press to B's compositor.
        """
        if self._pending_grab:
            self._pending_grab = False
            self._grab_all()
            self.forwarding_on = True
            for src in self.sources:
                self._seed_held_from_kernel(src)
                await self._emit_full_state(src)
            logger.debug("chord released; grabbed, forwarding ON")
        else:
            self._ungrab_all()
            logger.debug("trigger released; ungrabbed")

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
            if u is None or u in self.toggle_chord_hid or u in self._local_only_hid:
                continue
            src.held.add(u)

    async def _start_dictation_server(self):
        """Listen on a local unix socket for transcribed text to inject.

        Returns the asyncio server (or None if no path configured). The
        socket is the control plane for dictation: a client (dictate.py)
        sends one JSON object per paragraph and reads one back telling it
        whether we injected the text to the receiver or it should type
        locally. Trust boundary is the user's session -- mode 0600, same as
        the FIFO the toggle hotkey uses.
        """
        if not self.dictation_socket_path:
            return None
        try:
            os.unlink(self.dictation_socket_path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            logger.warning("dictation socket %s not removable: %s",
                           self.dictation_socket_path, exc)
        server = await asyncio.start_unix_server(
            self._handle_dictation_client, path=self.dictation_socket_path)
        try:
            os.chmod(self.dictation_socket_path, 0o600)
        except OSError:
            pass
        logger.info("dictation socket listening at %s", self.dictation_socket_path)
        return server

    async def _handle_dictation_client(self, reader: asyncio.StreamReader,
                                       writer: asyncio.StreamWriter) -> None:
        """One request/response per connection.

        Request:  {"text": "...", "submit": bool}
        Response: {"routed": "remote"}  -- we injected it to the receiver
                  {"routed": "local"}   -- caller should type it locally

        The routing decision is `forwarding_on`: while forwarding, the user
        is looking at the receiver, so keystrokes belong there; otherwise
        the caller's local wtype is the right sink. A missing text field or
        malformed line is answered "local" so the caller still gets its
        paragraph out.
        """
        try:
            raw = await reader.readline()
            try:
                msg = json.loads(raw.decode("utf-8"))
                text = msg.get("text", "")
                submit = bool(msg.get("submit", False))
            except (ValueError, AttributeError):
                text, submit = "", False
            routed = "local"
            if text and self.forwarding_on:
                await self._inject_text(text, submit)
                routed = "remote"
            writer.write((json.dumps({"routed": routed}) + "\n").encode("utf-8"))
            await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    def _dictation_target(self) -> Optional[Source]:
        """The source whose uinput on the receiver gets injected text: the
        first keyboard/combo source. Injected keys ride that device's id so
        no extra virtual device is needed on either host."""
        for src in self.sources:
            if src.role in (wire.DEV_KB, wire.DEV_COMBO):
                return src
        return self.sources[0] if self.sources else None

    async def _inject_text(self, text: str, submit: bool) -> None:
        """Convert text to synthetic key events on the target device.

        Each character becomes press/release of its US-layout key, with a
        shift wrapper when needed. Injected keys are deliberately kept out
        of `src.held`: they are transient, and the FULL_STATE resync samples
        the physical device, so leaving them untracked keeps synthetic input
        from racing the user's real typing. Characters with no US-layout key
        are dropped (logged once in aggregate)."""
        src = self._dictation_target()
        if src is None:
            return
        if submit and not text.endswith("\n"):
            text = text + "\n"
        dropped = 0
        shift_hid = hid_map.encode_key(e.KEY_LEFTSHIFT)
        for ch in text:
            mapped = hid_map.encode_char(ch)
            if mapped is None:
                dropped += 1
                continue
            ev_key, needs_shift = mapped
            u = hid_map.encode_key(ev_key)
            if u is None:
                dropped += 1
                continue
            async with self._write_lock:
                if needs_shift and shift_hid is not None:
                    self._write_bytes(self._event_bytes(wire.EV_KEY, shift_hid, 1, src.device_id))
                self._write_bytes(self._event_bytes(wire.EV_KEY, u, 1, src.device_id))
                self._write_bytes(self._event_bytes(wire.EV_SYN, 0, 0, src.device_id))
                self._write_bytes(self._event_bytes(wire.EV_KEY, u, 0, src.device_id))
                if needs_shift and shift_hid is not None:
                    self._write_bytes(self._event_bytes(wire.EV_KEY, shift_hid, 0, src.device_id))
                self._write_bytes(self._event_bytes(wire.EV_SYN, 0, 0, src.device_id))
        if dropped:
            logger.debug("dictation: dropped %d char(s) with no US-layout key", dropped)

    def _event_bytes(self, ev_kind: int, code: int, value: int, device_id: int) -> bytes:
        ev = wire.Event(ev_kind, code, value, self._ts_us(), device_id)
        return wire.encode_packet(wire.PACKET_EVENT, wire.encode_event(ev))

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


def _parse_action_chord(spec: str) -> tuple[list[int], str]:
    """Parse ``CHORD:COMMAND`` into (evdev codes, command).

    The command may itself contain colons, so split on the first only. The
    chord uses the same syntax as --toggle-chord; the command is passed to a
    shell verbatim.
    """
    chord_part, sep, cmd = spec.partition(":")
    if not sep or not cmd.strip():
        raise argparse.ArgumentTypeError(
            f"action chord needs CHORD:COMMAND, got {spec!r}")
    codes = _parse_chord(chord_part)
    if not codes:
        raise argparse.ArgumentTypeError(
            f"action chord has an empty key list: {spec!r}")
    return codes, cmd


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
                             "when it presses. Modifier names are literal -- "
                             "KEY_LEFTCTRL only matches the left ctrl key. "
                             "The trigger event is consumed (never forwarded). "
                             "Pass an empty string to disable toggling entirely. "
                             "Examples: KEY_F10 (default), "
                             "KEY_LEFTCTRL+KEY_F12, "
                             "KEY_LEFTCTRL,KEY_LEFTALT,KEY_T. "
                             "Default: " + ",".join(DEFAULT_TOGGLE_CHORD)
                             + ". Run with --log-level=DEBUG to see which "
                             "modifiers were missing when a chord fails to fire.")
    parser.add_argument("--action-chord", action="append", default=[],
                        metavar="K1+...+TRIGGER:COMMAND",
                        help="run a shell command when a chord fires while "
                             "forwarding is ON (grabbed). Same chord syntax as "
                             "--toggle-chord, then a colon, then the command. "
                             "The chord fires ONLY while grabbed -- when "
                             "ungrabbed the local compositor still sees the key "
                             "and runs its own binding. The trigger is consumed "
                             "(never forwarded). Use to reach a local hotkey "
                             "(e.g. dictation) whose compositor binding the grab "
                             "would otherwise hide. Repeatable; triggers must be "
                             "distinct. Example: "
                             "KEY_LEFTMETA+KEY_D:'echo toggle > $XDG_RUNTIME_DIR/dictate.fifo'.")
    parser.add_argument("--dictation-socket", metavar="PATH",
                        help="listen on this unix socket for transcribed text "
                             "to inject. While forwarding is ON the text is "
                             "typed on the receiver via the first keyboard "
                             "source; while OFF the sender replies 'local' so "
                             "the client types it itself. Pairs with "
                             "dictate.py --emit-socket.")
    parser.add_argument("--local-only", action="append", default=[],
                        metavar="KEY",
                        help="evdev key name that stays local: it still counts "
                             "toward chord matching but is never forwarded to "
                             "the receiver, nor included in the resync snapshot. "
                             "Use to keep a bare-modifier chord's modifier (e.g. "
                             "KEY_LEFTMETA for a Super+D action chord) from "
                             "tapping the remote compositor's overview. Note: "
                             "the key becomes unusable on the receiver. "
                             "Repeatable.")
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

    action_chords: list[tuple[list[int], str]] = []
    seen_triggers: set[int] = set()
    for spec in args.action_chord:
        codes, cmd = _parse_action_chord(spec)
        trigger = codes[-1]
        if trigger in seen_triggers:
            parser.error(f"action-chord trigger reused: {spec!r}")
        seen_triggers.add(trigger)
        action_chords.append((codes, cmd))

    local_only = [_resolve_evdev_key(name) for name in args.local_only]

    # Default: SIGPIPE delivers BrokenPipeError on writes (not termination)
    # so the try/finally in run() can ungrab everything before exit.
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)

    app = SenderApp(
        sources,
        toggle_chord_evdev=toggle_chord,
        stdout=sys.stdout.buffer,
        resync_interval_s=args.resync_interval,
        start_forwarding=not args.start_off,
        action_chords=action_chords,
        dictation_socket=args.dictation_socket,
        local_only_evdev=local_only,
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
