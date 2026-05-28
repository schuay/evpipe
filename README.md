# evpipe

Linux-to-Linux keyboard/mouse forwarding over ssh. Type/click on host B,
have the events land on host A. ssh is just the pipe carrier; the wire
format is HID-shaped so non-Linux ends can be added later without a
protocol break.

## Install

```
uv sync
uv run evpipe-send --help
uv run evpipe-recv --help
```

Sender needs read+grab access to the source `/dev/input/event*` nodes
(usually the `input` group). Receiver needs read+write on `/dev/uinput`.

## Smoke test (single host, no ssh)

```
uv run evpipe-send --list-devices                  # pick paths
uv run evpipe-send --kb /dev/input/eventXX | uv run evpipe-recv
```

Press a few keys on the source kb. They appear on the receiver's
virtual `evpipe: <name>` uinput device (visible in `xev` / Wayland's
key logs). Ctrl-C either end -- the grabs are released cleanly.

## Across hosts (the real use case)

Run the receiver on host A (where you want the input to land), the
sender on host B (where the physical kb/mouse lives):

```
# On A:
ssh -o ServerAliveInterval=5 -o ServerAliveCountMax=2 \
    user@B 'evpipe-send --kb /dev/input/event3 --mouse /dev/input/event7' \
  | evpipe-recv
```

`ServerAliveInterval=5 ServerAliveCountMax=2` is the load-bearing
piece: without it, a hung network leaves the sender holding `EVIOCGRAB`
on B's keyboard, which means the user at B can't type until the TCP
stack notices. With it, ssh tears down within ~10s on a stall, the
sender gets `SIGPIPE` -> ungrab path, and B's keyboard returns to the
local compositor.

## Toggle hotkey

`--toggle-chord` takes a comma- or plus-separated list of evdev key
names. The last key is the trigger; the rest must be held when it
transitions to pressed. The trigger event is consumed (never reaches A),
and the chord works whether forwarding is currently ON or OFF -- so it
also serves as the bailout when B's keyboard is grabbed.

The default is `KEY_F10` -- a single dedicated key, no modifiers, no
QMK-layer/tap-hold complications. Pass an empty string to disable
toggling entirely (always-on, kill the ssh to bail out). If a chord
isn't firing, run the sender with `--log-level=DEBUG`; the log shows
which modifiers were missing when the trigger pressed.

```
ssh user@B '
  evpipe-send --kb /dev/input/event3 --mouse /dev/input/event7 \
              --toggle-chord KEY_F10 \
              --start-off
' | evpipe-recv
```

`--start-off` is useful when you want B's keyboard to start out
local-only and flip into forwarding mode only after you press the chord.

## What gets forwarded

* Keyboard keys on the HID Keyboard usage page (a-z, 0-9, modifiers,
  function keys, navigation, numpad, common consumer media keys).
* Mouse buttons 1-8 (left, right, middle, side, extra, forward, back,
  task).
* REL_X, REL_Y, REL_WHEEL, REL_HWHEEL (incl. hi-res variants).

Anything outside the static `hid_map` table is dropped at the sender
with a debug log line. EV_ABS (tablets, touchscreens) is reserved in
the wire format but not yet wired in either direction.

## Documents

* `~/k/42-evpipe/start-here.md` -- project overview, next-tasks list.
* `~/k/42-evpipe/design.md` -- full architecture, wire format, teardown.
