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

When you want forwarding to be toggleable rather than always-on, point
`--toggle-device` at a kb whose chord you'll press to flip the switch.
The toggle device is not grabbed; events flow through to the local
compositor in addition to being seen by us. Default chord is
`KEY_SCROLLLOCK` (override with `--toggle-key KEY_NAME`).

```
ssh user@B '
  evpipe-send --kb /dev/input/event3 --mouse /dev/input/event7 \
              --toggle-device /dev/input/event3 \
              --toggle-key KEY_SCROLLLOCK \
              --start-off
' | evpipe-recv
```

`--start-off` is useful when you want B's keyboard to start out
local-only and flip into forwarding mode only after you press the
chord. Without `--toggle-device`, forwarding is always on.

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
