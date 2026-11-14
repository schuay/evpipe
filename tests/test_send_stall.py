"""A link that stops draining the sender's output must end the session,
not block the sender with the devices grabbed."""
from __future__ import annotations

import fcntl
import os
import pathlib
import pty
import subprocess
import sys

import evdev.ecodes as e

from .fakes import FakeInputDevice, SenderHarness, async_test

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
FRAME_BYTES = 20  # one EVENT packet on the wire


def _frames_to_fill(wfd: int) -> int:
    return fcntl.fcntl(wfd, fcntl.F_GETPIPE_SZ) // FRAME_BYTES + 1


@async_test
async def test_full_output_pipe_ends_the_session_and_releases_the_grab(caplog):
    rfd, wfd = os.pipe()  # never read: stands in for a stalled ssh
    os.set_blocking(wfd, False)
    out = os.fdopen(wfd, "wb", buffering=0)
    try:
        mouse = FakeInputDevice(keys=[e.BTN_LEFT], rels=[e.REL_X])
        h = SenderHarness(mouse, stdout=out)
        await h.start()
        assert mouse.grabbed
        for _ in range(_frames_to_fill(wfd)):
            mouse.rel(e.REL_X, 1)
        await h.task
        assert not mouse.grabbed
        assert mouse.closed
        assert "stalled" in caplog.text
    finally:
        out.close()
        os.close(rfd)


# evpipe-send's real main(), with the device node swapped for a fake mouse
# that has more motion queued than a pipe can hold.
_SENDER = r"""
import functools, sys
import evdev.ecodes as e
from evpipe import send
from tests.fakes import FakeInputDevice

frames = int(sys.argv[1])
devs = []

def open_device(path):
    dev = FakeInputDevice(path, keys=[e.BTN_LEFT], rels=[e.REL_X])
    for _ in range(frames):
        dev.rel(e.REL_X, 1)
    devs.append(dev)
    return dev

send.SenderApp = functools.partial(send.SenderApp, open_device=open_device)
sys.argv = ["evpipe-send", "--mouse", "/dev/input/fake0", "--lock-file", ""]
rc = send.main()
print(f"exit rc={rc} grabbed={devs[0].grabbed}", file=sys.stderr)
sys.exit(rc)
"""


def test_sender_process_exits_when_nothing_drains_its_stdout():
    rfd, wfd = os.pipe()
    try:
        proc = subprocess.Popen(
            [sys.executable, "-c", _SENDER, str(_frames_to_fill(wfd))],
            stdout=wfd, stderr=subprocess.PIPE, text=True, cwd=REPO_ROOT,
        )
        try:
            _, stderr = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            _, stderr = proc.communicate()
            raise AssertionError(f"sender blocked on its stdout:\n{stderr}")
        assert proc.returncode == 0, stderr
        assert "stalled" in stderr
        assert "exit rc=0 grabbed=False" in stderr
    finally:
        os.close(rfd)
        os.close(wfd)


def test_sender_refuses_a_terminal_on_stdout():
    master, slave = pty.openpty()
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "evpipe.send", "--kb", "/dev/input/fake0",
             "--lock-file", ""],
            stdout=slave, stderr=subprocess.PIPE, text=True, timeout=10,
        )
        assert proc.returncode == 2
        assert "terminal" in proc.stderr
        assert os.get_blocking(slave)
    finally:
        os.close(master)
        os.close(slave)
