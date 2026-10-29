"""Tests for the single-sender lock: eviction of a live holder, and the
cases where the newcomer must not signal anyone."""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

import pytest

from evpipe import senderlock

# A stand-in holder: takes the lock, optionally records a pid (its own by
# default), reports readiness on stdout, then sleeps. SIGTERM handling is
# selectable so the SIGKILL fallback can be exercised.
_HOLDER = r"""
import fcntl, os, signal, sys, time
path, mode, pid_to_write, pid_delay = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
if mode == "ignore":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
else:
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
print("ready", flush=True)
time.sleep(pid_delay)
pid = os.getpid() if pid_to_write == "self" else int(pid_to_write)
os.ftruncate(fd, 0)
os.pwrite(fd, f"{pid}\n".encode(), 0)
time.sleep(60)
"""


def _spawn_holder(path, mode="term", pid_to_write="self", pid_delay=0.0):
    proc = subprocess.Popen(
        [sys.executable, "-c", _HOLDER, path, mode, str(pid_to_write), str(pid_delay)],
        stdout=subprocess.PIPE, text=True,
    )
    assert proc.stdout.readline().strip() == "ready"
    return proc


@pytest.fixture
def lock_path(tmp_path):
    return str(tmp_path / "evpipe-send.lock")


@pytest.fixture
def reap():
    procs = []
    yield procs.append
    for p in procs:
        if p.poll() is None:
            p.kill()
        p.wait()


def _holder_pid(fd) -> int:
    return int(os.pread(fd, 32, 0).strip())


def test_free_lock_is_taken_and_records_pid(lock_path):
    fd = senderlock.acquire(lock_path)
    assert _holder_pid(fd) == os.getpid()
    os.close(fd)


def test_stale_file_without_holder_is_taken(lock_path):
    with open(lock_path, "w") as f:
        f.write("999999\n")
    fd = senderlock.acquire(lock_path)
    assert _holder_pid(fd) == os.getpid()
    os.close(fd)


def test_live_holder_is_evicted_with_sigterm(lock_path, reap):
    holder = _spawn_holder(lock_path)
    reap(holder)
    time.sleep(0.1)  # let it record its pid
    t0 = time.monotonic()
    fd = senderlock.acquire(lock_path, term_timeout_s=2.0, kill_timeout_s=2.0)
    assert time.monotonic() - t0 < 1.0
    assert holder.wait(timeout=1) == 0  # exited via its SIGTERM handler
    assert _holder_pid(fd) == os.getpid()
    os.close(fd)


def test_holder_ignoring_sigterm_is_killed(lock_path, reap):
    holder = _spawn_holder(lock_path, mode="ignore")
    reap(holder)
    time.sleep(0.1)
    fd = senderlock.acquire(lock_path, term_timeout_s=0.3, kill_timeout_s=2.0)
    assert holder.wait(timeout=1) == -signal.SIGKILL
    os.close(fd)


def test_holder_that_has_not_recorded_pid_yet_is_evicted(lock_path, reap):
    holder = _spawn_holder(lock_path, pid_delay=0.2)
    reap(holder)
    fd = senderlock.acquire(lock_path)
    assert holder.wait(timeout=1) == 0
    os.close(fd)


def test_pid_that_does_not_hold_the_lock_is_never_signalled(lock_path, reap):
    bystander = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    reap(bystander)
    holder = _spawn_holder(lock_path, pid_to_write=bystander.pid)
    reap(holder)
    time.sleep(0.1)
    with pytest.raises(senderlock.LockError, match="does not hold"):
        senderlock.acquire(lock_path, term_timeout_s=0.1, kill_timeout_s=0.1)
    assert bystander.poll() is None
    assert holder.poll() is None


def test_unopenable_path_is_a_lock_error(tmp_path):
    with pytest.raises(senderlock.LockError, match="cannot open"):
        senderlock.acquire(str(tmp_path / "missing" / "evpipe-send.lock"))
