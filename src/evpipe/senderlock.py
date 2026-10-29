"""One sender per user per host: the newest evpipe-send evicts the previous one.

Why: a sender whose ssh session died without the host noticing (peer
suspended, network gone) keeps running under sshd until TCP gives up,
~15 minutes. It still holds, or races for, EVIOCGRAB on the input
devices, so the sender of the next session shares them with a corpse.

How: an flock on a file in the runtime dir. The kernel drops the lock
when the holder's last fd to it closes, however the holder dies, so the
lock is the liveness check; the pid written into the file only says
whom to signal. A newcomer that finds the lock taken verifies the named
pid really has this file open, sends SIGTERM (the sender's shutdown
path ungrabs), and falls back to SIGKILL (the kernel ungrabs on fd
close). Two senders starting at once serialize on the lock.
"""
from __future__ import annotations

import errno
import fcntl
import logging
import os
import signal
import time
from typing import Optional

logger = logging.getLogger("evpipe-send")

LOCK_FILE_NAME = "evpipe-send.lock"
TERM_TIMEOUT_S = 2.0
KILL_TIMEOUT_S = 2.0
POLL_INTERVAL_S = 0.05
# A fresh holder is between flock() and writing its pid; how long to
# wait for the pid to appear before giving up.
PID_GRACE_S = 0.5


class LockError(Exception):
    """The lock could not be taken. The message says why."""


def default_lock_path() -> str:
    run_dir = os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}"
    return os.path.join(run_dir, LOCK_FILE_NAME)


def acquire(path: str, *, term_timeout_s: float = TERM_TIMEOUT_S,
            kill_timeout_s: float = KILL_TIMEOUT_S) -> int:
    """Take the sender lock at `path`, evicting a live holder if needed.

    Returns the lock fd. The caller must keep it open for the life of
    the process; closing it releases the lock. Raises LockError if the
    lock cannot be taken safely.
    """
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
    except OSError as exc:
        raise LockError(f"cannot open lock file {path}: {exc}") from exc
    try:
        evicted = _acquire_fd(fd, path, term_timeout_s, kill_timeout_s)
    except BaseException:
        os.close(fd)
        raise
    os.ftruncate(fd, 0)
    os.pwrite(fd, f"{os.getpid()}\n".encode("ascii"), 0)
    if evicted is not None:
        logger.warning("evicted stale sender pid %d", evicted)
    return fd


def _acquire_fd(fd: int, path: str, term_timeout_s: float,
                kill_timeout_s: float) -> Optional[int]:
    """Lock `fd`; return the pid evicted to do so, or None."""
    if _try_lock(fd):
        return None
    pid = _wait_for_pid(fd)
    if pid is None:
        if _try_lock(fd):
            return None
        raise LockError(f"{path} is locked but names no holder")
    if not _has_open(pid, fd):
        raise LockError(f"{path} names pid {pid}, which does not hold it; "
                        "not signalling it")
    logger.info("lock %s held by stale sender pid %d; evicting", path, pid)
    for sig, timeout in ((signal.SIGTERM, term_timeout_s),
                         (signal.SIGKILL, kill_timeout_s)):
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass
        except PermissionError as exc:
            raise LockError(f"cannot signal pid {pid}: {exc}") from exc
        if _poll_lock(fd, timeout):
            return pid
    raise LockError(f"pid {pid} still holds {path} after SIGKILL")


def _try_lock(fd: int) -> bool:
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError as exc:
        if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
            return False
        raise


def _poll_lock(fd: int, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while True:
        if _try_lock(fd):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(POLL_INTERVAL_S)


def _read_pid(fd: int) -> Optional[int]:
    try:
        return int(os.pread(fd, 32, 0).strip())
    except (OSError, ValueError):
        return None


def _wait_for_pid(fd: int) -> Optional[int]:
    deadline = time.monotonic() + PID_GRACE_S
    while True:
        pid = _read_pid(fd)
        if pid is not None or time.monotonic() >= deadline:
            return pid
        time.sleep(POLL_INTERVAL_S)


def _has_open(pid: int, fd: int) -> bool:
    """True if process `pid` has the file behind `fd` open.

    Stronger than a cmdline match and independent of how the holder was
    started: it proves the pid is alive and is the lock's holder (or at
    least shares its fd table), so signalling it cannot hit a stranger.
    """
    want = os.fstat(fd)
    fd_dir = f"/proc/{pid}/fd"
    try:
        names = os.listdir(fd_dir)
    except OSError:
        return False
    for name in names:
        try:
            st = os.stat(os.path.join(fd_dir, name))
        except OSError:
            continue
        if (st.st_dev, st.st_ino) == (want.st_dev, want.st_ino):
            return True
    return False
