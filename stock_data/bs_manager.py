"""Centralized baostock session management.

Two usage patterns:
1. BSSession context manager  -- for worker processes in ProcessPoolExecutor
2. bs_query_iter()            -- for main process (web server), serialized + auto re-login
"""

import sys
import os
import threading
import logging
from contextlib import contextmanager

import baostock as bs

logger = logging.getLogger(__name__)


@contextmanager
def _suppress_stdout():
    """Suppress baostock's direct stdout prints (login/logout messages)."""
    devnull = open(os.devnull, "w")
    old_stdout = sys.stdout
    sys.stdout = devnull
    try:
        yield
    finally:
        sys.stdout = old_stdout
        devnull.close()


def _silent_login():
    """bs.login() with suppressed stdout."""
    with _suppress_stdout():
        lg = bs.login()
    return lg


def _silent_logout():
    """bs.logout() with suppressed stdout."""
    with _suppress_stdout():
        bs.logout()


class BSSession:
    """Context manager: login on enter, logout on exit.

    One instance per worker process. No sharing across threads/processes.
    """

    def __enter__(self):
        lg = _silent_login()
        if lg.error_code != "0":
            raise RuntimeError(f"Baostock login failed: {lg.error_msg}")
        return self

    def __exit__(self, *exc):
        try:
            _silent_logout()
        except Exception:
            pass


# === Shared process-level session (for web server main process) ===

_bs_lock = threading.Lock()
_bs_logged_in = False


def _ensure_login():
    """Must be called while holding _bs_lock."""
    global _bs_logged_in
    if not _bs_logged_in:
        lg = _silent_login()
        if lg.error_code != "0":
            raise RuntimeError(f"Baostock login failed: {lg.error_msg}")
        _bs_logged_in = True


def _force_relogin():
    """Force logout + login. Must be called while holding _bs_lock."""
    global _bs_logged_in
    try:
        _silent_logout()
    except Exception:
        pass
    _bs_logged_in = False
    _ensure_login()


def _is_network_error(rs) -> bool:
    """Check if baostock result indicates a network/session error requiring re-login."""
    msg = getattr(rs, "error_msg", "")
    return any(kw in msg for kw in ("Broken pipe", "网络接收错误", "未登录"))


def bs_query_iter(func, *args, **kwargs):
    """Execute a baostock query with iteration, under lock + auto re-login.

    Returns (rows, rs) where rows is list of row data and rs is the result set.
    If session is lost or network error occurs, force re-login and retry once.
    """
    with _bs_lock:
        _ensure_login()
        rs = func(*args, **kwargs)

        if rs.error_code != "0" and _is_network_error(rs):
            logger.warning(f"Baostock session error ({rs.error_msg}), re-logging in...")
            _force_relogin()
            rs = func(*args, **kwargs)

        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())
        return rows, rs


def bs_shutdown():
    """Call on process exit (atexit)."""
    global _bs_logged_in
    with _bs_lock:
        if _bs_logged_in:
            try:
                _silent_logout()
            except Exception:
                pass
            _bs_logged_in = False
