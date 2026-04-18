"""Intraday (minute-level) data fetcher with caching and file locking.

Fetches 5-minute or 15-minute K-line data from baostock for a single stock
on a single trading day, caches to parquet with file locking.
"""

import logging
from pathlib import Path

import baostock as bs
import pandas as pd
from filelock import FileLock

from stock_data.config import INTRADAY_DIR, INTRADAY_FIELDS
from stock_data.bs_manager import bs_query_iter
from stock_data.indicators import add_intraday_indicators

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

VALID_FREQS = ("5", "15")


def _intraday_path(code: str, date: str, freq: str) -> Path:
    """Return cache path: data/intraday/{CODE}/{DATE}_{freq}min.parquet."""
    d = date.replace("-", "")
    subdir = INTRADAY_DIR / code
    subdir.mkdir(parents=True, exist_ok=True)
    return subdir / f"{d}_{freq}min.parquet"


def _is_fresh(cache_path: Path, date_str: str) -> bool:
    """Check if cached data is still fresh.

    Historical dates: always fresh if file exists.
    Today: only fresh after 15:00 Beijing time (market closed).
    """
    if not cache_path.exists():
        return False
    today = pd.Timestamp.now(tz="Asia/Shanghai").strftime("%Y%m%d")
    if date_str.replace("-", "") == today:
        now = pd.Timestamp.now(tz="Asia/Shanghai")
        return now.hour >= 15
    return True


def fetch_intraday(code: str, date: str, freq: str = "5") -> pd.DataFrame | None:
    """Fetch minute-level K-line data for one stock on one date.

    Args:
        code: 6-digit stock code
        date: YYYYMMDD or YYYY-MM-DD
        freq: "5" for 5-min, "15" for 15-min
    """
    if freq not in VALID_FREQS:
        raise ValueError(f"Invalid freq '{freq}', must be one of {VALID_FREQS}")

    if code.startswith("6"):
        bs_code = f"sh.{code}"
    else:
        bs_code = f"sz.{code}"

    d = date.replace("-", "")
    sd = f"{d[:4]}-{d[4:6]}-{d[6:8]}"

    for attempt in range(3):
        try:
            rows, rs = bs_query_iter(
                bs.query_history_k_data_plus,
                bs_code, INTRADAY_FIELDS,
                start_date=sd, end_date=sd,
                frequency=freq, adjustflag="2",
            )
            if rs.error_code != "0":
                logger.warning(f"Baostock intraday error for {code} on {sd} (attempt {attempt+1}): {rs.error_msg}")
                if attempt < 2:
                    import time; time.sleep(1)
                    continue
                return None
            if not rows:
                return None
            return pd.DataFrame(rows, columns=rs.fields)
        except (BrokenPipeError, ConnectionError, OSError) as e:
            logger.warning(f"Intraday network error for {code} on {sd} (attempt {attempt+1}): {e}")
            if attempt < 2:
                import time; time.sleep(1)
                continue
            return None
        except Exception as e:
            logger.warning(f"Intraday fetch failed for {code} on {sd}: {e}")
            return None


def _standardize_intraday(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize intraday DataFrame column types."""
    for col in ["open", "close", "high", "low", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")
    df["time"] = df["time"].apply(_format_time)
    return df


def _format_time(t: str) -> str:
    """Convert baostock time string to readable format.

    "20250415093500000" -> "09:35"
    """
    if not t:
        return t
    if len(t) >= 12:
        return f"{t[8:10]}:{t[10:12]}"
    if len(t) >= 6:
        return f"{t[:2]}:{t[2:4]}"
    return t


def get_intraday_data(code: str, date: str, freq: str = "5") -> pd.DataFrame | None:
    """Get intraday data with caching.

    1. Check local parquet cache (freshness check)
    2. If not fresh, fetch from baostock
    3. Compute indicators, save to cache
    4. Return DataFrame
    """
    if freq not in VALID_FREQS:
        raise ValueError(f"Invalid freq '{freq}'")

    d = date.replace("-", "")
    cache_path = _intraday_path(code, d, freq)
    lock_path = str(cache_path) + ".lock"

    if _is_fresh(cache_path, d):
        try:
            return pd.read_parquet(cache_path)
        except Exception:
            pass

    raw = fetch_intraday(code, d, freq)
    if raw is None or raw.empty:
        return None

    df = _standardize_intraday(raw)
    df = add_intraday_indicators(df)

    lock = FileLock(lock_path, timeout=30)
    try:
        with lock:
            if not _is_fresh(cache_path, d):
                df.to_parquet(cache_path, engine="pyarrow", index=False)
    except Exception as e:
        logger.warning(f"Failed to cache intraday data: {e}")

    return df
