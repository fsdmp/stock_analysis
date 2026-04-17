"""Main fetcher: download A-share main board stock data with technical indicators.

Primary data source: baostock (stable, free, no rate limit issues)
Fallback: akshare (eastmoney)
"""

import time
import sys
import logging
import signal

import baostock as bs
import pandas as pd
from tqdm import tqdm

from stock_data.config import (
    DATA_DIR, START_DATE, END_DATE, KLINE_FREQ,
    REQUEST_INTERVAL,
)
from stock_data.indicators import add_all_indicators

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Baostock login state
_bs_logged_in = False


def bs_login() -> None:
    global _bs_logged_in
    if not _bs_logged_in:
        lg = bs.login()
        if lg.error_code != "0":
            raise RuntimeError(f"Baostock login failed: {lg.error_msg}")
        _bs_logged_in = True


def bs_logout() -> None:
    global _bs_logged_in
    if _bs_logged_in:
        bs.logout()
        _bs_logged_in = False


def get_mainboard_stocks() -> pd.DataFrame:
    """Fetch all A-share main board stock codes from baostock.

    Returns DataFrame with columns: code, name, market (sh/sz)
    """
    logger.info("Fetching main board stock list...")
    bs_login()

    results = []
    # Query Shanghai main board (60xxxx)
    rs_sh = bs.query_stock_basic(code_name="")
    while rs_sh.error_code == "0" and rs_sh.next():
        row = rs_sh.get_row_data()
        code = row[0]  # e.g. sh.600000
        name = row[1]
        # Filter: only main board (60xxxx for SH, 00xxxx for SZ)
        pure_code = code.split(".")[1] if "." in code else code
        if pure_code.startswith("60") or pure_code.startswith("00"):
            market = code.split(".")[0]
            results.append({"code": pure_code, "name": name, "market": market})

    df = pd.DataFrame(results)
    df = df.drop_duplicates(subset=["code"]).reset_index(drop=True)
    logger.info(f"Found {len(df)} main board stocks")
    return df


def _timeout_handler(signum, frame):
    raise TimeoutError("baostock request timed out")


def fetch_stock_history(code: str, start_date: str, end_date: str,
                        max_retries: int = 3, timeout: int = 30) -> pd.DataFrame | None:
    """Fetch historical K-line data for a single stock via baostock.

    Args:
        code: 6-digit stock code, e.g. '600000'
        start_date: YYYYMMDD or YYYY-MM-DD
        end_date: YYYYMMDD or YYYY-MM-DD
        timeout: seconds before giving up on a single request
    """
    bs_login()

    # Convert code format: '600000' -> 'sh.600000'
    if code.startswith("6"):
        bs_code = f"sh.{code}"
    else:
        bs_code = f"sz.{code}"

    # Normalize date format to YYYY-MM-DD for baostock
    sd = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
    ed = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"

    fields = "date,open,high,low,close,volume,amount,turn,pctChg"

    for attempt in range(1, max_retries + 1):
        try:
            # Set alarm-based timeout (Unix only)
            old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(timeout)

            rs = bs.query_history_k_data_plus(
                bs_code, fields,
                start_date=sd, end_date=ed,
                frequency="d", adjustflag="2",  # 2 = forward-adjusted
            )

            rows = []
            while rs.next():
                rows.append(rs.get_row_data())

            # Cancel alarm
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

            if rs.error_code != "0":
                logger.warning(f"Baostock error for {code}: {rs.error_msg}")
                return None

            if not rows:
                return None

            df = pd.DataFrame(rows, columns=rs.fields)
            return df
        except TimeoutError:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
            # Reconnect baostock on timeout
            bs.logout()
            _bs_logged_in = False
            bs_login()
            if attempt < max_retries:
                wait = 2 * attempt
                logger.debug(f"Timeout for {code}, retry {attempt}/{max_retries} after {wait}s")
                time.sleep(wait)
            else:
                logger.warning(f"Timeout for {code} after {max_retries} retries, skipping")
        except Exception as e:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
            if attempt < max_retries:
                wait = REQUEST_INTERVAL * (2 ** attempt)
                logger.debug(f"Retry {attempt}/{max_retries} for {code} after {wait}s")
                time.sleep(wait)
            else:
                logger.warning(f"Failed to fetch {code} after {max_retries} retries: {e}")
    return None


def standardize(df: pd.DataFrame, code: str) -> pd.DataFrame:
    """Standardize column names and dtypes from baostock format."""
    df = df.rename(columns={
        "date": "date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
        "amount": "amount",
        "turn": "turnover",
        "pctChg": "pct_change",
    })

    df["date"] = pd.to_datetime(df["date"])
    df["code"] = code
    for col in ["open", "close", "high", "low", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")
    df["pct_change"] = pd.to_numeric(df["pct_change"], errors="coerce").round(3)
    df["turnover"] = pd.to_numeric(df["turnover"], errors="coerce").round(3)

    # Final column order
    cols = [
        "date", "code", "open", "close", "high", "low", "volume", "amount",
        "pct_change", "turnover",
        "ma5", "v_ma5", "ma7", "v_ma7", "ma10", "v_ma10", "ma20", "v_ma20",
        "macd_dif", "macd_dea", "macd_hist",
        "kdj_k", "kdj_d", "kdj_j",
        "obv", "vol_ratio",
    ]
    # Only keep columns that exist
    cols = [c for c in cols if c in df.columns]
    df = df[cols]
    return df


def get_output_path(code: str) -> "Path":
    """Get the Parquet file path for a stock code."""
    return DATA_DIR / f"{code}.parquet"


def save_stock(df: pd.DataFrame, code: str) -> None:
    """Save stock data to Parquet file."""
    path = get_output_path(code)
    df.to_parquet(path, engine="pyarrow", index=False)


def fetch_all(stocks_df: pd.DataFrame, start_date: str = START_DATE,
              end_date: str = END_DATE, skip_existing: bool = True) -> pd.DataFrame:
    """Fetch and save data for all stocks.

    Args:
        stocks_df: DataFrame with 'code' column
        start_date: Start date YYYYMMDD
        end_date: End date YYYYMMDD
        skip_existing: If True, skip stocks that already have data files

    Returns:
        Summary DataFrame with fetch status per stock
    """
    bs_login()
    results = []
    total = len(stocks_df)

    for _, row in tqdm(stocks_df.iterrows(), total=total, desc="Fetching stocks"):
        code = row["code"]
        name = row.get("name", "")
        path = get_output_path(code)

        # Skip if already fetched and up-to-date
        if skip_existing and path.exists():
            existing = pd.read_parquet(path)
            last_date = existing["date"].max()
            if pd.Timestamp(end_date) <= last_date:
                results.append({"code": code, "name": name, "status": "skipped", "rows": len(existing)})
                continue

        raw = fetch_stock_history(code, start_date, end_date)
        if raw is None or raw.empty:
            results.append({"code": code, "name": name, "status": "no_data", "rows": 0})
            continue

        df = standardize(raw, code)
        df = add_all_indicators(df)
        save_stock(df, code)

        results.append({"code": code, "name": name, "status": "ok", "rows": len(df)})
        time.sleep(REQUEST_INTERVAL)

    summary = pd.DataFrame(results)
    ok = len(summary[summary["status"] == "ok"])
    skipped = len(summary[summary["status"] == "skipped"])
    failed = len(summary[summary["status"] == "no_data"])
    logger.info(f"Done. OK: {ok}, Skipped: {skipped}, No data: {failed}")

    summary_path = DATA_DIR.parent / "fetch_summary.csv"
    summary.to_csv(summary_path, index=False)
    logger.info(f"Summary saved to {summary_path}")
    return summary


if __name__ == "__main__":
    stocks = get_mainboard_stocks()

    # Allow override via CLI args
    start = sys.argv[1] if len(sys.argv) > 1 else START_DATE
    end = sys.argv[2] if len(sys.argv) > 2 else END_DATE

    try:
        fetch_all(stocks, start_date=start, end_date=end)
    finally:
        bs_logout()
