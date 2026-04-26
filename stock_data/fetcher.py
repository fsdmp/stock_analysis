"""Main fetcher: download A-share main board stock data with technical indicators.

Primary data source: baostock (stable, free, no rate limit issues)
Fallback: akshare (eastmoney)
"""

import time
import sys
import logging
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor

import baostock as bs
import pandas as pd
from tqdm import tqdm

from stock_data.config import (
    DATA_DIR, START_DATE, END_DATE, KLINE_FREQ,
    REQUEST_INTERVAL, BATCH_WORKERS,
)
from stock_data.indicators import add_all_indicators
from stock_data.bs_manager import BSSession, bs_query_iter, _silent_login, _silent_logout

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_mainboard_stocks() -> pd.DataFrame:
    """Fetch all A-share main board stock codes from baostock.

    Returns DataFrame with columns: code, name, market (sh/sz)
    """
    logger.info("Fetching main board stock list...")

    rows, rs = bs_query_iter(bs.query_stock_basic, code_name="")
    if rs.error_code != "0":
        raise RuntimeError(f"Failed to query stock list: {rs.error_msg}")

    results = []
    for row in rows:
        code = row[0]  # e.g. sh.600000
        name = row[1]
        pure_code = code.split(".")[1] if "." in code else code
        market = code.split(".")[0]
        # sh.000xxx are Shanghai indices, not stocks — only keep sz.000xxx
        if pure_code.startswith("60") and market == "sh":
            results.append({"code": pure_code, "name": name, "market": market})
        elif pure_code.startswith("00") and market == "sz":
            results.append({"code": pure_code, "name": name, "market": market})

    df = pd.DataFrame(results)
    df = df.drop_duplicates(subset=["code"]).reset_index(drop=True)
    logger.info(f"Found {len(df)} main board stocks")
    return df


def fetch_stock_history(code: str, start_date: str, end_date: str,
                        max_retries: int = 3) -> pd.DataFrame | None:
    """Fetch historical K-line data for a single stock via baostock.

    Called inside a worker process that already has its own BSSession.
    """
    if code.startswith("6"):
        bs_code = f"sh.{code}"
    else:
        bs_code = f"sz.{code}"

    sd = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
    ed = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
    fields = "date,open,high,low,close,volume,amount,turn,pctChg"

    for attempt in range(1, max_retries + 1):
        try:
            rs = bs.query_history_k_data_plus(
                bs_code, fields,
                start_date=sd, end_date=ed,
                frequency="d", adjustflag="2",
            )

            if rs.error_code != "0":
                if "未登录" in rs.error_msg:
                    _silent_logout()
                    _silent_login()
                    continue
                logger.warning(f"Baostock error for {code}: {rs.error_msg}")
                return None

            rows = []
            while rs.next():
                rows.append(rs.get_row_data())

            if not rows:
                return None

            return pd.DataFrame(rows, columns=rs.fields)
        except Exception as e:
            if attempt < max_retries:
                time.sleep(REQUEST_INTERVAL * attempt)
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

    cols = [
        "date", "code", "open", "close", "high", "low", "volume", "amount",
        "pct_change", "turnover",
        "ma5", "v_ma5", "ma7", "v_ma7", "ma10", "v_ma10", "ma20", "v_ma20",
        "macd_dif", "macd_dea", "macd_hist",
        "kdj_k", "kdj_d", "kdj_j",
        "obv", "vol_ratio",
    ]
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


def _fetch_and_save_worker(code: str, name: str, start_date: str, end_date: str,
                           skip_existing: bool) -> dict:
    """Worker function for ProcessPoolExecutor. Each call runs in its own process."""
    with BSSession():
        path = get_output_path(code)

        if skip_existing and path.exists():
            existing = pd.read_parquet(path)
            last_date = existing["date"].max()
            if pd.Timestamp(end_date) <= last_date:
                return {"code": code, "name": name, "status": "skipped", "rows": len(existing)}

        raw = fetch_stock_history(code, start_date, end_date)
        if raw is None or raw.empty:
            return {"code": code, "name": name, "status": "no_data", "rows": 0}

        df = standardize(raw, code)
        df = add_all_indicators(df)
        save_stock(df, code)
        time.sleep(REQUEST_INTERVAL)

        return {"code": code, "name": name, "status": "ok", "rows": len(df)}


def fetch_all(stocks_df: pd.DataFrame, start_date: str = START_DATE,
              end_date: str = END_DATE, skip_existing: bool = True,
              max_workers: int = BATCH_WORKERS) -> pd.DataFrame:
    """Fetch and save data for all stocks using multiple processes.

    Args:
        stocks_df: DataFrame with 'code' column
        start_date: Start date YYYYMMDD
        end_date: End date YYYYMMDD
        skip_existing: If True, skip stocks that already have data files
        max_workers: Number of parallel worker processes

    Returns:
        Summary DataFrame with fetch status per stock
    """
    results = []
    total = len(stocks_df)
    code_name_map = dict(zip(stocks_df["code"], stocks_df.get("name", [""] * total)))

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _fetch_and_save_worker, code, code_name_map.get(code, ""),
                start_date, end_date, skip_existing
            ): code
            for code in stocks_df["code"]
        }
        for future in tqdm(
            concurrent.futures.as_completed(futures),
            total=total, desc="Fetching stocks"
        ):
            code = futures[future]
            try:
                result = future.result()
            except Exception as e:
                result = {"code": code, "name": code_name_map.get(code, ""),
                          "status": "error", "rows": 0}
                logger.warning(f"Worker error for {code}: {e}")
            results.append(result)

    summary = pd.DataFrame(results)
    ok = len(summary[summary["status"] == "ok"])
    skipped = len(summary[summary["status"] == "skipped"])
    failed = len(summary[summary["status"].isin(["no_data", "error"])])
    logger.info(f"Done. OK: {ok}, Skipped: {skipped}, No data: {failed}")

    summary_path = DATA_DIR.parent / "fetch_summary.csv"
    summary.to_csv(summary_path, index=False)
    logger.info(f"Summary saved to {summary_path}")
    return summary


if __name__ == "__main__":
    stocks = get_mainboard_stocks()

    start = sys.argv[1] if len(sys.argv) > 1 else START_DATE
    end = sys.argv[2] if len(sys.argv) > 2 else END_DATE

    fetch_all(stocks, start_date=start, end_date=end)
