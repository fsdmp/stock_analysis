"""Incremental update: append new data to existing stock files."""

import time
import sys
import logging
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime

import pandas as pd
from tqdm import tqdm

from stock_data.config import DATA_DIR, REQUEST_INTERVAL, BATCH_WORKERS
from stock_data.fetcher import (
    get_mainboard_stocks, fetch_stock_history, standardize,
    add_all_indicators, get_output_path, fetch_all,
)
from stock_data.bs_manager import BSSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _last_closed_date() -> pd.Timestamp:
    """Return the date of the last completed trading session.

    A-share closes at 15:00 Beijing time. Before that, the latest
    reliable daily bar is from the previous trading day.
    Also skips weekends.
    """
    now = pd.Timestamp.now(tz="Asia/Shanghai")
    if now.hour < 15:
        now = now - pd.Timedelta(days=1)
    while now.weekday() >= 5:
        now = now - pd.Timedelta(days=1)
    return now.normalize()


def update_stock(code: str, end_date: str | None = None) -> dict:
    """Incrementally update a single stock's data file.

    Only fetches data after the last existing date.
    Recalculates all indicators on the merged dataset.
    Called inside a worker process with its own BSSession.
    """
    path = get_output_path(code)

    if not path.exists():
        return {"code": code, "status": "not_found", "new_rows": 0}

    existing = pd.read_parquet(path)
    last_date = existing["date"].max()
    next_day = (last_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
    if end_date is None:
        end_date = _last_closed_date().strftime("%Y%m%d")

    if next_day > end_date:
        return {"code": code, "status": "up_to_date", "new_rows": 0}

    new_raw = fetch_stock_history(code, next_day, end_date)
    if new_raw is None or new_raw.empty:
        return {"code": code, "status": "up_to_date", "new_rows": 0}

    new_df = standardize(new_raw, code)

    merged = pd.concat([existing, new_df], ignore_index=True)
    merged = merged.drop_duplicates(subset=["date"], keep="last")
    merged = add_all_indicators(merged)
    merged.to_parquet(path, engine="pyarrow", index=False)

    return {"code": code, "status": "updated", "new_rows": len(new_df)}


def _update_stock_worker(code: str, end_date: str) -> dict:
    """Worker function for ProcessPoolExecutor."""
    with BSSession():
        return update_stock(code, end_date)


def update_all(codes: list[str] | None = None, progress_cb=None,
               max_workers: int = BATCH_WORKERS) -> pd.DataFrame:
    """Update all or specified stocks using multiple processes.

    Args:
        codes: Optional list of stock codes. If None, updates all existing files.
        progress_cb: Optional callback(current, total, code) called after each stock.
        max_workers: Number of parallel worker processes.
    """
    if codes is None:
        files = sorted(DATA_DIR.glob("*.parquet"))
        codes = [f.stem for f in files]

    if not codes:
        logger.info("No stock files found to update")
        return pd.DataFrame()

    end_date = _last_closed_date().strftime("%Y%m%d")
    logger.info(f"Updating {len(codes)} stocks (end_date={end_date})...")
    results = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_update_stock_worker, code, end_date): code
            for code in codes
        }
        for i, future in enumerate(
            concurrent.futures.as_completed(futures), 1
        ):
            code = futures[future]
            try:
                result = future.result()
            except Exception as e:
                result = {"code": code, "status": "error", "new_rows": 0}
                logger.warning(f"Worker error for {code}: {e}")
            results.append(result)
            if progress_cb:
                progress_cb(i, len(codes), code)

    summary = pd.DataFrame(results)
    updated = len(summary[summary["status"] == "updated"])
    uptodate = len(summary[summary["status"] == "up_to_date"])
    errors = len(summary[summary["status"].isin(["error", "not_found"])])
    logger.info(f"Update done. Updated: {updated}, Up-to-date: {uptodate}, Errors: {errors}")

    return summary


def fetch_new_listings() -> pd.DataFrame:
    """Check for any new main board stocks not yet in data directory,
    and fetch their full history."""
    all_stocks = get_mainboard_stocks()
    existing_codes = {f.stem for f in DATA_DIR.glob("*.parquet")}
    new_stocks = all_stocks[~all_stocks["code"].isin(existing_codes)]

    if new_stocks.empty:
        logger.info("No new stocks found")
        return pd.DataFrame()

    logger.info(f"Found {len(new_stocks)} new stocks to fetch")
    return fetch_all(new_stocks, skip_existing=False)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        codes = sys.argv[1:]
        update_all(codes)
    else:
        update_all()
        fetch_new_listings()
