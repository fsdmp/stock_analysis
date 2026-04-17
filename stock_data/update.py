"""Incremental update: append new data to existing stock files."""

import time
import sys
import logging

import pandas as pd
from tqdm import tqdm

from stock_data.config import DATA_DIR, REQUEST_INTERVAL
from stock_data.fetcher import (
    get_mainboard_stocks, fetch_stock_history, standardize,
    add_all_indicators, bs_login, bs_logout, get_output_path,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def update_stock(code: str) -> dict:
    """Incrementally update a single stock's data file.

    Only fetches data after the last existing date.
    Recalculates all indicators on the merged dataset.
    """
    path = get_output_path(code)

    if not path.exists():
        return {"code": code, "status": "not_found", "new_rows": 0}

    existing = pd.read_parquet(path)
    last_date = existing["date"].max()
    next_day = (last_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
    today_str = pd.Timestamp.now().strftime("%Y%m%d")

    if next_day > today_str:
        return {"code": code, "status": "up_to_date", "new_rows": 0}

    new_raw = fetch_stock_history(code, next_day, today_str)
    if new_raw is None or new_raw.empty:
        return {"code": code, "status": "up_to_date", "new_rows": 0}

    new_df = standardize(new_raw, code)

    # Merge: drop overlap, recalculate indicators
    merged = pd.concat([existing, new_df], ignore_index=True)
    merged = merged.drop_duplicates(subset=["date"], keep="last")
    merged = add_all_indicators(merged)
    merged.to_parquet(path, engine="pyarrow", index=False)

    return {"code": code, "status": "updated", "new_rows": len(new_df)}


def update_all(codes: list[str] | None = None) -> pd.DataFrame:
    """Update all or specified stocks.

    Args:
        codes: Optional list of stock codes. If None, updates all existing files.
    """
    bs_login()
    try:
        if codes is None:
            files = sorted(DATA_DIR.glob("*.parquet"))
            codes = [f.stem for f in files]

        if not codes:
            logger.info("No stock files found to update")
            return pd.DataFrame()

        logger.info(f"Updating {len(codes)} stocks...")
        results = []

        for code in tqdm(codes, desc="Updating"):
            result = update_stock(code)
            results.append(result)
            time.sleep(REQUEST_INTERVAL)

        summary = pd.DataFrame(results)
        updated = len(summary[summary["status"] == "updated"])
        uptodate = len(summary[summary["status"] == "up_to_date"])
        errors = len(summary[summary["status"].isin(["error", "not_found"])])
        logger.info(f"Update done. Updated: {updated}, Up-to-date: {uptodate}, Errors: {errors}")

        return summary
    finally:
        bs_logout()


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
    from stock_data.fetcher import fetch_all
    return fetch_all(new_stocks, skip_existing=False)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        codes = sys.argv[1:]
        update_all(codes)
    else:
        update_all()
        fetch_new_listings()
