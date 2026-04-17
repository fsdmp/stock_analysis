"""Utility functions for reading and querying stored stock data."""

import pandas as pd
from pathlib import Path

from stock_data.config import DATA_DIR


def list_available_stocks() -> list[str]:
    """Return list of stock codes with data files."""
    return sorted(f.stem for f in DATA_DIR.glob("*.parquet"))


def load_stock(code: str) -> pd.DataFrame:
    """Load a single stock's full data."""
    path = DATA_DIR / f"{code}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"No data for stock {code}")
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_multiple(codes: list[str]) -> dict[str, pd.DataFrame]:
    """Load multiple stocks into a dict {code: DataFrame}."""
    return {code: load_stock(code) for code in codes}


def load_all_as_panel() -> pd.DataFrame:
    """Load all stocks into a single DataFrame with a 'code' column."""
    frames = []
    for path in DATA_DIR.glob("*.parquet"):
        df = pd.read_parquet(path)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    panel = pd.concat(frames, ignore_index=True)
    panel["date"] = pd.to_datetime(panel["date"])
    return panel


def get_stock_info(code: str) -> dict:
    """Get summary info for a stock."""
    df = load_stock(code)
    return {
        "code": code,
        "date_range": (str(df["date"].min().date()), str(df["date"].max().date())),
        "total_rows": len(df),
        "latest_close": float(df["close"].iloc[-1]),
        "latest_volume": int(df["volume"].iloc[-1]),
    }


def filter_by_date(df: pd.DataFrame, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    """Filter DataFrame by date range. Dates in YYYY-MM-DD format."""
    if start:
        df = df[df["date"] >= pd.Timestamp(start)]
    if end:
        df = df[df["date"] <= pd.Timestamp(end)]
    return df
