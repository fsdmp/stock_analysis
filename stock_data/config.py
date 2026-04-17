"""Configuration for A-share stock data fetcher."""

import os
from datetime import datetime, timedelta
from pathlib import Path

# === Paths ===
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "stocks"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# === Date Range ===
# Default: past 5 years from today
END_DATE = datetime.now().strftime("%Y%m%d")
START_DATE = (datetime.now() - timedelta(days=5 * 365)).strftime("%Y%m%d")

# === Data Source ===
# akshare K-line frequency: "daily", "weekly", "monthly"
KLINE_FREQ = "daily"

# === Technical Indicator Parameters ===
MA_PERIODS = [5, 7, 10, 20]

# MACD parameters
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# KDJ parameters
KDJ_N = 9
KDJ_M1 = 3
KDJ_M2 = 3

# === Rate Limiting ===
# Seconds between requests to avoid being blocked
REQUEST_INTERVAL = 0.3

# === Column Name Mapping (Chinese -> English) ===
COLUMN_MAP = {
    "日期": "date",
    "股票代码": "symbol",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "amount",
    "振幅": "amplitude",
    "涨跌幅": "pct_change",
    "涨跌额": "change",
    "换手率": "turnover",
}
