"""Technical indicators calculation: MACD, KDJ, Moving Averages, Volume metrics."""

import numpy as np
import pandas as pd

from stock_data.config import MA_PERIODS, MACD_FAST, MACD_SLOW, MACD_SIGNAL, KDJ_N, KDJ_M1, KDJ_M2


def calc_ma(df: pd.DataFrame, periods: list[int] | None = None) -> pd.DataFrame:
    """Calculate Moving Averages for close price and volume."""
    if periods is None:
        periods = MA_PERIODS
    for p in periods:
        df[f"ma{p}"] = df["close"].rolling(window=p, min_periods=1).mean().round(3)
        df[f"v_ma{p}"] = df["volume"].rolling(window=p, min_periods=1).mean().round(0)
    return df


def calc_ema(series: pd.Series, span: int) -> pd.Series:
    """Calculate Exponential Moving Average."""
    return series.ewm(span=span, adjust=False).mean()


def calc_macd(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate MACD (Moving Average Convergence Divergence).

    Returns columns: macd_dif, macd_dea, macd_hist
    """
    ema_fast = calc_ema(df["close"], MACD_FAST)
    ema_slow = calc_ema(df["close"], MACD_SLOW)

    df["macd_dif"] = (ema_fast - ema_slow).round(4)
    df["macd_dea"] = calc_ema(df["macd_dif"], MACD_SIGNAL).round(4)
    df["macd_hist"] = (2 * (df["macd_dif"] - df["macd_dea"])).round(4)
    return df


def calc_kdj(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate KDJ indicator.

    Returns columns: kdj_k, kdj_d, kdj_j
    """
    low_n = df["low"].rolling(window=KDJ_N, min_periods=1).min()
    high_n = df["high"].rolling(window=KDJ_N, min_periods=1).max()

    rsv = ((df["close"] - low_n) / (high_n - low_n) * 100).fillna(50)

    k = np.zeros(len(df))
    d = np.zeros(len(df))
    k[0] = 50
    d[0] = 50

    for i in range(1, len(df)):
        k[i] = (KDJ_M1 - 1) / KDJ_M1 * k[i - 1] + 1 / KDJ_M1 * rsv.iloc[i]
        d[i] = (KDJ_M2 - 1) / KDJ_M2 * d[i - 1] + 1 / KDJ_M2 * k[i]

    df["kdj_k"] = np.round(k, 3)
    df["kdj_d"] = np.round(d, 3)
    df["kdj_j"] = np.round(3 * k - 2 * d, 3)
    return df


def calc_volume_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate volume-related indicators."""
    # Fill NaN volume with 0 (suspended days)
    vol = df["volume"].fillna(0).to_numpy(dtype=np.float64)

    # OBV (On Balance Volume)
    obv = np.zeros(len(df))
    for i in range(1, len(df)):
        if df["close"].iloc[i] > df["close"].iloc[i - 1]:
            obv[i] = obv[i - 1] + vol[i]
        elif df["close"].iloc[i] < df["close"].iloc[i - 1]:
            obv[i] = obv[i - 1] - vol[i]
        else:
            obv[i] = obv[i - 1]
    df["obv"] = obv.astype(np.int64)

    # Volume ratio (today's volume vs MA5 volume)
    vol_ma5 = pd.Series(vol).rolling(5, min_periods=1).mean()
    df["vol_ratio"] = (vol / vol_ma5).round(3)
    return df


def add_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate and append all technical indicators to the DataFrame."""
    df = df.sort_values("date").reset_index(drop=True)
    df = calc_ma(df)
    df = calc_macd(df)
    df = calc_kdj(df)
    df = calc_volume_indicators(df)
    return df


def add_intraday_indicators(df: pd.DataFrame, ma_periods: list[int] | None = None) -> pd.DataFrame:
    """Calculate indicators for intraday minute data.

    Adds MA lines (default MA5, MA20) and MACD.
    Does NOT calculate KDJ or OBV (not meaningful on intraday scale).
    """
    if ma_periods is None:
        ma_periods = [5, 20]
    df = df.sort_values("time").reset_index(drop=True)
    df = calc_ma(df, periods=ma_periods)
    df = calc_macd(df)
    return df
