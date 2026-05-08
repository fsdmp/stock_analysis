"""
Comprehensive multi-dimensional technical analysis for trade pattern evaluation.

Analyzes stocks bought at specific dates to find distinguishing features
between rising and falling stocks, with focus on:
1. Pre-buy technical state
2. Volume-price patterns
3. Momentum indicators
4. Trend context
5. Market microstructure signals
"""

import os
import sys
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# ─── Configuration ───────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "stocks")
NAMES_FILE = os.path.join(os.path.dirname(__file__), "data", "stock_names.json")

# Stock list with trade info
TRADES = {
    "2026-04-17": {
        "600773": {"name": "西藏城投", "result": "大涨", "ideal_sell": ["2026-04-29", "2026-04-30"]},
        "603682": {"name": "锦和商管", "result": "涨", "ideal_sell": ["2026-04-30", "2026-05-06"]},
        "002733": {"name": "雄韬股份", "result": "跌", "ideal_sell": None},
        "002074": {"name": "国轩高科", "result": "大跌", "ideal_sell": None},
        "603738": {"name": "泰晶科技", "result": "涨", "ideal_sell": None},  # hold
        "600654": {"name": "中安科", "result": "大跌", "ideal_sell": None},
        "600527": {"name": "江南高纤", "result": "涨", "ideal_sell": ["2026-05-07"]},
    },
    "2026-04-20": {
        "600855": {"name": "航天长峰", "result": "大跌", "ideal_sell": None},
        "603109": {"name": "神驰机电", "result": "大跌", "ideal_sell": None},
        "002943": {"name": "宇晶股份", "result": "涨", "ideal_sell": None},  # hold
        "002515": {"name": "金字火腿", "result": "涨", "ideal_sell": None},  # hold
        "002436": {"name": "兴森科技", "result": "涨", "ideal_sell": None},  # hold
        "000612": {"name": "焦作万方", "result": "跌", "ideal_sell": None},
        "000791": {"name": "甘肃能源", "result": "跌", "ideal_sell": None},
    },
    "2026-04-21": {
        "603815": {"name": "交建股份", "result": "涨", "ideal_sell": None},  # hold
        "605365": {"name": "立达信", "result": "涨", "ideal_sell": ["2026-04-23", "2026-04-24"]},
        "605366": {"name": "宏柏新材", "result": "跌", "ideal_sell": None},
        "605098": {"name": "行动教育", "result": "大跌", "ideal_sell": None},
        "603150": {"name": "万朗磁塑", "result": "跌", "ideal_sell": None},
        "600135": {"name": "乐凯胶片", "result": "跌", "ideal_sell": None},
        "002463": {"name": "沪电股份", "result": "小涨", "ideal_sell": ["2026-04-23"]},
    },
}

# Classification
WINNERS = []  # (code, name, buy_date, sell_dates)
LOSERS = []   # (code, name, buy_date)

for buy_date, stocks in TRADES.items():
    for code, info in stocks.items():
        result = info["result"]
        if result in ("大涨", "涨", "小涨"):
            WINNERS.append((code, info["name"], buy_date, info["ideal_sell"]))
        else:
            LOSERS.append((code, info["name"], buy_date))


def load_stock(code: str) -> pd.DataFrame | None:
    """Load stock data from parquet file."""
    # Try different code patterns
    for pattern in [code, f"SH{code}", f"SZ{code}"]:
        path = os.path.join(DATA_DIR, f"{pattern}.parquet")
        if os.path.exists(path):
            df = pd.read_parquet(path)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            return df
    return None


def get_idx_on_date(df: pd.DataFrame, date_str: str):
    """Find the row index for a given date (or the next trading day)."""
    target = pd.Timestamp(date_str)
    mask = df["date"] >= target
    if mask.any():
        return mask.idxmax()
    return None


def compute_rsi(series, period=14):
    """Compute RSI from a price series."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def compute_atr(df, period=14):
    """Compute Average True Range."""
    high = df["high"]
    low = df["low"]
    close = df["close"]
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period, min_periods=period).mean()
    return atr


def compute_williams_r(df, period=14):
    """Williams %R"""
    high_n = df["high"].rolling(window=period).max()
    low_n = df["low"].rolling(window=period).min()
    wr = (high_n - df["close"]) / (high_n - low_n) * -100
    return wr


def compute_adx(df, period=14):
    """Average Directional Index"""
    high = df["high"]
    low = df["low"]
    close = df["close"]

    plus_dm = high.diff()
    minus_dm = -low.diff()

    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)

    atr = compute_atr(df, period)

    plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr)

    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.rolling(period).mean()

    return adx, plus_di, minus_di


def compute_obv_signal(df, short=5, long=20):
    """OBV moving average cross signal."""
    obv = df["obv"]
    obv_ma_short = obv.rolling(short).mean()
    obv_ma_long = obv.rolling(long).mean()
    return obv_ma_short, obv_ma_long


def compute_volume_price_divergence(df, window=10):
    """Detect volume-price divergence over a window."""
    results = []
    for i in range(len(df)):
        if i < window:
            results.append(0)
            continue
        sub = df.iloc[i - window + 1:i + 1]
        price_trend = sub["close"].iloc[-1] - sub["close"].iloc[0]
        vol_trend = sub["volume"].sum()  # total volume
        avg_vol = df["volume"].iloc[max(0, i - 2 * window):i].mean()

        if price_trend > 0 and vol_trend < avg_vol * window * 0.8:
            results.append(-1)  # bearish divergence: price up but volume shrinking
        elif price_trend < 0 and vol_trend > avg_vol * window * 1.2:
            results.append(1)  # bullish divergence: price down but volume expanding
        elif price_trend > 0 and vol_trend > avg_vol * window * 1.2:
            results.append(2)  # strong confirmation
        else:
            results.append(0)
    return results


def compute_chip_concentration(df, window=20):
    """Estimate chip concentration area (volume-weighted price zone)."""
    results = []
    for i in range(len(df)):
        if i < window:
            results.append(None)
            continue
        sub = df.iloc[i - window + 1:i + 1]
        total_vol = sub["volume"].sum()
        if total_vol == 0:
            results.append(None)
            continue
        vwap = (sub["close"] * sub["volume"]).sum() / total_vol
        # Standard deviation weighted by volume
        variance = (sub["volume"] * (sub["close"] - vwap) ** 2).sum() / total_vol
        std = np.sqrt(variance) if variance > 0 else 0
        results.append({"vwap": vwap, "std": std, "price_vs_vwap": (sub["close"].iloc[-1] - vwap) / vwap * 100})
    return results


def analyze_stock_at_date(df: pd.DataFrame, buy_date: str, code: str, name: str, sell_dates=None) -> dict:
    """
    Comprehensive technical analysis at the buy date point.
    Returns dict of indicators.
    """
    buy_idx = get_idx_on_date(df, buy_date)
    if buy_idx is None or buy_idx < 25:
        return None

    # We analyze up to the day BEFORE buy date (i.e., the last closed day)
    # So we look at idx = buy_idx - 1 (the day before buy, which is the decision day)
    # Actually, the buy is at open of buy_date, so we know everything up to buy_date-1 close
    analysis_idx = buy_idx  # This is the buy date row, but we use data up to buy_idx-1

    # For pre-buy analysis, use the row before buy date
    pre_idx = buy_idx - 1  # Last closed day before buying

    if pre_idx < 20:
        return None

    row = df.iloc[pre_idx]
    result = {}

    # ─── 1. Price Action ─────────────────────────────────────────────────
    close = row["close"]
    open_ = row["open"]
    high = row["high"]
    low = row["low"]

    # Candlestick type
    body = close - open_
    upper_shadow = high - max(close, open_)
    lower_shadow = min(close, open_) - low
    total_range = high - low

    result["candle_body_ratio"] = abs(body) / total_range if total_range > 0 else 0
    result["upper_shadow_ratio"] = upper_shadow / total_range if total_range > 0 else 0
    result["lower_shadow_ratio"] = lower_shadow / total_range if total_range > 0 else 0
    result["is_yang"] = 1 if close > open_ else 0

    # Recent N-day price change
    for n_days in [3, 5, 10, 20]:
        if pre_idx >= n_days:
            prev_close = df.iloc[pre_idx - n_days]["close"]
            result[f"pct_{n_days}d"] = (close / prev_close - 1) * 100
        else:
            result[f"pct_{n_days}d"] = None

    # ─── 2. MA Analysis ──────────────────────────────────────────────────
    for ma in ["ma5", "ma7", "ma10", "ma20"]:
        val = row.get(ma)
        if val and not np.isnan(val) and val > 0:
            result[f"close_vs_{ma}"] = (close - val) / val * 100
        else:
            result[f"close_vs_{ma}"] = None

    # MA alignment (bullish: ma5 > ma7 > ma10 > ma20)
    mas = []
    for ma in ["ma5", "ma7", "ma10", "ma20"]:
        val = row.get(ma)
        if val and not np.isnan(val):
            mas.append(val)
    if len(mas) == 4:
        result["ma_alignment"] = 1 if (mas[0] > mas[1] > mas[2] > mas[3]) else (-1 if mas[0] < mas[1] < mas[2] < mas[3] else 0)
    else:
        result["ma_alignment"] = None

    # MA5 slope
    if pre_idx >= 3:
        ma5_now = row.get("ma5")
        ma5_3d = df.iloc[pre_idx - 3].get("ma5")
        if ma5_now and ma5_3d and not np.isnan(ma5_now) and not np.isnan(ma5_3d) and ma5_3d > 0:
            result["ma5_slope_3d"] = (ma5_now - ma5_3d) / ma5_3d * 100
        else:
            result["ma5_slope_3d"] = None

    # MA20 slope
    if pre_idx >= 5:
        ma20_now = row.get("ma20")
        ma20_5d = df.iloc[pre_idx - 5].get("ma20")
        if ma20_now and ma20_5d and not np.isnan(ma20_now) and not np.isnan(ma20_5d) and ma20_5d > 0:
            result["ma20_slope_5d"] = (ma20_now - ma20_5d) / ma20_5d * 100
        else:
            result["ma20_slope_5d"] = None

    # VWMA vs MA comparison (anti-trap)
    for period in [5, 10, 20]:
        ma_col = f"ma{period}"
        vwma_col = f"vwma{period}"
        ma_val = row.get(ma_col)
        vwma_val = row.get(vwma_col)
        if (ma_val is not None and vwma_val is not None
            and not np.isnan(ma_val) and not np.isnan(vwma_val) and ma_val > 0):
            result[f"vwma{period}_vs_ma{period}"] = (vwma_val - ma_val) / ma_val * 100
        else:
            result[f"vwma{period}_vs_ma{period}"] = None

    # ─── 3. Volume Analysis ──────────────────────────────────────────────
    vol = row["volume"]
    avg_vol_5 = df.iloc[pre_idx - 4:pre_idx + 1]["volume"].mean()
    avg_vol_10 = df.iloc[max(0, pre_idx - 9):pre_idx + 1]["volume"].mean()
    avg_vol_20 = df.iloc[max(0, pre_idx - 19):pre_idx + 1]["volume"].mean()

    result["vol_ratio_5"] = vol / avg_vol_5 if avg_vol_5 > 0 else None
    result["vol_ratio_10"] = vol / avg_vol_10 if avg_vol_10 > 0 else None
    result["vol_ratio_20"] = vol / avg_vol_20 if avg_vol_20 > 0 else None

    # Volume trend (5d avg vs 10d avg)
    result["vol_trend"] = avg_vol_5 / avg_vol_10 if avg_vol_10 > 0 else None

    # Volume-price consistency (recent 5 days)
    recent_5 = df.iloc[max(0, pre_idx - 4):pre_idx + 1]
    up_days_vol = recent_5[recent_5["pct_change"] > 0]["volume"].mean()
    down_days_vol = recent_5[recent_5["pct_change"] < 0]["volume"].mean()
    if down_days_vol > 0 and not np.isnan(up_days_vol) and not np.isnan(down_days_vol):
        result["vol_up_down_ratio"] = up_days_vol / down_days_vol
    else:
        result["vol_up_down_ratio"] = None

    # ─── 4. MACD Analysis ────────────────────────────────────────────────
    dif = row.get("macd_dif")
    dea = row.get("macd_dea")
    hist = row.get("macd_hist")

    if dif is not None and not np.isnan(dif):
        result["macd_dif"] = dif
        result["macd_dea"] = dea if dea and not np.isnan(dea) else None
        result["macd_hist"] = hist if hist and not np.isnan(hist) else None
        result["macd_cross"] = 1 if dif > dea else -1

        # MACD histogram trend (increasing or decreasing)
        hist_3 = []
        for i in range(3):
            h = df.iloc[pre_idx - i].get("macd_hist")
            if h is not None and not np.isnan(h):
                hist_3.append(h)
        if len(hist_3) >= 2:
            result["macd_hist_trend"] = 1 if hist_3[0] > hist_3[1] else -1
        else:
            result["macd_hist_trend"] = None

        # Is MACD in golden cross zone?
        result["macd_above_zero"] = 1 if dif > 0 else -1
    else:
        result["macd_dif"] = None
        result["macd_dea"] = None
        result["macd_hist"] = None
        result["macd_cross"] = None
        result["macd_hist_trend"] = None
        result["macd_above_zero"] = None

    # ─── 5. KDJ Analysis ─────────────────────────────────────────────────
    k = row.get("kdj_k")
    d = row.get("kdj_d")
    j = row.get("kdj_j")

    if k is not None and not np.isnan(k):
        result["kdj_k"] = k
        result["kdj_d"] = d if d and not np.isnan(d) else None
        result["kdj_j"] = j if j and not np.isnan(j) else None
        result["kdj_cross"] = 1 if k > d else -1
        result["kdj_overbought"] = 1 if k > 80 else 0
        result["kdj_oversold"] = 1 if k < 20 else 0
    else:
        for key in ["kdj_k", "kdj_d", "kdj_j", "kdj_cross", "kdj_overbought", "kdj_oversold"]:
            result[key] = None

    # ─── 6. Bollinger Bands ──────────────────────────────────────────────
    bb_upper = row.get("bb_upper")
    bb_lower = row.get("bb_lower")
    bb_mid = row.get("bb_middle")
    bb_bw = row.get("bb_bandwidth")

    if bb_upper is not None and not np.isnan(bb_upper) and bb_mid and bb_mid > 0:
        result["bb_position"] = (close - bb_lower) / (bb_upper - bb_lower) * 100 if bb_upper > bb_lower else 50
        result["bb_bandwidth"] = bb_bw
        result["bb_squeeze"] = 1 if bb_bw and bb_bw < 5 else 0  # narrow band = squeeze
    else:
        result["bb_position"] = None
        result["bb_bandwidth"] = None
        result["bb_squeeze"] = None

    # ─── 7. RSI ──────────────────────────────────────────────────────────
    rsi_14 = compute_rsi(df["close"], 14)
    rsi_6 = compute_rsi(df["close"], 6)
    result["rsi_14"] = rsi_14.iloc[pre_idx] if pre_idx < len(rsi_14) else None
    result["rsi_6"] = rsi_6.iloc[pre_idx] if pre_idx < len(rsi_6) else None

    # ─── 8. ATR / Volatility ─────────────────────────────────────────────
    atr = compute_atr(df, 14)
    atr_val = atr.iloc[pre_idx] if pre_idx < len(atr) else None
    result["atr_14"] = atr_val
    result["atr_pct"] = atr_val / close * 100 if atr_val and close > 0 else None

    # Historical volatility (20-day)
    if pre_idx >= 20:
        returns = df.iloc[pre_idx - 19:pre_idx + 1]["pct_change"].dropna()
        result["hist_vol_20"] = returns.std() * np.sqrt(252) if len(returns) > 5 else None
    else:
        result["hist_vol_20"] = None

    # ─── 9. ADX (Trend Strength) ─────────────────────────────────────────
    adx_series, plus_di, minus_di = compute_adx(df, 14)
    result["adx"] = adx_series.iloc[pre_idx] if pre_idx < len(adx_series) else None
    result["plus_di"] = plus_di.iloc[pre_idx] if pre_idx < len(plus_di) else None
    result["minus_di"] = minus_di.iloc[pre_idx] if pre_idx < len(minus_di) else None

    # ─── 10. Williams %R ─────────────────────────────────────────────────
    wr = compute_williams_r(df, 14)
    result["williams_r"] = wr.iloc[pre_idx] if pre_idx < len(wr) else None

    # ─── 11. OBV Signal ──────────────────────────────────────────────────
    obv_ma5, obv_ma20 = compute_obv_signal(df, 5, 20)
    result["obv_above_ma5"] = 1 if pre_idx < len(obv_ma5) and obv_ma5.iloc[pre_idx] > obv_ma20.iloc[pre_idx] else -1

    # OBV slope (5-day change in OBV MA5)
    if pre_idx >= 5:
        obv_slope = obv_ma5.iloc[pre_idx] - obv_ma5.iloc[pre_idx - 5]
        result["obv_slope"] = obv_slope if not np.isnan(obv_slope) else None
    else:
        result["obv_slope"] = None

    # ─── 12. Turnover Rate ───────────────────────────────────────────────
    turnover = row.get("turnover")
    result["turnover"] = turnover
    if pre_idx >= 5:
        avg_turnover_5 = df.iloc[pre_idx - 4:pre_idx + 1]["turnover"].mean()
        result["turnover_vs_avg"] = turnover / avg_turnover_5 if avg_turnover_5 > 0 and turnover else None
    else:
        result["turnover_vs_avg"] = None

    # ─── 13. Chip Concentration ──────────────────────────────────────────
    chips = compute_chip_concentration(df, 20)
    chip_data = chips[pre_idx] if pre_idx < len(chips) else None
    if chip_data:
        result["price_vs_vwap_20d"] = chip_data["price_vs_vwap"]
    else:
        result["price_vs_vwap_20d"] = None

    # ─── 14. Support / Resistance ────────────────────────────────────────
    # Distance from recent high/low
    if pre_idx >= 20:
        recent_high = df.iloc[pre_idx - 19:pre_idx + 1]["high"].max()
        recent_low = df.iloc[pre_idx - 19:pre_idx + 1]["low"].min()
        result["dist_from_20d_high"] = (close - recent_high) / recent_high * 100
        result["dist_from_20d_low"] = (close - recent_low) / recent_low * 100
        result["price_position_20d"] = (close - recent_low) / (recent_high - recent_low) * 100 if recent_high > recent_low else 50
    else:
        result["dist_from_20d_high"] = None
        result["dist_from_20d_low"] = None
        result["price_position_20d"] = None

    # ─── 15. Consecutive Days Pattern ────────────────────────────────────
    consec_up = 0
    consec_down = 0
    for i in range(pre_idx, max(pre_idx - 10, -1), -1):
        pct = df.iloc[i].get("pct_change", 0)
        if pct and pct > 0:
            if consec_down == 0:
                consec_up += 1
            else:
                break
        elif pct and pct < 0:
            if consec_up == 0:
                consec_down += 1
            else:
                break
        else:
            break
    result["consec_up"] = consec_up
    result["consec_down"] = consec_down

    # ─── 16. Gap Analysis ────────────────────────────────────────────────
    if pre_idx >= 1:
        prev_close = df.iloc[pre_idx - 1]["close"]
        gap = (row["open"] - prev_close) / prev_close * 100
        result["gap_pct"] = gap
    else:
        result["gap_pct"] = None

    # ─── 17. Recent Limit-up History ─────────────────────────────────────
    limit_up_count = 0
    for i in range(pre_idx, max(pre_idx - 5, -1), -1):
        pct = df.iloc[i].get("pct_change", 0)
        if pct and pct >= 9.5:
            limit_up_count += 1
        else:
            break
    result["recent_limit_up"] = limit_up_count

    # ─── 18. Volume Price Divergence ─────────────────────────────────────
    vpd = compute_volume_price_divergence(df, 10)
    result["vol_price_divergence"] = vpd[pre_idx] if pre_idx < len(vpd) else None

    # ─── 19. Buy Date Context ────────────────────────────────────────────
    # Day of week (Monday=0)
    buy_ts = pd.Timestamp(buy_date)
    result["day_of_week"] = buy_ts.dayofweek
    result["is_monday"] = 1 if buy_ts.dayofweek == 0 else 0
    result["is_friday"] = 1 if buy_ts.dayofweek == 4 else 0

    # ─── 20. Actual Performance (for verification) ───────────────────────
    buy_open = df.iloc[buy_idx]["open"]
    result["buy_open"] = buy_open

    # Next 1, 2, 3, 5, 10 day returns
    for nd in [1, 2, 3, 5, 10]:
        if buy_idx + nd < len(df):
            future_close = df.iloc[buy_idx + nd]["close"]
            result[f"return_{nd}d"] = (future_close / buy_open - 1) * 100
        else:
            result[f"return_{nd}d"] = None

    # Max return and max drawdown in next N days
    for nd in [3, 5, 10]:
        if buy_idx + nd < len(df):
            future = df.iloc[buy_idx:buy_idx + nd + 1]
            max_price = future["high"].max()
            min_price = future["low"].min()
            result[f"max_return_{nd}d"] = (max_price / buy_open - 1) * 100
            result[f"max_drawdown_{nd}d"] = (min_price / buy_open - 1) * 100
        else:
            result[f"max_return_{nd}d"] = None
            result[f"max_drawdown_{nd}d"] = None

    # If sell dates provided, calculate ideal sell return
    if sell_dates:
        for sd in sell_dates:
            sell_idx = get_idx_on_date(df, sd)
            if sell_idx is not None:
                # Sell at open of sell date (or close of previous day)
                sell_open = df.iloc[sell_idx]["open"]
                result["ideal_sell_return"] = (sell_open / buy_open - 1) * 100
                result["ideal_sell_date"] = sd
                break

    # Max return before ideal sell date
    if sell_dates:
        for sd in sell_dates:
            sell_idx = get_idx_on_date(df, sd)
            if sell_idx is not None and sell_idx > buy_idx:
                period_df = df.iloc[buy_idx:sell_idx + 1]
                max_high = period_df["high"].max()
                result["max_return_to_sell"] = (max_high / buy_open - 1) * 100
                result["min_low_to_sell"] = (period_df["low"].min() / buy_open - 1) * 100
                break

    # Hold return to latest data
    last_idx = len(df) - 1
    if last_idx > buy_idx:
        last_close = df.iloc[last_idx]["close"]
        result["hold_to_latest"] = (last_close / buy_open - 1) * 100
        max_hold = df.iloc[buy_idx:last_idx + 1]["high"].max()
        min_hold = df.iloc[buy_idx:last_idx + 1]["low"].min()
        result["max_return_hold"] = (max_hold / buy_open - 1) * 100
        result["max_drawdown_hold"] = (min_hold / buy_open - 1) * 100

    result["code"] = code
    result["name"] = name
    result["buy_date"] = buy_date
    result["result"] = TRADES[buy_date][code]["result"]

    return result


def compare_groups(winners_data, losers_data):
    """Compare indicators between winners and losers."""
    all_keys = set()
    for d in winners_data + losers_data:
        all_keys.update(k for k in d.keys() if k not in ["code", "name", "buy_date", "result", "ideal_sell_date"])

    # Numeric keys only
    numeric_keys = []
    for k in sorted(all_keys):
        vals = [d.get(k) for d in winners_data + losers_data if d.get(k) is not None]
        if vals and isinstance(vals[0], (int, float, np.integer, np.floating)):
            numeric_keys.append(k)

    print("\n" + "=" * 120)
    print("多维度技术指标对比: 涨票 vs 跌票")
    print("=" * 120)

    comparison = []
    for k in numeric_keys:
        w_vals = [d[k] for d in winners_data if d.get(k) is not None and not np.isnan(d[k]) if isinstance(d[k], (int, float, np.integer, np.floating))]
        l_vals = [d[k] for d in losers_data if d.get(k) is not None and not np.isnan(d[k]) if isinstance(d[k], (int, float, np.integer, np.floating))]

        if len(w_vals) < 2 or len(l_vals) < 2:
            continue

        w_mean = np.mean(w_vals)
        l_mean = np.mean(l_vals)
        w_std = np.std(w_vals)
        l_std = np.std(l_vals)

        # Effect size (Cohen's d)
        pooled_std = np.sqrt((w_std ** 2 + l_std ** 2) / 2)
        cohens_d = (w_mean - l_mean) / pooled_std if pooled_std > 0 else 0

        # Discrimination score
        discrimination = abs(w_mean - l_mean) / ((w_std + l_std) / 2) if (w_std + l_std) > 0 else 0

        comparison.append({
            "key": k,
            "w_mean": w_mean,
            "w_std": w_std,
            "l_mean": l_mean,
            "l_std": l_std,
            "diff": w_mean - l_mean,
            "cohens_d": cohens_d,
            "discrimination": discrimination,
            "w_vals": w_vals,
            "l_vals": l_vals,
        })

    # Sort by discrimination score
    comparison.sort(key=lambda x: abs(x["discrimination"]), reverse=True)

    # Print top discriminators
    print(f"\n{'指标':<30} {'涨票均值':>10} {'涨票标准差':>10} {'跌票均值':>10} {'跌票标准差':>10} {'差异':>10} {'区分度':>10} {'Cohen-d':>10}")
    print("-" * 120)

    for c in comparison[:40]:
        print(f"{c['key']:<30} {c['w_mean']:>10.3f} {c['w_std']:>10.3f} {c['l_mean']:>10.3f} {c['l_std']:>10.3f} {c['diff']:>10.3f} {c['discrimination']:>10.3f} {c['cohens_d']:>10.3f}")

    return comparison


def print_individual_analysis(all_results):
    """Print individual stock analysis."""
    print("\n" + "=" * 120)
    print("个股技术面详细分析")
    print("=" * 120)

    for r in all_results:
        result_type = r["result"]
        marker = "▲" if result_type in ("大涨", "涨", "小涨") else "▼"
        print(f"\n{marker} {r['code']} {r['name']} | 买入日: {r['buy_date']} | 结果: {result_type}")
        print(f"  买入价: {r.get('buy_open', 'N/A'):.2f}")

        # Returns
        for nd in [1, 2, 3, 5, 10]:
            ret = r.get(f"return_{nd}d")
            if ret is not None:
                sign = "+" if ret > 0 else ""
                print(f"  {nd}日收益: {sign}{ret:.2f}%", end="")
        print()

        hold_ret = r.get("hold_to_latest")
        if hold_ret is not None:
            sign = "+" if hold_ret > 0 else ""
            print(f"  持有至今: {sign}{hold_ret:.2f}% | 最大涨幅: {r.get('max_return_hold', 0):.2f}% | 最大回撤: {r.get('max_drawdown_hold', 0):.2f}%")

        if r.get("ideal_sell_return") is not None:
            print(f"  理想卖出收益: {r['ideal_sell_return']:.2f}% (日期: {r.get('ideal_sell_date', 'N/A')})")

        # Key indicators
        print(f"  ── 买入前一日状态 ──")

        # Momentum
        for nd in [3, 5, 10, 20]:
            val = r.get(f"pct_{nd}d")
            if val is not None:
                print(f"  {nd}日动量: {val:.2f}%", end="")
        print()

        # MA
        for ma in ["ma5", "ma7", "ma10", "ma20"]:
            val = r.get(f"close_vs_{ma}")
            if val is not None:
                print(f"  vs {ma.upper()}: {val:.2f}%", end="")
        print()

        ma_align = r.get("ma_alignment")
        align_str = "多头排列" if ma_align == 1 else ("空头排列" if ma_align == -1 else "交叉")
        print(f"  均线排列: {align_str}")

        # Volume
        for key in ["vol_ratio_5", "vol_ratio_10", "vol_ratio_20", "vol_trend", "vol_up_down_ratio", "turnover", "turnover_vs_avg"]:
            val = r.get(key)
            if val is not None:
                print(f"  {key}: {val:.3f}", end="")
        print()

        # MACD
        print(f"  MACD: DIF={r.get('macd_dif', 'N/A')}, DEA={r.get('macd_dea', 'N/A')}, HIST={r.get('macd_hist', 'N/A')}")
        print(f"  MACD金叉: {'是' if r.get('macd_cross') == 1 else '否'} | 零轴上方: {'是' if r.get('macd_above_zero') == 1 else '否'} | 柱状趋势: {'增强' if r.get('macd_hist_trend') == 1 else '减弱'}")

        # KDJ
        print(f"  KDJ: K={r.get('kdj_k', 'N/A'):.1f}, D={r.get('kdj_d', 'N/A'):.1f}, J={r.get('kdj_j', 'N/A'):.1f} | {'超买' if r.get('kdj_overbought') else ('超卖' if r.get('kdj_oversold') else '中性')}")

        # Bollinger
        if r.get("bb_position") is not None:
            print(f"  布林带位置: {r['bb_position']:.1f}% | 带宽: {r.get('bb_bandwidth', 'N/A')} | {'收口' if r.get('bb_squeeze') else '正常'}")

        # RSI
        if r.get("rsi_14") is not None:
            print(f"  RSI(14): {r['rsi_14']:.1f} | RSI(6): {r.get('rsi_6', 'N/A'):.1f}")

        # ADX
        if r.get("adx") is not None:
            adx_val = r['adx']
            trend_str = "强趋势" if adx_val > 25 else "弱趋势/震荡"
            print(f"  ADX: {adx_val:.1f} ({trend_str}) | +DI: {r.get('plus_di', 'N/A'):.1f} | -DI: {r.get('minus_di', 'N/A'):.1f}")

        # Support/Resistance
        if r.get("price_position_20d") is not None:
            print(f"  20日价格位置: {r['price_position_20d']:.1f}% (0=最低, 100=最高)")
            print(f"  距20日高点: {r.get('dist_from_20d_high', 'N/A'):.2f}% | 距20日低点: {r.get('dist_from_20d_low', 'N/A'):.2f}%")

        # Candlestick
        print(f"  K线实体比: {r.get('candle_body_ratio', 0):.2f} | {'阳线' if r.get('is_yang') else '阴线'}")
        print(f"  上影线比: {r.get('upper_shadow_ratio', 0):.2f} | 下影线比: {r.get('lower_shadow_ratio', 0):.2f}")
        print(f"  连涨天数: {r.get('consec_up', 0)} | 连跌天数: {r.get('consec_down', 0)}")

        # Gap
        if r.get("gap_pct") is not None:
            print(f"  跳空: {r['gap_pct']:.2f}%")

        # VWMA vs MA
        for period in [5, 10, 20]:
            val = r.get(f"vwma{period}_vs_ma{period}")
            if val is not None:
                print(f"  VWMA{period} vs MA{period}: {val:.3f}", end="")
        print()

        # OBV
        print(f"  OBV信号: {'看多' if r.get('obv_above_ma5') == 1 else '看空'} | OBV斜率: {r.get('obv_slope', 'N/A')}")

        # Recent limit-up
        print(f"  近期涨停: {r.get('recent_limit_up', 0)}天")

        # Vol price divergence
        vpd = r.get("vol_price_divergence")
        vpd_str = "看多背离" if vpd and vpd > 0 else ("看空背离" if vpd and vpd < 0 else "无背离/确认")
        print(f"  量价背离: {vpd_str}")


def analyze_sell_timing(df: pd.DataFrame, buy_date: str, code: str, name: str, ideal_sell_dates: list):
    """Analyze the ideal sell timing for winners."""
    buy_idx = get_idx_on_date(df, buy_date)
    if buy_idx is None:
        return None

    buy_open = df.iloc[buy_idx]["open"]

    print(f"\n  ── {code} {name} 卖出时机分析 ──")
    print(f"  买入价: {buy_open:.2f}")

    # Show daily performance from buy to sell
    end_idx = min(buy_idx + 20, len(df))
    for i in range(buy_idx, end_idx):
        row = df.iloc[i]
        pct = row.get("pct_change", 0)
        cum_ret = (row["close"] / buy_open - 1) * 100
        vol_ratio = row["volume"] / df.iloc[max(0, i - 5):i]["volume"].mean() if i > 0 and df.iloc[max(0, i - 5):i]["volume"].mean() > 0 else 0

        # MACD
        hist = row.get("macd_hist", 0)

        # KDJ
        k = row.get("kdj_k", 0)

        sell_marker = ""
        date_str = row["date"].strftime("%Y-%m-%d")
        if ideal_sell_dates and date_str in ideal_sell_dates:
            sell_marker = " ★理想卖出"

        turnover = row.get("turnover", 0)

        print(f"  {date_str} | 收盘: {row['close']:.2f} | 日涨跌: {pct:+.2f}% | 累计: {cum_ret:+.2f}% | 量比: {vol_ratio:.2f} | 换手: {turnover:.2f}% | MACD-H: {hist:.4f} | K: {k:.1f}{sell_marker}")

    # Technical signals at sell date
    if ideal_sell_dates:
        for sd in ideal_sell_dates:
            sell_idx = get_idx_on_date(df, sd)
            if sell_idx is not None and sell_idx > 0:
                pre_sell = df.iloc[sell_idx - 1]
                print(f"\n  卖出日前({sd}前一天)技术状态:")
                print(f"    KDJ: K={pre_sell.get('kdj_k', 'N/A')}, D={pre_sell.get('kdj_d', 'N/A')}")
                print(f"    MACD: DIF={pre_sell.get('macd_dif', 'N/A')}, HIST={pre_sell.get('macd_hist', 'N/A')}")
                print(f"    换手率: {pre_sell.get('turnover', 'N/A')}")
                vol_r = pre_sell["volume"] / df.iloc[max(0, sell_idx - 6):sell_idx - 1]["volume"].mean() if sell_idx > 5 else 0
                print(f"    量比: {vol_r:.2f}")

                # Check for overbought signals
                k = pre_sell.get("kdj_k", 0)
                signals = []
                if k > 80:
                    signals.append("KDJ超买")
                if k > 90:
                    signals.append("KDJ极度超买")
                hist = pre_sell.get("macd_hist", 0)
                # Check if MACD hist turning down
                if sell_idx >= 2:
                    prev_hist = df.iloc[sell_idx - 2].get("macd_hist", 0)
                    if prev_hist > hist:
                        signals.append("MACD柱转弱")
                if pre_sell.get("turnover", 0) > df.iloc[max(0, sell_idx - 6):sell_idx]["turnover"].mean() * 1.5:
                    signals.append("放量(可能见顶)")

                if signals:
                    print(f"    卖出信号: {', '.join(signals)}")
                break


def generate_rules(comparison, winners_data, losers_data):
    """Generate trading rules based on analysis."""
    print("\n" + "=" * 120)
    print("交易规则建议 (基于本次数据分析)")
    print("=" * 120)

    # Find significant discriminators
    significant = [c for c in comparison if abs(c["discrimination"]) > 0.3]

    rules = []

    for c in significant[:15]:
        key = c["key"]
        w_mean = c["w_mean"]
        l_mean = c["l_mean"]
        diff = w_mean - l_mean

        if "pct_" in key and "return" not in key:
            if diff > 0:
                rules.append(f"  ✓ 动量({key}): 涨票该值为 {w_mean:.2f}%, 跌票为 {l_mean:.2f}% → 倾向选择正动量股票")
            else:
                rules.append(f"  ✓ 动量({key}): 涨票该值为 {w_mean:.2f}%, 跌票为 {l_mean:.2f}% → 倾向选择低动量/回调股票")

        elif "vol_ratio" in key or "vol_trend" in key:
            if diff > 0:
                rules.append(f"  ✓ 成交量({key}): 涨票均值 {w_mean:.3f}, 跌票 {l_mean:.3f} → 放量更优")
            else:
                rules.append(f"  ✓ 成交量({key}): 涨票均值 {w_mean:.3f}, 跌票 {l_mean:.3f} → 缩量更优")

        elif "close_vs_ma" in key:
            if diff > 0:
                rules.append(f"  ✓ 均线位置({key}): 涨票 {w_mean:.2f}%, 跌票 {l_mean:.2f}% → 价格在均线上方更优")
            else:
                rules.append(f"  ✓ 均线位置({key}): 涨票 {w_mean:.2f}%, 跌票 {l_mean:.2f}% → 价格靠近/低于均线更优")

        elif "bb_" in key:
            rules.append(f"  ✓ 布林带({key}): 涨票 {w_mean:.2f}, 跌票 {l_mean:.2f}")

        elif "rsi" in key:
            rules.append(f"  ✓ RSI({key}): 涨票 {w_mean:.1f}, 跌票 {l_mean:.1f}")

        elif "kdj" in key:
            rules.append(f"  ✓ KDJ({key}): 涨票 {w_mean:.1f}, 跌票 {l_mean:.1f}")

        elif "adx" in key:
            rules.append(f"  ✓ ADX({key}): 涨票 {w_mean:.1f}, 跌票 {l_mean:.1f}")

        elif "turnover" in key:
            rules.append(f"  ✓ 换手率({key}): 涨票 {w_mean:.3f}, 跌票 {l_mean:.3f}")

        elif "vwma" in key:
            rules.append(f"  ✓ VWMA({key}): 涨票 {w_mean:.4f}, 跌票 {l_mean:.4f}")

    for rule in rules:
        print(rule)

    # Generate composite filter suggestions
    print("\n── 综合过滤规则建议 ──")

    # Find threshold values
    for c in significant[:20]:
        key = c["key"]
        w_vals = c["w_vals"]
        l_vals = c["l_vals"]

        # Try to find a threshold that separates winners from losers
        all_vals = sorted([(v, "W") for v in w_vals] + [(v, "L") for v in l_vals])

        best_threshold = None
        best_score = 0

        for i in range(len(all_vals) - 1):
            threshold = (all_vals[i][0] + all_vals[i + 1][0]) / 2

            # Above threshold
            w_above = sum(1 for v, t in all_vals if v >= threshold and t == "W")
            l_above = sum(1 for v, t in all_vals if v >= threshold and t == "L")
            w_below = sum(1 for v, t in all_vals if v < threshold and t == "W")
            l_below = sum(1 for v, t in all_vals if v < threshold and t == "L")

            # Score: how well does this threshold separate?
            if w_above + l_above > 0:
                precision_above = w_above / (w_above + l_above)
            else:
                precision_above = 0
            if w_below + l_below > 0:
                precision_below = l_below / (w_below + l_below)
            else:
                precision_below = 0

            score = max(precision_above * (w_above + l_above), precision_below * (w_below + l_below))

            if score > best_score:
                best_score = score
                best_threshold = threshold
                best_direction = "above" if w_mean > l_mean else "below"

        if best_threshold is not None and best_score > 0.6:
            direction = f">= {best_threshold:.3f}" if c["w_mean"] > c["l_mean"] else f"< {best_threshold:.3f}"
            print(f"  {key} {direction} → 精确度 {best_score:.1%}")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print("正在加载股票数据并进行分析...\n")

    all_results = []
    winners_data = []
    losers_data = []

    for buy_date, stocks in TRADES.items():
        for code, info in stocks.items():
            df = load_stock(code)
            if df is None:
                print(f"⚠ 无法加载 {code} {info['name']}")
                continue

            sell_dates = info.get("ideal_sell")
            r = analyze_stock_at_date(df, buy_date, code, info["name"], sell_dates)
            if r is None:
                print(f"⚠ 数据不足 {code} {info['name']}")
                continue

            all_results.append(r)

            if info["result"] in ("大涨", "涨", "小涨"):
                winners_data.append(r)
            else:
                losers_data.append(r)

    print(f"成功分析 {len(all_results)} 只股票 (涨票: {len(winners_data)}, 跌票: {len(losers_data)})")

    # Print individual analysis
    print_individual_analysis(all_results)

    # Compare groups
    if winners_data and losers_data:
        comparison = compare_groups(winners_data, losers_data)
        generate_rules(comparison, winners_data, losers_data)

    # Sell timing analysis
    print("\n" + "=" * 120)
    print("卖出时机分析")
    print("=" * 120)

    for buy_date, stocks in TRADES.items():
        for code, info in stocks.items():
            if info.get("ideal_sell"):
                df = load_stock(code)
                if df is not None:
                    analyze_sell_timing(df, buy_date, code, info["name"], info["ideal_sell"])

    # Final summary
    print("\n" + "=" * 120)
    print("最终总结")
    print("=" * 120)

    print(f"\n总股票数: {len(all_results)}")
    print(f"涨票: {len(winners_data)} 只")
    print(f"跌票: {len(losers_data)} 只")

    if winners_data:
        avg_w_return = np.mean([r.get("return_5d", 0) or 0 for r in winners_data])
        print(f"涨票平均5日收益: {avg_w_return:.2f}%")
    if losers_data:
        avg_l_return = np.mean([r.get("return_5d", 0) or 0 for r in losers_data])
        print(f"跌票平均5日收益: {avg_l_return:.2f}%")


if __name__ == "__main__":
    main()
