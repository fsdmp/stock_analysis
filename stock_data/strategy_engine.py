"""Strategy-based stock screening engine.

Evaluates JSON strategy definitions (DSL v2.0) against stock DataFrames.
Supports nested AND/OR/NOT logic with 30+ operators covering technical
indicators, K-line patterns, volume-price relationships, and derived fields.
"""

import math

import numpy as np
import pandas as pd

# --- DataFrame columns that exist directly in parquet ---
_DF_COLUMNS = {
    "date", "code", "open", "close", "high", "low", "volume", "amount",
    "pct_change", "change", "turnover", "amplitude",
    "ma5", "ma7", "ma10", "ma20",
    "v_ma5", "v_ma7", "v_ma10", "v_ma20",
    "vwma5", "vwma10", "vwma20",
    "macd_dif", "macd_dea", "macd_hist",
    "kdj_k", "kdj_d", "kdj_j",
    "bb_upper", "bb_middle", "bb_lower", "bb_bandwidth",
    "obv", "vol_ratio",
}

# Fields that do NOT map to a DataFrame column
_META_FIELDS = {
    "code_prefix", "is_st", "trading_status",
    "is_limit_up", "is_limit_down",
    "limit_up_count", "limit_down_count",
    "continuous_limit_up", "continuous_limit_down",
    "continuous_up", "continuous_down",
    "pct_change_sum", "turnover_avg", "amount_avg",
    "high_max", "low_min", "amplitude_max", "vol_ratio_avg",
    "kline", "vol_price", "ma_align",
}

# Base columns always needed for derived field computation
_BASE_COLUMNS = [
    "date", "open", "close", "high", "low", "volume", "amount",
    "pct_change", "turnover",
]


class StrategyEngine:
    """Evaluate a strategy definition against stock data."""

    def __init__(self, strategy: dict):
        self.strategy = strategy
        self.name = strategy.get("name", "")
        self.filter_root = strategy["filter"]

    def evaluate(self, df: pd.DataFrame, code: str, stock_name: str) -> bool:
        return _eval_group(self.filter_root, df, code, stock_name)

    def get_required_columns(self) -> list[str]:
        """Collect all DataFrame columns referenced in the strategy."""
        cols = set(_BASE_COLUMNS)
        _collect_columns(self.filter_root, cols)
        return sorted(cols)


# ---------------------------------------------------------------------------
# Recursive evaluation
# ---------------------------------------------------------------------------

def _eval_group(group: dict, df: pd.DataFrame, code: str, stock_name: str) -> bool:
    logic = group.get("logic", "AND")
    rules = group.get("rules", [])

    if logic == "NOT":
        return not any(_eval_rule(r, df, code, stock_name) for r in rules)

    if logic == "AND":
        # Optimization: sort by estimated cost (cheap first)
        ordered = sorted(rules, key=_rule_cost)
        return all(_eval_rule(r, df, code, stock_name) for r in ordered)

    # OR
    return any(_eval_rule(r, df, code, stock_name) for r in rules)


def _eval_rule(rule: dict, df: pd.DataFrame, code: str, stock_name: str) -> bool:
    if "logic" in rule and "rules" in rule:
        return _eval_group(rule, df, code, stock_name)
    return _eval_condition(rule, df, code, stock_name)


def _rule_cost(rule: dict) -> int:
    """Heuristic cost for AND-group short-circuit ordering."""
    if "logic" in rule:
        return 5
    field = rule.get("field", "")
    op = rule.get("op", "")
    if field in ("code_prefix", "is_st", "trading_status"):
        return 0
    if op in ("in", "not_in", "contains", "starts_with", "=", "!="):
        return 1
    if op in (">", ">=", "<", "<="):
        return 2
    if op in ("cross_above", "cross_below", "in_zone", "is_shape"):
        return 3
    if op in ("rising", "falling", "is_new_high", "is_new_low", "ma_align", "vol_price"):
        return 4
    return 5


# ---------------------------------------------------------------------------
# Condition dispatch
# ---------------------------------------------------------------------------

def _eval_condition(rule: dict, df: pd.DataFrame, code: str, stock_name: str) -> bool:
    field = rule.get("field", "")
    op = rule.get("op", "")
    offset = rule.get("offset", 0)
    n = len(df)
    idx = n - 1 - offset

    if idx < 0 or idx >= n:
        return False

    # --- Special field dispatches ---
    if field == "kline":
        return _check_kline_shape(df, idx, rule.get("value", ""))
    if field == "vol_price":
        return _check_vol_price(df, idx, rule.get("value", ""))
    if field == "ma_align":
        return _check_ma_align(df, idx, rule.get("mas", []), rule.get("value", ""))
    if op == "in_zone":
        return _check_in_zone(df, idx, field, rule.get("value", ""))
    if op in ("cross_above", "cross_below"):
        return _check_cross(df, idx, field, rule.get("ref", ""), op)
    if op in ("rising", "falling"):
        return _check_rising_falling(df, idx, field, rule.get("lookback", 5), op)
    if op in ("is_new_high", "is_new_low"):
        return _check_new_extrema(df, idx, field, rule.get("lookback", 20), op)

    # --- General comparison ---
    lookback = rule.get("lookback")
    aggregate = rule.get("aggregate")
    ref = rule.get("ref")

    lhs = _resolve_field(field, df, code, stock_name, idx, lookback, aggregate)
    if ref:
        rhs = _resolve_field(ref, df, code, stock_name, idx)
    else:
        rhs = rule.get("value")

    if lhs is None or rhs is None:
        return False

    return _compare(op, lhs, rhs)


def _compare(op: str, lhs, rhs) -> bool:
    try:
        if op == ">":
            return lhs > rhs
        if op == ">=":
            return lhs >= rhs
        if op == "<":
            return lhs < rhs
        if op == "<=":
            return lhs <= rhs
        if op == "=":
            return lhs == rhs
        if op == "!=":
            return lhs != rhs
        if op == "in":
            return lhs in rhs
        if op == "not_in":
            return lhs not in rhs
        if op == "contains":
            return rhs in str(lhs)
        if op == "not_contains":
            return rhs not in str(lhs)
        if op == "starts_with":
            return str(lhs).startswith(str(rhs))
    except (TypeError, ValueError):
        return False
    return False


# ---------------------------------------------------------------------------
# Field resolver
# ---------------------------------------------------------------------------

def _resolve_field(
    field: str,
    df: pd.DataFrame,
    code: str,
    stock_name: str,
    idx: int,
    lookback: int | None = None,
    aggregate: str | None = None,
):
    """Resolve the value of a field at a given index."""
    n = len(df)

    # --- Meta / computed fields (no DataFrame column) ---
    if field == "code_prefix":
        return code[:2]
    if field == "is_st":
        return "ST" in stock_name.upper()
    if field == "trading_status":
        vol = _safe(df, idx, "volume")
        return "交易" if vol is not None and vol > 0 else "停牌"
    if field == "is_limit_up":
        v = _safe(df, idx, "pct_change")
        return v is not None and v >= 9.5
    if field == "is_limit_down":
        v = _safe(df, idx, "pct_change")
        return v is not None and v <= -9.5

    # --- Aggregate lookback fields ---
    if lookback and aggregate:
        return _resolve_aggregate(field, df, idx, lookback, aggregate)

    # --- Derived statistical fields ---
    if field == "limit_up_count":
        lb = lookback or 10
        start = max(0, idx - lb + 1)
        vals = df["pct_change"].iloc[start:idx + 1]
        return int((vals >= 9.5).sum())
    if field == "limit_down_count":
        lb = lookback or 10
        start = max(0, idx - lb + 1)
        vals = df["pct_change"].iloc[start:idx + 1]
        return int((vals <= -9.5).sum())
    if field == "continuous_limit_up":
        count = 0
        for i in range(idx, -1, -1):
            v = _safe(df, i, "pct_change")
            if v is not None and v >= 9.5:
                count += 1
            else:
                break
        return count
    if field == "continuous_limit_down":
        count = 0
        for i in range(idx, -1, -1):
            v = _safe(df, i, "pct_change")
            if v is not None and v <= -9.5:
                count += 1
            else:
                break
        return count
    if field == "continuous_up":
        count = 0
        for i in range(idx, 0, -1):
            if _safe(df, i, "close") is not None and _safe(df, i - 1, "close") is not None:
                if df["close"].iloc[i] > df["close"].iloc[i - 1]:
                    count += 1
                else:
                    break
            else:
                break
        return count
    if field == "continuous_down":
        count = 0
        for i in range(idx, 0, -1):
            if _safe(df, i, "close") is not None and _safe(df, i - 1, "close") is not None:
                if df["close"].iloc[i] < df["close"].iloc[i - 1]:
                    count += 1
                else:
                    break
            else:
                break
        return count
    if field == "pct_change_sum":
        lb = lookback or 10
        start = max(0, idx - lb + 1)
        vals = df["pct_change"].iloc[start:idx + 1].dropna()
        return float(vals.sum()) if len(vals) > 0 else None
    if field == "turnover_avg":
        lb = lookback or 10
        start = max(0, idx - lb + 1)
        vals = df["turnover"].iloc[start:idx + 1].dropna()
        return float(vals.mean()) if len(vals) > 0 else None
    if field == "amount_avg":
        lb = lookback or 10
        start = max(0, idx - lb + 1)
        vals = df["amount"].iloc[start:idx + 1].dropna()
        return float(vals.mean()) if len(vals) > 0 else None
    if field == "high_max":
        lb = lookback or 20
        start = max(0, idx - lb + 1)
        vals = df["high"].iloc[start:idx + 1].dropna()
        return float(vals.max()) if len(vals) > 0 else None
    if field == "low_min":
        lb = lookback or 20
        start = max(0, idx - lb + 1)
        vals = df["low"].iloc[start:idx + 1].dropna()
        return float(vals.min()) if len(vals) > 0 else None
    if field == "amplitude_max":
        lb = lookback or 10
        start = max(0, idx - lb + 1)
        h = df["high"].iloc[start:idx + 1].dropna()
        l = df["low"].iloc[start:idx + 1].dropna()
        c = df["close"].iloc[start:idx + 1].dropna()
        if len(h) == 0:
            return None
        amps = ((h.values - l.values) / c.values * 100)
        return float(np.nanmax(amps)) if len(amps) > 0 else None
    if field == "vol_ratio_avg":
        lb = lookback or 10
        start = max(0, idx - lb + 1)
        vals = df["vol_ratio"].iloc[start:idx + 1].dropna()
        return float(vals.mean()) if len(vals) > 0 else None

    # --- Direct DataFrame column ---
    if field in _DF_COLUMNS:
        return _safe(df, idx, field)

    return None


def _resolve_aggregate(field: str, df: pd.DataFrame, idx: int, lookback: int, aggregate: str):
    """Compute aggregate of a field over a lookback window."""
    start = max(0, idx - lookback + 1)
    series = df[field].iloc[start:idx + 1].dropna()
    if len(series) == 0:
        return None
    if aggregate == "avg":
        return float(series.mean())
    if aggregate == "sum":
        return float(series.sum())
    if aggregate == "max":
        return float(series.max())
    if aggregate == "min":
        return float(series.min())
    return None


def _safe(df: pd.DataFrame, idx: int, col: str):
    """Safe accessor returning None for NaN or out-of-bounds."""
    if col not in df.columns or idx < 0 or idx >= len(df):
        return None
    val = df[col].iloc[idx]
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or np.isnan(val)):
        return None
    return val


# ---------------------------------------------------------------------------
# Cross detection
# ---------------------------------------------------------------------------

def _check_cross(df: pd.DataFrame, idx: int, field: str, ref: str, op: str) -> bool:
    if idx < 1:
        return False
    cur_f = _safe(df, idx, field)
    cur_r = _safe(df, idx, ref)
    prev_f = _safe(df, idx - 1, field)
    prev_r = _safe(df, idx - 1, ref)
    if any(v is None for v in [cur_f, cur_r, prev_f, prev_r]):
        return False
    if op == "cross_above":
        return prev_f <= prev_r and cur_f > cur_r
    # cross_below
    return prev_f >= prev_r and cur_f < cur_r


# ---------------------------------------------------------------------------
# Trend: rising / falling / new high / new low
# ---------------------------------------------------------------------------

def _check_rising_falling(df: pd.DataFrame, idx: int, field: str, lookback: int, op: str) -> bool:
    start = max(0, idx - lookback + 1)
    if start >= idx:
        return False
    vals = df[field].iloc[start:idx + 1].dropna()
    if len(vals) < 2:
        return False
    if op == "rising":
        return bool((vals.diff().iloc[1:] > 0).all())
    # falling
    return bool((vals.diff().iloc[1:] < 0).all())


def _check_new_extrema(df: pd.DataFrame, idx: int, field: str, lookback: int, op: str) -> bool:
    start = max(0, idx - lookback + 1)
    cur = _safe(df, idx, field)
    if cur is None:
        return False
    vals = df[field].iloc[start:idx + 1].dropna()
    if len(vals) == 0:
        return False
    if op == "is_new_high":
        return cur >= vals.max()
    # is_new_low
    return cur <= vals.min()


# ---------------------------------------------------------------------------
# K-line shapes
# ---------------------------------------------------------------------------

def _check_kline_shape(df: pd.DataFrame, idx: int, shape: str) -> bool:
    o = _safe(df, idx, "open")
    c = _safe(df, idx, "close")
    h = _safe(df, idx, "high")
    l = _safe(df, idx, "low")
    pct = _safe(df, idx, "pct_change")
    if any(v is None for v in [o, c, h, l, pct]):
        return False

    body = abs(c - o)
    rng = h - l
    us = h - max(c, o)
    ls = min(c, o) - l

    if shape == "大阳线":
        return pct > 5 and (rng == 0 or body / rng > 0.6)
    if shape == "大阴线":
        return pct < -5 and (rng == 0 or body / rng > 0.6)
    if shape == "小阳线":
        return 0 < pct < 3
    if shape == "小阴线":
        return -3 < pct < 0
    if shape == "十字星":
        return rng > 0 and body / rng < 0.1
    if shape == "长上影":
        return body > 0 and us > body * 2
    if shape == "长下影":
        return body > 0 and ls > body * 2
    if shape == "锤子线":
        return body > 0 and ls > body * 2 and us < body * 0.5 and c > o
    if shape == "倒锤子":
        return body > 0 and us > body * 2 and ls < body * 0.5
    if shape == "一字板":
        return o == h == l == c
    if shape == "T字板":
        return o == c == h and l < o
    if shape == "倒T字板":
        return o == c == l and h > o
    return False


# ---------------------------------------------------------------------------
# Volume-price patterns
# ---------------------------------------------------------------------------

def _check_vol_price(df: pd.DataFrame, idx: int, pattern: str) -> bool:
    vr = _safe(df, idx, "vol_ratio")
    pct = _safe(df, idx, "pct_change")
    if vr is None or pct is None:
        return False

    if pattern == "放量上涨":
        return vr > 1.5 and pct > 1
    if pattern == "缩量上涨":
        return vr < 0.7 and pct > 0
    if pattern == "放量下跌":
        return vr > 1.5 and pct < -1
    if pattern == "缩量下跌":
        return vr < 0.7 and pct < 0
    if pattern == "放量滞涨":
        return vr > 2 and abs(pct) < 1

    if pattern == "天量":
        if idx < 1:
            return False
        vol = _safe(df, idx, "volume")
        if vol is None or vol <= 0:
            return False
        start = max(0, idx - 120)
        return bool((df["volume"].iloc[start:idx] < vol).all())

    if pattern == "地量":
        if idx < 1:
            return False
        vol = _safe(df, idx, "volume")
        if vol is None:
            return False
        start = max(0, idx - 30)
        prev = df["volume"].iloc[start:idx]
        prev = prev[prev > 0]
        if len(prev) == 0:
            return False
        return bool(vol <= prev.min())

    return False


# ---------------------------------------------------------------------------
# MA alignment
# ---------------------------------------------------------------------------

def _check_ma_align(df: pd.DataFrame, idx: int, mas: list[str], align: str) -> bool:
    if not mas:
        return False
    vals = []
    for m in mas:
        v = _safe(df, idx, m)
        if v is None:
            return False
        vals.append(v)

    if align == "多头排列":
        return all(vals[i] > vals[i + 1] for i in range(len(vals) - 1))
    if align == "空头排列":
        return all(vals[i] < vals[i + 1] for i in range(len(vals) - 1))
    if align == "粘合":
        mid = vals[len(vals) // 2]
        if mid == 0:
            return False
        spread = (max(vals) - min(vals)) / abs(mid) * 100
        return spread < 1.2
    return False


# ---------------------------------------------------------------------------
# Zone checks
# ---------------------------------------------------------------------------

def _check_in_zone(df: pd.DataFrame, idx: int, field: str, zone: str) -> bool:
    v = _safe(df, idx, field)
    if v is None:
        return False

    # KDJ zones
    if zone == "超买":
        k = _safe(df, idx, "kdj_k")
        d = _safe(df, idx, "kdj_d")
        return k is not None and d is not None and k > 80 and d > 70
    if zone == "超卖":
        k = _safe(df, idx, "kdj_k")
        d = _safe(df, idx, "kdj_d")
        return k is not None and d is not None and k < 20 and d < 30

    # MACD zero line
    if zone == "零轴上方":
        return v > 0
    if zone == "零轴下方":
        return v < 0

    # MACD histogram
    if zone == "红柱":
        return v > 0
    if zone == "绿柱":
        return v < 0
    if zone in ("红柱放大", "绿柱放大"):
        if idx < 1:
            return False
        prev = _safe(df, idx - 1, "macd_hist")
        if prev is None:
            return False
        if zone == "红柱放大":
            return v > 0 and v > prev
        return v < 0 and v < prev

    # Bollinger zones
    close = _safe(df, idx, "close")
    bb_upper = _safe(df, idx, "bb_upper")
    bb_lower = _safe(df, idx, "bb_lower")
    bb_middle = _safe(df, idx, "bb_middle")
    if any(x is None for x in [close, bb_upper, bb_lower]):
        return False
    if zone == "布林上轨外":
        return close > bb_upper
    if zone == "布林下轨外":
        return close < bb_lower
    if zone == "布林中轨上":
        return bb_lower < close < bb_upper and close > bb_middle
    if zone == "布林中轨下":
        return bb_lower < close < bb_middle

    return False


# ---------------------------------------------------------------------------
# Column collection (for parquet column pruning)
# ---------------------------------------------------------------------------

def _collect_columns(group: dict, cols: set):
    """Walk the strategy tree and collect all referenced DataFrame columns."""
    for rule in group.get("rules", []):
        if "logic" in rule:
            _collect_columns(rule, cols)
            continue

        field = rule.get("field", "")
        ref = rule.get("ref", "")

        for f in (field, ref):
            if f in _DF_COLUMNS:
                cols.add(f)

        # ma_align references MA columns
        for m in rule.get("mas", []):
            if m in _DF_COLUMNS:
                cols.add(m)

        # in_zone may need extra columns
        if rule.get("op") == "in_zone":
            zone = rule.get("value", "")
            if zone in ("超买", "超卖"):
                cols.update(["kdj_k", "kdj_d"])
            elif "布林" in zone:
                cols.update(["close", "bb_upper", "bb_lower", "bb_middle"])
            elif "柱" in zone:
                cols.add("macd_hist")
