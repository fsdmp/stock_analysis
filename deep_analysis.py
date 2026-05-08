"""
Deep comparative analysis: What truly separates winners from losers?
Focus on pre-buy signals only (no future data leakage).
"""

import os
import numpy as np
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "stocks")

TRADES = {
    "2026-04-17": {
        "600773": {"name": "西藏城投", "result": "大涨", "result_cat": "win"},
        "603682": {"name": "锦和商管", "result": "涨", "result_cat": "win"},
        "002733": {"name": "雄韬股份", "result": "跌", "result_cat": "lose"},
        "002074": {"name": "国轩高科", "result": "大跌", "result_cat": "lose"},
        "603738": {"name": "泰晶科技", "result": "涨", "result_cat": "win"},
        "600654": {"name": "中安科", "result": "大跌", "result_cat": "lose"},
        "600527": {"name": "江南高纤", "result": "涨", "result_cat": "win"},
    },
    "2026-04-20": {
        "600855": {"name": "航天长峰", "result": "大跌", "result_cat": "lose"},
        "603109": {"name": "神驰机电", "result": "大跌", "result_cat": "lose"},
        "002943": {"name": "宇晶股份", "result": "涨", "result_cat": "win"},
        "002515": {"name": "金字火腿", "result": "涨", "result_cat": "win"},
        "002436": {"name": "兴森科技", "result": "涨", "result_cat": "win"},
        "000612": {"name": "焦作万方", "result": "跌", "result_cat": "lose"},
        "000791": {"name": "甘肃能源", "result": "跌", "result_cat": "lose"},
    },
    "2026-04-21": {
        "603815": {"name": "交建股份", "result": "涨", "result_cat": "win"},
        "605365": {"name": "立达信", "result": "涨", "result_cat": "win"},
        "605366": {"name": "宏柏新材", "result": "跌", "result_cat": "lose"},
        "605098": {"name": "行动教育", "result": "大跌", "result_cat": "lose"},
        "603150": {"name": "万朗磁塑", "result": "跌", "result_cat": "lose"},
        "600135": {"name": "乐凯胶片", "result": "跌", "result_cat": "lose"},
        "002463": {"name": "沪电股份", "result": "小涨", "result_cat": "win"},
    },
}


def load_stock(code):
    for pattern in [code, f"SH{code}", f"SZ{code}"]:
        path = os.path.join(DATA_DIR, f"{pattern}.parquet")
        if os.path.exists(path):
            df = pd.read_parquet(path)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            return df
    return None


def get_idx(df, date_str):
    target = pd.Timestamp(date_str)
    mask = df["date"] >= target
    return mask.idxmax() if mask.any() else None


def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def compute_atr(df, period=14):
    tr1 = df["high"] - df["low"]
    tr2 = abs(df["high"] - df["close"].shift(1))
    tr3 = abs(df["low"] - df["close"].shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=period).mean()


def compute_adx(df, period=14):
    plus_dm = df["high"].diff()
    minus_dm = -df["low"].diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
    atr = compute_atr(df, period)
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr)
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.rolling(period).mean()
    return adx, plus_di, minus_di


def extract_features(df, buy_date_str):
    """Extract all pre-buy features (no future leakage)."""
    buy_idx = get_idx(df, buy_date_str)
    if buy_idx is None or buy_idx < 25:
        return None

    pre = buy_idx - 1  # last closed day
    row = df.iloc[pre]
    f = {}

    close = row["close"]
    open_ = row["open"]
    high = row["high"]
    low = row["low"]
    pct = row["pct_change"]

    # ─── A. 动量过热信号 (关键区分维度) ────────────────────────────
    # 3-day momentum vs 10-day momentum ratio → 是否短期加速过快
    cum3 = (close / df.iloc[pre - 3]["close"] - 1) * 100 if pre >= 3 else 0
    cum5 = (close / df.iloc[pre - 5]["close"] - 1) * 100 if pre >= 5 else 0
    cum10 = (close / df.iloc[pre - 10]["close"] - 1) * 100 if pre >= 10 else 0

    f["pct_3d"] = cum3
    f["pct_5d"] = cum5
    f["pct_10d"] = cum10

    # 动量加速度: 3日动量 vs 5日动量的比例 → 越大说明短期加速越快(可能过热)
    f["momentum_accel"] = cum3 / max(abs(cum5), 0.1) * (1 if cum5 > 0 else -1) if abs(cum5) > 0.1 else 0
    # 5日 vs 10日
    f["momentum_accel_5_10"] = cum5 / max(abs(cum10), 0.1) * (1 if cum10 > 0 else -1) if abs(cum10) > 0.1 else 0

    # ─── B. 均线偏离度综合 ────────────────────────────────────────
    for ma in ["ma5", "ma7", "ma10", "ma20"]:
        val = row.get(ma)
        if val and not np.isnan(val) and val > 0:
            f[f"dev_{ma}"] = (close - val) / val * 100
        else:
            f[f"dev_{ma}"] = None

    # 均线乖离率综合: 对MA5的偏离
    f["bias_ma5"] = f.get("dev_ma5", 0)

    # 均线排列得分
    mas = []
    for ma in ["ma5", "ma7", "ma10", "ma20"]:
        val = row.get(ma)
        if val and not np.isnan(val):
            mas.append(val)
    if len(mas) == 4:
        # Count how many are in order
        score = 0
        for i in range(len(mas) - 1):
            if mas[i] > mas[i + 1]:
                score += 1
            elif mas[i] < mas[i + 1]:
                score -= 1
        f["ma_order_score"] = score  # +3 perfect bull, -3 perfect bear
    else:
        f["ma_order_score"] = None

    # ─── C. 成交量质量 ────────────────────────────────────────────
    vol = row["volume"]
    avg5 = df.iloc[max(0, pre - 4):pre + 1]["volume"].mean()
    avg10 = df.iloc[max(0, pre - 9):pre + 1]["volume"].mean()
    avg20 = df.iloc[max(0, pre - 19):pre + 1]["volume"].mean()

    f["vol_ratio_5"] = vol / avg5 if avg5 > 0 else 1
    f["vol_ratio_20"] = vol / avg20 if avg20 > 0 else 1

    # 上涨日vs下跌日成交量比
    recent_5 = df.iloc[max(0, pre - 4):pre + 1]
    up_vols = recent_5[recent_5["pct_change"] > 0]["volume"]
    dn_vols = recent_5[recent_5["pct_change"] < 0]["volume"]
    f["vol_up_dn_ratio"] = up_vols.mean() / dn_vols.mean() if len(dn_vols) > 0 and dn_vols.mean() > 0 else (999 if len(up_vols) > 0 else 0)

    # 量价配合度: 最近5天里，涨日放量+跌日缩量的天数
    vol_price_ok = 0
    for i in range(max(0, pre - 4), pre + 1):
        r = df.iloc[i]
        if r["pct_change"] > 0 and r["volume"] > avg5:
            vol_price_ok += 1
        elif r["pct_change"] < 0 and r["volume"] < avg5:
            vol_price_ok += 1
    f["vol_price_confirm_5d"] = vol_price_ok

    # ─── D. KDJ状态 ──────────────────────────────────────────────
    k = row.get("kdj_k", np.nan)
    d = row.get("kdj_d", np.nan)
    j = row.get("kdj_j", np.nan)

    if not np.isnan(k):
        f["kdj_k"] = k
        f["kdj_j"] = j
        # KDJ是否在高位钝化: K>D且K>80
        f["kdj_overbought_strong"] = 1 if (k > 80 and k > d) else 0
        # J值极端
        f["kdj_j_extreme"] = abs(j - 100) if not np.isnan(j) else 0
    else:
        f["kdj_k"] = None
        f["kdj_j"] = None
        f["kdj_overbought_strong"] = None
        f["kdj_j_extreme"] = None

    # ─── E. RSI ──────────────────────────────────────────────────
    rsi14 = compute_rsi(df["close"], 14)
    rsi6 = compute_rsi(df["close"], 6)
    f["rsi_14"] = rsi14.iloc[pre] if pre < len(rsi14) else None
    f["rsi_6"] = rsi6.iloc[pre] if pre < len(rsi6) else None

    # RSI与KDJ的超买共振
    r14 = f.get("rsi_14", 50)
    f["overbought_resonance"] = (1 if k > 80 else 0) + (1 if (r14 and r14 > 70) else 0)

    # ─── F. MACD ─────────────────────────────────────────────────
    dif = row.get("macd_dif", np.nan)
    dea = row.get("macd_dea", np.nan)
    hist = row.get("macd_hist", np.nan)

    if not np.isnan(dif):
        f["macd_dif"] = dif
        f["macd_hist"] = hist
        f["macd_cross"] = 1 if dif > dea else -1
        f["macd_above_zero"] = 1 if dif > 0 else -1

        # MACD柱状连续增强天数
        hist_increase_days = 0
        for i in range(pre, max(pre - 5, -1), -1):
            h = df.iloc[i].get("macd_hist", np.nan)
            h_prev = df.iloc[i - 1].get("macd_hist", np.nan) if i > 0 else np.nan
            if not np.isnan(h) and not np.isnan(h_prev) and h > h_prev:
                hist_increase_days += 1
            else:
                break
        f["macd_hist_increase_days"] = hist_increase_days

        # MACD柱状值相对大小 (相对于价格)
        f["macd_hist_pct"] = hist / close * 100 if close > 0 else 0
    else:
        for key in ["macd_dif", "macd_hist", "macd_cross", "macd_above_zero", "macd_hist_increase_days", "macd_hist_pct"]:
            f[key] = None

    # ─── G. ADX (趋势强度) ──────────────────────────────────────
    adx, pdi, mdi = compute_adx(df, 14)
    f["adx"] = adx.iloc[pre] if pre < len(adx) else None
    f["plus_di"] = pdi.iloc[pre] if pre < len(pdi) else None
    f["minus_di"] = mdi.iloc[pre] if pre < len(mdi) else None
    adx_val = f.get("adx", 0)
    pdi_val = f.get("plus_di", 0)
    mdi_val = f.get("minus_di", 0)
    f["di_diff"] = (pdi_val - mdi_val) if pdi_val and mdi_val else None

    # ─── H. 换手率 ───────────────────────────────────────────────
    turnover = row.get("turnover", np.nan)
    f["turnover"] = turnover
    if pre >= 5 and not np.isnan(turnover):
        avg_to = df.iloc[pre - 4:pre + 1]["turnover"].mean()
        f["turnover_ratio"] = turnover / avg_to if avg_to > 0 else 1
    else:
        f["turnover_ratio"] = None

    # ─── I. K线形态 ──────────────────────────────────────────────
    body = abs(close - open_)
    total_range = high - low
    f["body_ratio"] = body / total_range if total_range > 0 else 0
    f["upper_shadow"] = (high - max(close, open_)) / total_range if total_range > 0 else 0
    f["lower_shadow"] = (min(close, open_) - low) / total_range if total_range > 0 else 0
    f["is_yang"] = close > open_

    # ─── J. 价格位置 ─────────────────────────────────────────────
    if pre >= 20:
        h20 = df.iloc[pre - 19:pre + 1]["high"].max()
        l20 = df.iloc[pre - 19:pre + 1]["low"].min()
        f["price_pos_20d"] = (close - l20) / (h20 - l20) * 100 if h20 > l20 else 50
        f["dist_from_high_20d"] = (close - h20) / h20 * 100
    else:
        f["price_pos_20d"] = None
        f["dist_from_high_20d"] = None

    # ─── K. 连涨连跌 ─────────────────────────────────────────────
    consec_up = 0
    for i in range(pre, max(pre - 10, -1), -1):
        if df.iloc[i].get("pct_change", 0) > 0:
            consec_up += 1
        else:
            break
    f["consec_up"] = consec_up

    # ─── L. 跳空 ─────────────────────────────────────────────────
    if pre >= 1:
        f["gap"] = (open_ - df.iloc[pre - 1]["close"]) / df.iloc[pre - 1]["close"] * 100
    else:
        f["gap"] = None

    # ─── M. VWMA vs MA (量价背离) ────────────────────────────────
    for p in [5, 10, 20]:
        ma_v = row.get(f"ma{p}")
        vwma_v = row.get(f"vwma{p}")
        if (ma_v is not None and vwma_v is not None
            and not np.isnan(ma_v) and not np.isnan(vwma_v) and ma_v > 0):
            f[f"vwma{p}_diff"] = (vwma_v - ma_v) / ma_v * 100
        else:
            f[f"vwma{p}_diff"] = None

    # ─── N. OBV斜率 ──────────────────────────────────────────────
    if pre >= 10:
        obv = df["obv"]
        obv_ma5 = obv.rolling(5).mean()
        f["obv_slope_5d"] = obv_ma5.iloc[pre] - obv_ma5.iloc[pre - 5]
    else:
        f["obv_slope_5d"] = None

    # ─── O. 布林带 ───────────────────────────────────────────────
    bb_upper = row.get("bb_upper")
    bb_lower = row.get("bb_lower")
    bb_bw = row.get("bb_bandwidth")
    if bb_upper is not None and not np.isnan(bb_upper) and bb_upper > bb_lower:
        f["bb_pos"] = (close - bb_lower) / (bb_upper - bb_lower) * 100
        f["bb_bw"] = bb_bw
    else:
        f["bb_pos"] = None
        f["bb_bw"] = None

    # ─── P. 波动率 ───────────────────────────────────────────────
    if pre >= 10:
        rets = df.iloc[pre - 9:pre + 1]["pct_change"].dropna()
        f["volatility_10d"] = rets.std()
    else:
        f["volatility_10d"] = None

    # ─── Q. 涨停相关 ─────────────────────────────────────────────
    limit_up_count = 0
    for i in range(pre, max(pre - 5, -1), -1):
        if df.iloc[i].get("pct_change", 0) >= 9.5:
            limit_up_count += 1
        else:
            break
    f["recent_limit_up"] = limit_up_count

    # ─── Actual returns (for verification only) ─────────────────
    buy_open = df.iloc[buy_idx]["open"]
    f["buy_open"] = buy_open
    for nd in [1, 2, 3, 5, 10]:
        if buy_idx + nd < len(df):
            f[f"ret_{nd}d"] = (df.iloc[buy_idx + nd]["close"] / buy_open - 1) * 100
        else:
            f[f"ret_{nd}d"] = None

    return f


def main():
    print("=" * 100)
    print("深度区分分析: 涨票 vs 跌票 的买入前技术面差异")
    print("=" * 100)

    all_features = []
    for buy_date, stocks in TRADES.items():
        for code, info in stocks.items():
            df = load_stock(code)
            if df is None:
                continue
            f = extract_features(df, buy_date)
            if f is None:
                continue
            f["code"] = code
            f["name"] = info["name"]
            f["result"] = info["result"]
            f["cat"] = info["result_cat"]
            f["buy_date"] = buy_date
            all_features.append(f)

    winners = [f for f in all_features if f["cat"] == "win"]
    losers = [f for f in all_features if f["cat"] == "lose"]

    print(f"\n涨票: {len(winners)} 只, 跌票: {len(losers)} 只\n")

    # ─── 逐个打印 ────────────────────────────────────────────────
    print("── 涨票买入前关键指标 ──\n")
    for f in winners:
        print(f"  {f['code']} {f['name']} ({f['buy_date']})")
        print(f"    动量: 3d={f.get('pct_3d', 'N/A'):.1f}%, 5d={f.get('pct_5d', 'N/A'):.1f}%, 10d={f.get('pct_10d', 'N/A'):.1f}%")
        print(f"    偏离MA: MA5={f.get('dev_ma5', 'N/A'):.1f}%, MA10={f.get('dev_ma10', 'N/A'):.1f}%, MA20={f.get('dev_ma20', 'N/A'):.1f}%")
        print(f"    KDJ: K={f.get('kdj_k', 'N/A'):.1f}, J={f.get('kdj_j', 'N/A'):.1f}, 超买={f.get('kdj_overbought_strong', 'N/A')}")
        print(f"    RSI14={f.get('rsi_14', 'N/A'):.1f}, RSI6={f.get('rsi_6', 'N/A'):.1f}, 超买共振={f.get('overbought_resonance', 'N/A')}")
        print(f"    量比5={f.get('vol_ratio_5', 'N/A'):.2f}, 量比20={f.get('vol_ratio_20', 'N/A'):.2f}, 涨跌量比={f.get('vol_up_dn_ratio', 'N/A'):.2f}")
        print(f"    换手率={f.get('turnover', 'N/A'):.2f}%, 换手比={f.get('turnover_ratio', 'N/A'):.2f}")
        print(f"    MACD柱增强天数={f.get('macd_hist_increase_days', 'N/A')}, MACD零轴={'上' if f.get('macd_above_zero') == 1 else '下'}")
        print(f"    ADX={f.get('adx', 'N/A'):.1f}, +DI={f.get('plus_di', 'N/A'):.1f}, -DI={f.get('minus_di', 'N/A'):.1f}")
        print(f"    均线排列分={f.get('ma_order_score', 'N/A')}, 20日位置={f.get('price_pos_20d', 'N/A'):.1f}%")
        print(f"    K线: 实体比={f.get('body_ratio', 'N/A'):.2f}, 上影={f.get('upper_shadow', 'N/A'):.2f}, 下影={f.get('lower_shadow', 'N/A'):.2f}")
        print(f"    连涨={f.get('consec_up', 'N/A')}日, 跳空={f.get('gap', 'N/A'):.2f}%")
        print(f"    VWMA5偏差={f.get('vwma5_diff', 'N/A'):.3f}, BB位置={f.get('bb_pos', 'N/A'):.1f}%")
        _fmt = lambda v: f"{v:.1f}" if v is not None else "N/A"
        print(f"    实际: 1d={_fmt(f.get('ret_1d'))}%, 3d={_fmt(f.get('ret_3d'))}%, 5d={_fmt(f.get('ret_5d'))}%, 10d={_fmt(f.get('ret_10d'))}%")
        print()

    print("── 跌票买入前关键指标 ──\n")
    for f in losers:
        print(f"  {f['code']} {f['name']} ({f['buy_date']})")
        print(f"    动量: 3d={f.get('pct_3d', 'N/A'):.1f}%, 5d={f.get('pct_5d', 'N/A'):.1f}%, 10d={f.get('pct_10d', 'N/A'):.1f}%")
        print(f"    偏离MA: MA5={f.get('dev_ma5', 'N/A'):.1f}%, MA10={f.get('dev_ma10', 'N/A'):.1f}%, MA20={f.get('dev_ma20', 'N/A'):.1f}%")
        print(f"    KDJ: K={f.get('kdj_k', 'N/A'):.1f}, J={f.get('kdj_j', 'N/A'):.1f}, 超买={f.get('kdj_overbought_strong', 'N/A')}")
        print(f"    RSI14={f.get('rsi_14', 'N/A'):.1f}, RSI6={f.get('rsi_6', 'N/A'):.1f}, 超买共振={f.get('overbought_resonance', 'N/A')}")
        print(f"    量比5={f.get('vol_ratio_5', 'N/A'):.2f}, 量比20={f.get('vol_ratio_20', 'N/A'):.2f}, 涨跌量比={f.get('vol_up_dn_ratio', 'N/A'):.2f}")
        print(f"    换手率={f.get('turnover', 'N/A'):.2f}%, 换手比={f.get('turnover_ratio', 'N/A'):.2f}")
        print(f"    MACD柱增强天数={f.get('macd_hist_increase_days', 'N/A')}, MACD零轴={'上' if f.get('macd_above_zero') == 1 else '下'}")
        print(f"    ADX={f.get('adx', 'N/A'):.1f}, +DI={f.get('plus_di', 'N/A'):.1f}, -DI={f.get('minus_di', 'N/A'):.1f}")
        print(f"    均线排列分={f.get('ma_order_score', 'N/A')}, 20日位置={f.get('price_pos_20d', 'N/A'):.1f}%")
        print(f"    K线: 实体比={f.get('body_ratio', 'N/A'):.2f}, 上影={f.get('upper_shadow', 'N/A'):.2f}, 下影={f.get('lower_shadow', 'N/A'):.2f}")
        print(f"    连涨={f.get('consec_up', 'N/A')}日, 跳空={f.get('gap', 'N/A'):.2f}%")
        print(f"    VWMA5偏差={f.get('vwma5_diff', 'N/A'):.3f}, BB位置={f.get('bb_pos', 'N/A'):.1f}%")
        print(f"    实际: 1d={_fmt(f.get('ret_1d'))}%, 3d={_fmt(f.get('ret_3d'))}%, 5d={_fmt(f.get('ret_5d'))}%, 10d={_fmt(f.get('ret_10d'))}%")
        print()

    # ─── 核心对比分析 ────────────────────────────────────────────
    print("\n" + "=" * 100)
    print("核心区分因子分析 (排除未来数据)")
    print("=" * 100)

    # Define pre-buy features only
    pre_buy_keys = [
        "pct_3d", "pct_5d", "pct_10d", "momentum_accel", "momentum_accel_5_10",
        "dev_ma5", "dev_ma10", "dev_ma20",
        "ma_order_score",
        "vol_ratio_5", "vol_ratio_20", "vol_up_dn_ratio", "vol_price_confirm_5d",
        "kdj_k", "kdj_j", "kdj_overbought_strong", "kdj_j_extreme",
        "rsi_14", "rsi_6", "overbought_resonance",
        "macd_hist_increase_days", "macd_hist_pct",
        "adx", "plus_di", "minus_di", "di_diff",
        "turnover", "turnover_ratio",
        "body_ratio", "upper_shadow", "lower_shadow",
        "price_pos_20d", "dist_from_high_20d",
        "consec_up", "gap",
        "vwma5_diff", "vwma10_diff", "vwma20_diff",
        "bb_pos", "bb_bw",
        "volatility_10d",
    ]

    analysis = []
    for key in pre_buy_keys:
        w_vals = [f[key] for f in winners if f.get(key) is not None and not (isinstance(f[key], float) and np.isnan(f[key]))]
        l_vals = [f[key] for f in losers if f.get(key) is not None and not (isinstance(f[key], float) and np.isnan(f[key]))]

        if len(w_vals) < 2 or len(l_vals) < 2:
            continue

        w_mean = np.mean(w_vals)
        l_mean = np.mean(l_vals)
        w_std = np.std(w_vals, ddof=1)
        l_std = np.std(l_vals, ddof=1)

        pooled_std = np.sqrt((w_std ** 2 + l_std ** 2) / 2)
        cohens_d = abs(w_mean - l_mean) / pooled_std if pooled_std > 0 else 0

        # Separation score
        sep = abs(w_mean - l_mean) / ((w_std + l_std) / 2) if (w_std + l_std) > 0 else 0

        analysis.append({
            "key": key,
            "w_mean": w_mean, "w_std": w_std,
            "l_mean": l_mean, "l_std": l_std,
            "diff": w_mean - l_mean,
            "cohens_d": cohens_d,
            "separation": sep,
            "w_vals": w_vals, "l_vals": l_vals,
        })

    analysis.sort(key=lambda x: x["separation"], reverse=True)

    print(f"\n{'指标':<30} {'涨票均值':>10} {'涨票σ':>8} {'跌票均值':>10} {'跌票σ':>8} {'差异':>10} {'区分度':>8} {'Cohen-d':>8} {'方向':>6}")
    print("-" * 110)

    for a in analysis:
        direction = "W>L" if a["diff"] > 0 else "L>W"
        print(f"{a['key']:<30} {a['w_mean']:>10.3f} {a['w_std']:>8.3f} {a['l_mean']:>10.3f} {a['l_std']:>8.3f} {a['diff']:>10.3f} {a['separation']:>8.3f} {a['cohens_d']:>8.3f} {direction:>6}")

    # ─── 复合打分模型 ────────────────────────────────────────────
    print("\n" + "=" * 100)
    print("复合过滤模型测试")
    print("=" * 100)

    # Based on the analysis, build a composite filter
    def composite_score(f):
        """Generate a composite score based on identified factors."""
        score = 0
        reasons = []

        # 1. KDJ超买: 跌票KDJ K值明显高于涨票 (83.5 vs 76.8)
        k = f.get("kdj_k", 80)
        if k is not None:
            if k < 75:
                score += 3
                reasons.append(f"KDJ-K适中({k:.0f})")
            elif k > 85:
                score -= 3
                reasons.append(f"KDJ-K过高({k:.0f})")
            elif k > 80:
                score -= 1
                reasons.append(f"KDJ-K偏高({k:.0f})")
            else:
                score += 1

        # 2. KDJ超买共振
        ob_res = f.get("overbought_resonance", 0)
        if ob_res == 2:
            score -= 3
            reasons.append("KDJ+RSI双超买")
        elif ob_res == 1:
            score -= 1
        else:
            score += 2
            reasons.append("无超买共振")

        # 3. 下影线比率: 跌票有明显更大的下影线 (0.33 vs 0.18)
        ls = f.get("lower_shadow", 0)
        if ls is not None:
            if ls > 0.4:
                score -= 2
                reasons.append(f"长下影线({ls:.2f})")
            elif ls < 0.15:
                score += 1
                reasons.append(f"短下影线({ls:.2f})")

        # 4. 上影线比率: 涨票有更大的上影线 (0.38 vs 0.24)
        us = f.get("upper_shadow", 0)
        if us is not None:
            if us > 0.3:
                score += 2
                reasons.append(f"上影线明显({us:.2f})")
            elif us < 0.15:
                score -= 1

        # 5. 均线排列: 涨票100%多头排列, 跌票仅64%
        mo = f.get("ma_order_score", 0)
        if mo is not None:
            if mo >= 2:
                score += 2
                reasons.append("均线多头")
            elif mo <= 0:
                score -= 3
                reasons.append("均线非多头")

        # 6. KDJ J值: 跌票J值明显更高 (101 vs 91)
        j = f.get("kdj_j", 100)
        if j is not None:
            if j > 110:
                score -= 3
                reasons.append(f"J值极高({j:.0f})")
            elif j > 100:
                score -= 1
            elif j < 85:
                score += 2
                reasons.append(f"J值适中({j:.0f})")

        # 7. MACD柱增强天数: 跌票反而更多天增强 (0.64 vs 0.20)
        # 这意味着: 连续多天MACD柱增强 → 可能已到后期
        mhid = f.get("macd_hist_increase_days", 0)
        if mhid is not None:
            if mhid >= 3:
                score -= 2
                reasons.append(f"MACD柱连续增强{mhid}天(可能见顶)")
            elif mhid == 0:
                score += 1
                reasons.append("MACD柱非连续增强")

        # 8. 换手率: 涨票平均更高 (8.5 vs 6.6)
        to = f.get("turnover", 0)
        if to is not None:
            if to > 8:
                score += 1
            elif to < 3:
                score -= 1

        # 9. ADX: 涨票ADX相对较低 (中位数33 vs 38)
        adx_v = f.get("adx", 30)
        if adx_v is not None:
            if adx_v > 45:
                score -= 2
                reasons.append(f"ADX极高({adx_v:.0f})趋势可能衰竭")
            elif adx_v < 25:
                score += 1

        # 10. 连涨天数: 避免连涨过多
        cu = f.get("consec_up", 0)
        if cu >= 4:
            score -= 2
            reasons.append(f"连涨{cu}天(过热)")
        elif cu == 0:
            score += 1
            reasons.append("非连涨状态")

        return score, reasons

    print("\n复合打分结果:\n")
    print(f"{'股票':<12} {'结果':>6} {'得分':>6} {'判断':>8} {'关键因素'}")
    print("-" * 90)

    scored = []
    for f in all_features:
        s, reasons = composite_score(f)
        f["comp_score"] = s
        f["reasons"] = reasons
        judge = "应该买" if s >= 2 else ("应该避" if s <= -2 else "中性")
        scored.append(f)

    scored.sort(key=lambda x: x["comp_score"], reverse=True)

    correct = 0
    total = len(scored)
    for f in scored:
        is_win = f["cat"] == "win"
        s = f["comp_score"]
        judge = "买入" if s >= 2 else ("回避" if s <= -2 else "中性")
        correct_judge = (s >= 2 and is_win) or (s <= -2 and not is_win)
        mark = "✓" if correct_judge else ("△" if s > -2 and s < 2 else "✗")
        correct += 1 if correct_judge else 0

        r_str = " | ".join(f["reasons"][:4])
        print(f"{f['code']} {f['name']:<6} {f['result']:>4} {s:>6} {judge:>8}{mark} {r_str}")

    print(f"\n区分准确率: {correct}/{total} = {correct/total*100:.1f}%")

    # ─── 卖出时机规律总结 ────────────────────────────────────────
    print("\n" + "=" * 100)
    print("卖出时机规律总结")
    print("=" * 100)

    sell_cases = [
        ("600773", "西藏城投", "2026-04-17", ["2026-04-29", "2026-04-30"]),
        ("603682", "锦和商管", "2026-04-17", ["2026-04-30", "2026-05-06"]),
        ("600527", "江南高纤", "2026-04-17", ["2026-05-07"]),
        ("605365", "立达信", "2026-04-21", ["2026-04-23", "2026-04-24"]),
        ("002463", "沪电股份", "2026-04-21", ["2026-04-23"]),
    ]

    print("\n理想卖出日的技术信号:\n")

    for code, name, buy_date, sell_dates in sell_cases:
        df = load_stock(code)
        if df is None:
            continue
        buy_idx = get_idx(df, buy_date)
        buy_open = df.iloc[buy_idx]["open"]

        print(f"  {code} {name}:")
        print(f"    买入价: {buy_open:.2f}")

        max_cum = 0
        max_cum_date = ""
        for sell_date in sell_dates:
            sell_idx = get_idx(df, sell_date)
            if sell_idx is None:
                continue

            # Show the day before sell date's signals
            if sell_idx > 0:
                pre_sell = df.iloc[sell_idx - 1]
                k = pre_sell.get("kdj_k", np.nan)
                j = pre_sell.get("kdj_j", np.nan)
                hist = pre_sell.get("macd_hist", np.nan)
                turnover = pre_sell.get("turnover", np.nan)

                # Volume ratio
                avg5 = df.iloc[max(0, sell_idx - 6):sell_idx - 1]["volume"].mean()
                vr = pre_sell["volume"] / avg5 if avg5 > 0 else 0

                sell_open = df.iloc[sell_idx]["open"]
                ret = (sell_open / buy_open - 1) * 100

                # Check KDJ turning
                if sell_idx >= 2:
                    prev_k = df.iloc[sell_idx - 2].get("kdj_k", np.nan)
                    k_turning = prev_k > k if not np.isnan(prev_k) and not np.isnan(k) else False
                else:
                    k_turning = False

                print(f"    卖出日{sell_date}: 收益={ret:.1f}% | 前日K={k:.1f}, J={j:.1f}, VR={vr:.2f}, HIST={hist:.4f}, TO={turnover:.2f}%")
                print(f"      KDJ转向: {'是-K从高点回落' if k_turning else '否'}")

            # Track max cumulative return
            for i in range(buy_idx, sell_idx + 1):
                cum = (df.iloc[i]["close"] / buy_open - 1) * 100
                if cum > max_cum:
                    max_cum = cum
                    max_cum_date = df.iloc[i]["date"].strftime("%Y-%m-%d")

            print(f"    期间最大收益: {max_cum:.1f}% (日期: {max_cum_date})")

        # Check all days after buy for peak signals
        print(f"    ── 每日信号追踪 ──")
        end_idx = min(buy_idx + 20, len(df))
        for i in range(buy_idx, end_idx):
            row = df.iloc[i]
            cum = (row["close"] / buy_open - 1) * 100
            k = row.get("kdj_k", np.nan)
            hist = row.get("macd_hist", np.nan)

            # KDJ turning signal
            kdj_turn = ""
            if i > buy_idx:
                prev_k = df.iloc[i - 1].get("kdj_k", np.nan)
                if not np.isnan(prev_k) and not np.isnan(k):
                    if prev_k > 85 and k < prev_k:
                        kdj_turn = " ← KDJ高位回落!"
                    elif prev_k > 80 and k < prev_k and k < 80:
                        kdj_turn = " ← KDJ从超买回落!"

            # Volume surge
            avg5 = df.iloc[max(buy_idx, i - 5):i]["volume"].mean()
            vr = row["volume"] / avg5 if avg5 > 0 else 0
            vol_sig = ""
            if vr > 1.5 and cum > 10:
                vol_sig = " ← 放量大涨(可能见顶)"

            date_str = row["date"].strftime("%Y-%m-%d")
            is_sell_day = date_str in sell_dates if sell_dates else False
            sell_mark = " ★SELL" if is_sell_day else ""

            print(f"      {date_str}: {cum:+6.1f}% K={k:.0f} HIST={hist:.4f} VR={vr:.2f}{kdj_turn}{vol_sig}{sell_mark}")

        print()

    # ─── 卖出信号总结 ────────────────────────────────────────────
    print("── 卖出信号规律 ──")
    print("""
  综合分析以上卖出时机，理想卖出的技术特征为:

  1. KDJ信号:
     - K值从高位(>85)开始回落是第一卖出信号
     - K值跌穿D值(K<D)是确认卖出信号
     - 卖出信号通常在K值见顶后的1-2天内出现

  2. 成交量信号:
     - 在大涨日(>5%)出现放量(量比>1.5)是短期见顶信号
     - 但缩量上涨可以继续持有

  3. MACD信号:
     - MACD柱从高点开始缩小是弱势信号
     - 但不一定是立即卖出信号，要结合KDJ

  4. 综合卖出策略:
     - 优先信号: KDJ K值>85后开始回落 + 当日放量
     - 次要信号: 连续3天上涨后出现阴线 + 量缩
     - 安全策略: 分批卖出，第一信号出50%，第二信号出剩余
""")


if __name__ == "__main__":
    main()
