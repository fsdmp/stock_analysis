"""
完全客观数据驱动的多维分析 —— 不预设任何结论
目标: 从尽可能多的维度提取特征, 让数据自己告诉我们涨跌票的真正差异在哪里
"""

import os
import numpy as np
import pandas as pd
from itertools import combinations

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "stocks")

TRADES = {
    "2026-04-17": {
        "600773": {"name": "西藏城投", "result": "大涨", "cat": "win"},
        "603682": {"name": "锦和商管", "result": "涨", "cat": "win"},
        "002733": {"name": "雄韬股份", "result": "跌", "cat": "lose"},
        "002074": {"name": "国轩高科", "result": "大跌", "cat": "lose"},
        "603738": {"name": "泰晶科技", "result": "涨", "cat": "win"},
        "600654": {"name": "中安科", "result": "大跌", "cat": "lose"},
        "600527": {"name": "江南高纤", "result": "涨", "cat": "win"},
    },
    "2026-04-20": {
        "600855": {"name": "航天长峰", "result": "大跌", "cat": "lose"},
        "603109": {"name": "神驰机电", "result": "大跌", "cat": "lose"},
        "002943": {"name": "宇晶股份", "result": "涨", "cat": "win"},
        "002515": {"name": "金字火腿", "result": "涨", "cat": "win"},
        "002436": {"name": "兴森科技", "result": "涨", "cat": "win"},
        "000612": {"name": "焦作万方", "result": "跌", "cat": "lose"},
        "000791": {"name": "甘肃能源", "result": "跌", "cat": "lose"},
    },
    "2026-04-21": {
        "603815": {"name": "交建股份", "result": "涨", "cat": "win"},
        "605365": {"name": "立达信", "result": "涨", "cat": "win"},
        "605366": {"name": "宏柏新材", "result": "跌", "cat": "lose"},
        "605098": {"name": "行动教育", "result": "大跌", "cat": "lose"},
        "603150": {"name": "万朗磁塑", "result": "跌", "cat": "lose"},
        "600135": {"name": "乐凯胶片", "result": "跌", "cat": "lose"},
        "002463": {"name": "沪电股份", "result": "小涨", "cat": "win"},
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


def safe(v):
    """Return None if NaN or invalid."""
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    return v


def extract_all_features(df, buy_date_str):
    """
    暴力提取尽可能多的特征，不做主观筛选。
    包括：价格、量、均线、MACD、KDJ、布林、RSI、ADX、形态、节奏、
    比率、斜率、差值、位置、波动、换手、OBV、VWMA等所有能想到的维度。
    """
    buy_idx = get_idx(df, buy_date_str)
    if buy_idx is None or buy_idx < 30:
        return None

    pre = buy_idx - 1
    row = df.iloc[pre]
    f = {}

    c = row["close"]
    o = row["open"]
    h = row["high"]
    lo = row["low"]
    vol = row["volume"]
    pct = safe(row.get("pct_change"))
    turnover = safe(row.get("turnover"))

    # ═══════════════════════════════════════════════════════════
    # A. 基础价格形态
    # ═══════════════════════════════════════════════════════════
    tr = h - lo
    body = abs(c - o)
    us = h - max(c, o)
    ls = min(c, o) - lo

    f["candle_body_ratio"] = body / tr if tr > 0 else 0
    f["upper_shadow_ratio"] = us / tr if tr > 0 else 0
    f["lower_shadow_ratio"] = ls / tr if tr > 0 else 0
    f["is_yang"] = 1 if c > o else 0
    f["pct_change"] = pct if pct else 0

    # ═══════════════════════════════════════════════════════════
    # B. 动量 (多时间窗口)
    # ═══════════════════════════════════════════════════════════
    for n in [1, 2, 3, 4, 5, 7, 10, 15, 20]:
        if pre >= n:
            prev_c = df.iloc[pre - n]["close"]
            f[f"cum_{n}d"] = (c / prev_c - 1) * 100
        else:
            f[f"cum_{n}d"] = None

    # 动量加速度: 短期动量 vs 中期动量
    for a, b in [(3, 5), (3, 10), (5, 10), (5, 20), (3, 7)]:
        va = f.get(f"cum_{a}d")
        vb = f.get(f"cum_{b}d")
        if va is not None and vb is not None and abs(vb) > 0.1:
            f[f"accel_{a}_{b}"] = va / abs(vb)
        else:
            f[f"accel_{a}_{b}"] = None

    # ═══════════════════════════════════════════════════════════
    # C. 均线系统
    # ═══════════════════════════════════════════════════════════
    for ma in ["ma5", "ma7", "ma10", "ma20"]:
        v = safe(row.get(ma))
        if v and v > 0:
            f[f"close_vs_{ma}"] = (c - v) / v * 100
        else:
            f[f"close_vs_{ma}"] = None

    # MA斜率(多窗口)
    for ma_name in ["ma5", "ma10", "ma20"]:
        for slope_n in [3, 5]:
            v_now = safe(row.get(ma_name))
            if pre >= slope_n:
                v_prev = safe(df.iloc[pre - slope_n].get(ma_name))
            else:
                v_prev = None
            if v_now and v_prev and v_prev > 0:
                f[f"{ma_name}_slope_{slope_n}d"] = (v_now - v_prev) / v_prev * 100
            else:
                f[f"{ma_name}_slope_{slope_n}d"] = None

    # 均线间距
    ma_vals = {}
    for ma in ["ma5", "ma7", "ma10", "ma20"]:
        v = safe(row.get(ma))
        if v:
            ma_vals[ma] = v
    # 所有MA两两差距
    for m1, m2 in [("ma5", "ma10"), ("ma5", "ma20"), ("ma10", "ma20"), ("ma7", "ma10")]:
        if m1 in ma_vals and m2 in ma_vals and ma_vals[m2] > 0:
            f[f"{m1}_minus_{m2}_pct"] = (ma_vals[m1] - ma_vals[m2]) / ma_vals[m2] * 100

    # 均线多头排列得分
    if len(ma_vals) == 4:
        ma_list = [ma_vals["ma5"], ma_vals["ma7"], ma_vals["ma10"], ma_vals["ma20"]]
        f["ma_bull_score"] = sum(1 if ma_list[i] > ma_list[i+1] else (-1 if ma_list[i] < ma_list[i+1] else 0) for i in range(3))
    else:
        f["ma_bull_score"] = None

    # ═══════════════════════════════════════════════════════════
    # D. 成交量维度
    # ═══════════════════════════════════════════════════════════
    avg5 = df.iloc[max(0, pre-4):pre+1]["volume"].mean()
    avg10 = df.iloc[max(0, pre-9):pre+1]["volume"].mean()
    avg20 = df.iloc[max(0, pre-19):pre+1]["volume"].mean()

    f["vol_ratio_1_5"] = vol / avg5 if avg5 > 0 else None
    f["vol_ratio_1_10"] = vol / avg10 if avg10 > 0 else None
    f["vol_ratio_1_20"] = vol / avg20 if avg20 > 0 else None
    f["vol_ratio_5_10"] = avg5 / avg10 if avg10 > 0 else None

    # 涨日vs跌日量
    recent = df.iloc[max(0, pre-9):pre+1]
    up_v = recent[recent["pct_change"] > 0]["volume"]
    dn_v = recent[recent["pct_change"] < 0]["volume"]
    f["vol_up_vs_dn_10d"] = up_v.mean() / dn_v.mean() if len(dn_v) > 0 and dn_v.mean() > 0 else None

    recent5 = df.iloc[max(0, pre-4):pre+1]
    up5 = recent5[recent5["pct_change"] > 0]["volume"]
    dn5 = recent5[recent5["pct_change"] < 0]["volume"]
    f["vol_up_vs_dn_5d"] = up5.mean() / dn5.mean() if len(dn5) > 0 and dn5.mean() > 0 else None

    # ═══════════════════════════════════════════════════════════
    # E. 换手率
    # ═══════════════════════════════════════════════════════════
    f["turnover"] = turnover
    if turnover and pre >= 5:
        avg_to = df.iloc[pre-4:pre+1]["turnover"].mean()
        f["turnover_ratio_1_5"] = turnover / avg_to if avg_to > 0 else None
    else:
        f["turnover_ratio_1_5"] = None

    # ═══════════════════════════════════════════════════════════
    # F. MACD
    # ═══════════════════════════════════════════════════════════
    dif = safe(row.get("macd_dif"))
    dea = safe(row.get("macd_dea"))
    hist = safe(row.get("macd_hist"))

    f["macd_dif"] = dif
    f["macd_dea"] = dea
    f["macd_hist"] = hist
    f["macd_dif_vs_dea"] = dif - dea if dif is not None and dea is not None else None
    f["macd_above_zero"] = 1 if dif is not None and dif > 0 else (-1 if dif is not None else None)

    # MACD柱变化趋势
    if hist is not None and pre >= 3:
        h_prev = safe(df.iloc[pre-1].get("macd_hist"))
        h_prev2 = safe(df.iloc[pre-2].get("macd_hist"))
        h_prev3 = safe(df.iloc[pre-3].get("macd_hist"))
        if h_prev is not None:
            f["macd_hist_delta_1d"] = hist - h_prev
        if h_prev2 is not None:
            f["macd_hist_delta_2d"] = hist - h_prev2
        # 连续增强天数
        inc_days = 0
        for i in range(pre, max(pre-6, -1), -1):
            hi = safe(df.iloc[i].get("macd_hist"))
            hi_prev = safe(df.iloc[i-1].get("macd_hist")) if i > 0 else None
            if hi is not None and hi_prev is not None and hi > hi_prev:
                inc_days += 1
            else:
                break
        f["macd_hist_inc_days"] = inc_days
    else:
        f["macd_hist_delta_1d"] = None
        f["macd_hist_delta_2d"] = None
        f["macd_hist_inc_days"] = None

    # DIF斜率
    if dif is not None and pre >= 3:
        dif_prev = safe(df.iloc[pre-3].get("macd_dif"))
        f["dif_change_3d"] = dif - dif_prev if dif_prev is not None else None
    else:
        f["dif_change_3d"] = None

    # ═══════════════════════════════════════════════════════════
    # G. KDJ
    # ═══════════════════════════════════════════════════════════
    k = safe(row.get("kdj_k"))
    d = safe(row.get("kdj_d"))
    j = safe(row.get("kdj_j"))

    f["kdj_k"] = k
    f["kdj_d"] = d
    f["kdj_j"] = j
    f["kdj_k_minus_d"] = k - d if k is not None and d is not None else None
    f["kdj_j_minus_k"] = j - k if j is not None and k is not None else None

    # KDJ变化速率
    if k is not None and pre >= 3:
        k_prev = safe(df.iloc[pre-3].get("kdj_k"))
        f["kdj_k_change_3d"] = k - k_prev if k_prev is not None else None
    else:
        f["kdj_k_change_3d"] = None

    # ═══════════════════════════════════════════════════════════
    # H. 布林带
    # ═══════════════════════════════════════════════════════════
    bbu = safe(row.get("bb_upper"))
    bbl = safe(row.get("bb_lower"))
    bbm = safe(row.get("bb_middle"))
    bbw = safe(row.get("bb_bandwidth"))

    if bbu is not None and bbl is not None and bbu > bbl:
        f["bb_pos"] = (c - bbl) / (bbu - bbl) * 100
        f["bb_range_pct"] = (bbu - bbl) / bbm * 100 if bbm and bbm > 0 else None
    else:
        f["bb_pos"] = None
        f["bb_range_pct"] = None
    f["bb_bandwidth"] = bbw

    # ═══════════════════════════════════════════════════════════
    # I. RSI
    # ═══════════════════════════════════════════════════════════
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss_s = (-delta).where(delta < 0, 0.0)
    for period in [6, 9, 14, 21]:
        ag = gain.rolling(window=period, min_periods=period).mean()
        al = loss_s.rolling(window=period, min_periods=period).mean()
        rs = ag / al.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        f[f"rsi_{period}"] = safe(rsi.iloc[pre]) if pre < len(rsi) else None

    # ═══════════════════════════════════════════════════════════
    # J. ADX / DI
    # ═══════════════════════════════════════════════════════════
    pdm = df["high"].diff()
    mdm = -df["low"].diff()
    pdm = pdm.where((pdm > mdm) & (pdm > 0), 0.0)
    mdm = mdm.where((mdm > pdm) & (mdm > 0), 0.0)
    tr1 = df["high"] - df["low"]
    tr2 = abs(df["high"] - df["close"].shift(1))
    tr3 = abs(df["low"] - df["close"].shift(1))
    atr_raw = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr14 = atr_raw.rolling(14).mean()
    pdi = 100 * (pdm.rolling(14).mean() / atr14)
    mdi = 100 * (mdm.rolling(14).mean() / atr14)
    dx = 100 * abs(pdi - mdi) / (pdi + mdi).replace(0, np.nan)
    adx = dx.rolling(14).mean()

    f["atr_14"] = safe(atr14.iloc[pre]) if pre < len(atr14) else None
    f["atr_pct"] = (f["atr_14"] / c * 100) if f["atr_14"] and c > 0 else None
    f["adx"] = safe(adx.iloc[pre]) if pre < len(adx) else None
    f["plus_di"] = safe(pdi.iloc[pre]) if pre < len(pdi) else None
    f["minus_di"] = safe(mdi.iloc[pre]) if pre < len(mdi) else None
    f["di_diff"] = (f["plus_di"] - f["minus_di"]) if f["plus_di"] is not None and f["minus_di"] is not None else None

    # ═══════════════════════════════════════════════════════════
    # K. OBV
    # ═══════════════════════════════════════════════════════════
    obv = df["obv"]
    obv_ma5 = obv.rolling(5).mean()
    obv_ma10 = obv.rolling(10).mean()
    obv_ma20 = obv.rolling(20).mean()
    f["obv_vs_ma5"] = safe((obv.iloc[pre] / obv_ma5.iloc[pre] - 1) * 100) if pre < len(obv_ma5) and safe(obv_ma5.iloc[pre]) else None
    f["obv_vs_ma20"] = safe((obv.iloc[pre] / obv_ma20.iloc[pre] - 1) * 100) if pre < len(obv_ma20) and safe(obv_ma20.iloc[pre]) else None
    f["obv_ma5_slope_5d"] = safe(obv_ma5.iloc[pre] - obv_ma5.iloc[pre-5]) if pre >= 5 and safe(obv_ma5.iloc[pre]) and safe(obv_ma5.iloc[pre-5]) else None

    # ═══════════════════════════════════════════════════════════
    # L. VWMA vs MA
    # ═══════════════════════════════════════════════════════════
    for p in [5, 10, 20]:
        ma_v = safe(row.get(f"ma{p}"))
        vwma_v = safe(row.get(f"vwma{p}"))
        if ma_v and vwma_v and ma_v > 0:
            f[f"vwma{p}_minus_ma{p}_pct"] = (vwma_v - ma_v) / ma_v * 100
        else:
            f[f"vwma{p}_minus_ma{p}_pct"] = None

    # ═══════════════════════════════════════════════════════════
    # M. 价格位置 / 支撑压力
    # ═══════════════════════════════════════════════════════════
    for n in [5, 10, 20]:
        if pre >= n:
            window = df.iloc[pre-n+1:pre+1]
            hi_n = window["high"].max()
            lo_n = window["low"].min()
            f[f"price_pos_{n}d"] = (c - lo_n) / (hi_n - lo_n) * 100 if hi_n > lo_n else 50
            f[f"dist_high_{n}d_pct"] = (c - hi_n) / hi_n * 100
            f[f"dist_low_{n}d_pct"] = (c - lo_n) / lo_n * 100
            f[f"range_{n}d_pct"] = (hi_n - lo_n) / lo_n * 100
        else:
            for suffix in [f"price_pos_{n}d", f"dist_high_{n}d_pct", f"dist_low_{n}d_pct", f"range_{n}d_pct"]:
                f[suffix] = None

    # ═══════════════════════════════════════════════════════════
    # N. 波动率
    # ═══════════════════════════════════════════════════════════
    for n in [5, 10, 20]:
        if pre >= n:
            rets = df.iloc[pre-n+1:pre+1]["pct_change"].dropna()
            f[f"volatility_{n}d"] = rets.std() if len(rets) >= 3 else None
        else:
            f[f"volatility_{n}d"] = None

    # ═══════════════════════════════════════════════════════════
    # O. 连涨连跌
    # ═══════════════════════════════════════════════════════════
    consec_up = 0
    for i in range(pre, max(pre-10, -1), -1):
        p = safe(df.iloc[i].get("pct_change"))
        if p and p > 0:
            consec_up += 1
        else:
            break
    f["consec_up"] = consec_up

    consec_dn = 0
    for i in range(pre, max(pre-10, -1), -1):
        p = safe(df.iloc[i].get("pct_change"))
        if p and p < 0:
            consec_dn += 1
        else:
            break
    f["consec_dn"] = consec_dn

    # ═══════════════════════════════════════════════════════════
    # P. 跳空
    # ═══════════════════════════════════════════════════════════
    if pre >= 1:
        f["gap_pct"] = (o - df.iloc[pre-1]["close"]) / df.iloc[pre-1]["close"] * 100
    else:
        f["gap_pct"] = None

    # ═══════════════════════════════════════════════════════════
    # Q. 节奏特征 (前N天)
    # ═══════════════════════════════════════════════════════════
    for n in [5, 10]:
        if pre >= n:
            window = df.iloc[pre-n+1:pre+1]
            pcts = window["pct_change"].dropna().tolist()
            up_count = sum(1 for p in pcts if p > 0)
            dn_count = sum(1 for p in pcts if p < 0)
            f[f"up_ratio_{n}d"] = up_count / len(pcts) if pcts else None

            # 前半后半对比
            half = n // 2
            first = pcts[:half]
            second = pcts[half:]
            f[f"avg_pct_first_half_{n}d"] = np.mean(first) if first else None
            f[f"avg_pct_second_half_{n}d"] = np.mean(second) if second else None
            if first and second:
                f[f"momentum_shift_{n}d"] = np.mean(second) - np.mean(first)
            else:
                f[f"momentum_shift_{n}d"] = None
        else:
            f[f"up_ratio_{n}d"] = None
            f[f"avg_pct_first_half_{n}d"] = None
            f[f"avg_pct_second_half_{n}d"] = None
            f[f"momentum_shift_{n}d"] = None

    # ═══════════════════════════════════════════════════════════
    # R. 量价背离检测
    # ═══════════════════════════════════════════════════════════
    for n in [5, 10]:
        if pre >= n:
            w = df.iloc[pre-n+1:pre+1]
            price_chg = w["close"].iloc[-1] - w["close"].iloc[0]
            vol_total = w["volume"].sum()
            avg_vol_2n = df.iloc[max(0, pre-2*n+1):pre-n+1]["volume"].mean() if pre >= 2*n else vol_total / n
            if price_chg > 0 and vol_total < avg_vol_2n * n * 0.8:
                f[f"bearish_divergence_{n}d"] = 1
            elif price_chg < 0 and vol_total > avg_vol_2n * n * 1.2:
                f[f"bullish_divergence_{n}d"] = 1
            else:
                f[f"bearish_divergence_{n}d"] = 0
                f[f"bullish_divergence_{n}d"] = 0

    # ═══════════════════════════════════════════════════════════
    # S. 涨停相关
    # ═══════════════════════════════════════════════════════════
    lu_count = 0
    for i in range(pre, max(pre-5, -1), -1):
        p = safe(df.iloc[i].get("pct_change"))
        if p and p >= 9.5:
            lu_count += 1
        else:
            break
    f["limit_up_consec"] = lu_count

    # 10天内涨停天数
    if pre >= 10:
        lu_10d = sum(1 for i in range(pre-9, pre+1) if safe(df.iloc[i].get("pct_change")) and df.iloc[i].get("pct_change", 0) >= 9.5)
        f["limit_up_count_10d"] = lu_10d
    else:
        f["limit_up_count_10d"] = None

    # ═══════════════════════════════════════════════════════════
    # T. 额外交叉指标
    # ═══════════════════════════════════════════════════════════
    # RSI与KDJ超买共振
    r14 = f.get("rsi_14")
    f["rsi_k_overbought"] = (1 if (k is not None and k > 80) else 0) + (1 if (r14 is not None and r14 > 70) else 0)

    # 收盘价相对MA5的Z-score
    if pre >= 10:
        closes_10 = df.iloc[pre-9:pre+1]["close"]
        ma10_val = closes_10.mean()
        std10_val = closes_10.std()
        f["zscore_close_10d"] = (c - ma10_val) / std10_val if std10_val > 0 else 0
    else:
        f["zscore_close_10d"] = None

    # 实际收益 (for verification)
    buy_open = df.iloc[buy_idx]["open"]
    f["buy_open"] = buy_open
    for nd in [1, 2, 3, 5, 10]:
        if buy_idx + nd < len(df):
            f[f"ret_{nd}d"] = (df.iloc[buy_idx + nd]["close"] / buy_open - 1) * 100
        else:
            f[f"ret_{nd}d"] = None

    return f


def main():
    print("=" * 120)
    print("完全数据驱动的多维特征分析")
    print("=" * 120)

    all_features = []
    for buy_date, stocks in TRADES.items():
        for code, info in stocks.items():
            df = load_stock(code)
            if df is None:
                continue
            f = extract_all_features(df, buy_date)
            if f is None:
                continue
            f["code"] = code
            f["name"] = info["name"]
            f["result"] = info["result"]
            f["cat"] = info["cat"]
            f["buy_date"] = buy_date
            all_features.append(f)

    winners = [f for f in all_features if f["cat"] == "win"]
    losers = [f for f in all_features if f["cat"] == "lose"]
    print(f"\n涨票: {len(winners)}, 跌票: {len(losers)}, 总计: {len(all_features)}")

    # ─── 排除所有未来数据列 ──────────────────────────────────────
    exclude = {"code", "name", "result", "cat", "buy_date", "buy_open",
               "ret_1d", "ret_2d", "ret_3d", "ret_5d", "ret_10d"}

    # ─── 第一步: 逐指标统计对比 ──────────────────────────────────
    print("\n" + "=" * 120)
    print("第一步: 全指标统计对比 (按区分度排序)")
    print("=" * 120)

    results = []
    for key in sorted(set(k for f in all_features for k in f.keys()) - exclude):
        w = [f[key] for f in winners if f.get(key) is not None and isinstance(f[key], (int, float, np.integer, np.floating))]
        l = [f[key] for f in losers if f.get(key) is not None and isinstance(f[key], (int, float, np.integer, np.floating))]
        w = [v for v in w if not np.isnan(v)]
        l = [v for v in l if not np.isnan(v)]

        if len(w) < 2 or len(l) < 2:
            continue

        wm, lm = np.mean(w), np.mean(l)
        ws, ls_ = np.std(w, ddof=1), np.std(l, ddof=1)

        # 区分度: |均值差| / 平均标准差
        avg_std = (ws + ls_) / 2
        separation = abs(wm - lm) / avg_std if avg_std > 0 else 0

        # 单指标分类能力: 能否找到一个阈值把涨跌分开
        # 尝试所有可能的阈值，找最优切分
        all_vals = sorted([(v, "W") for v in w] + [(v, "L") for v in l])
        best_acc = 0
        best_thresh = None
        best_dir = None

        for i in range(len(all_vals) - 1):
            thresh = (all_vals[i][0] + all_vals[i+1][0]) / 2
            # 方向1: >= thresh → W
            above_w = sum(1 for v, t in all_vals if v >= thresh and t == "W")
            above_l = sum(1 for v, t in all_vals if v >= thresh and t == "L")
            below_w = sum(1 for v, t in all_vals if v < thresh and t == "W")
            below_l = sum(1 for v, t in all_vals if v < thresh and t == "L")
            acc1 = (above_w + below_l) / len(all_vals)
            acc2 = (below_w + above_l) / len(all_vals)
            if acc1 > best_acc:
                best_acc = acc1
                best_thresh = thresh
                best_dir = "high→W" if wm > lm else "high→L"
            if acc2 > best_acc:
                best_acc = acc2
                best_thresh = thresh
                best_dir = "high→L" if wm > lm else "high→W"

        results.append({
            "key": key,
            "w_mean": wm, "w_std": ws,
            "l_mean": lm, "l_std": ls_,
            "diff": wm - lm,
            "separation": separation,
            "best_acc": best_acc,
            "best_thresh": best_thresh,
            "best_dir": best_dir,
            "w_vals": w, "l_vals": l,
        })

    results.sort(key=lambda x: x["separation"], reverse=True)

    # 打印所有有区分度的指标
    print(f"\n{'指标':<35} {'涨票均值':>10} {'跌票均值':>10} {'差异':>10} {'区分度':>8} {'最优阈值':>10} {'阈值方向':>10} {'单因子准确率':>10}")
    print("-" * 120)
    for r in results:
        if r["separation"] < 0.1:  # 只看有一定区分度的
            continue
        print(f"{r['key']:<35} {r['w_mean']:>10.3f} {r['l_mean']:>10.3f} {r['diff']:>10.3f} {r['separation']:>8.3f} {r['best_thresh']:>10.3f} {r['best_dir']:>10} {r['best_acc']*100:>9.1f}%")

    # ─── 第二步: 组合因子搜索 ──────────────────────────────────────
    print("\n" + "=" * 120)
    print("第二步: 双因子组合搜索 (寻找互补的因子对)")
    print("=" * 120)

    # 取区分度 top 25 的因子
    top_features = [r for r in results if r["separation"] >= 0.25][:25]

    combo_results = []
    for r1, r2 in combinations(top_features, 2):
        k1, k2 = r1["key"], r2["key"]
        t1, d1 = r1["best_thresh"], r1["best_dir"]
        t2, d2 = r2["best_thresh"], r2["best_dir"]

        # 用两个阈值组合判断
        correct = 0
        total = 0
        for f in all_features:
            v1, v2 = f.get(k1), f.get(k2)
            if v1 is None or v2 is None:
                continue
            total += 1

            # 根据方向判断
            s1 = 1 if (d1 == "high→W" and v1 >= t1) or (d1 == "high→L" and v1 < t1) else -1
            s2 = 1 if (d2 == "high→W" and v2 >= t2) or (d2 == "high→L" and v2 < t2) else -1

            # 组合: 两者一致才算
            if s1 + s2 >= 1:  # 至少一个强看涨 或 两个都看涨
                pred = "win"
            elif s1 + s2 <= -1:
                pred = "lose"
            else:
                pred = "neutral"

            if pred != "neutral":
                actual = f["cat"]
                if pred == actual:
                    correct += 1

        decided = total - sum(1 for f in all_features
                              if f.get(k1) is not None and f.get(k2) is not None
                              and (
                                  (1 if (d1 == "high→W" and f[k1] >= t1) or (d1 == "high→L" and f[k1] < t1) else -1) +
                                  (1 if (d2 == "high→W" and f[k2] >= t2) or (d2 == "high→L" and f[k2] < t2) else -1)
                              ) in (-1, 1))
        coverage = (total - (total - correct + (total - correct))) / total if total > 0 else 0  # decided ratio

        if correct >= 1:
            n_decided = sum(1 for f in all_features
                           if f.get(k1) is not None and f.get(k2) is not None
                           and (
                               (1 if (d1 == "high→W" and f[k1] >= t1) or (d1 == "high→L" and f[k1] < t1) else -1) +
                               (1 if (d2 == "high→W" and f[k2] >= t2) or (d2 == "high→L" and f[k2] < t2) else -1)
                           ) not in (-1, 1))
            actual_decided = total - n_decided
            acc = correct / actual_decided if actual_decided > 0 else 0
            combo_results.append({
                "k1": k1, "k2": k2,
                "correct": correct,
                "decided": actual_decided,
                "total": total,
                "accuracy": acc,
                "t1": t1, "t2": t2,
                "d1": d1, "d2": d2,
            })

    combo_results.sort(key=lambda x: (-x["accuracy"], -x["decided"]))

    print(f"\n找到了 {len(combo_results)} 个双因子组合，按准确率排序 (只显示准确率>60%的组合):")
    print(f"\n{'因子1':<28} {'因子2':<28} {'阈值1':>8} {'阈值2':>8} {'正确':>5} {'判定数':>5} {'总数':>5} {'准确率':>8}")
    print("-" * 130)
    shown = 0
    for cr in combo_results:
        if cr["accuracy"] < 0.6 or cr["decided"] < 5:
            continue
        print(f"{cr['k1']:<28} {cr['k2']:<28} {cr['t1']:>8.3f} {cr['t2']:>8.3f} {cr['correct']:>5} {cr['decided']:>5} {cr['total']:>5} {cr['accuracy']*100:>7.1f}%")
        shown += 1
        if shown >= 50:
            break

    # ─── 第三步: 三因子组合搜索 ────────────────────────────────────
    print("\n" + "=" * 120)
    print("第三步: 三因子组合搜索")
    print("=" * 120)

    top15 = [r for r in results if r["separation"] >= 0.3][:15]
    triple_results = []

    for r1, r2, r3 in combinations(top15, 3):
        keys = [r["key"] for r in [r1, r2, r3]]
        thresholds = [r["best_thresh"] for r in [r1, r2, r3]]
        directions = [r["best_dir"] for r in [r1, r2, r3]]

        correct = 0
        decided = 0
        total = 0

        for f in all_features:
            vals = [f.get(k) for k in keys]
            if any(v is None for v in vals):
                continue
            total += 1

            scores = []
            for v, t, d in zip(vals, thresholds, directions):
                s = 1 if (d == "high→W" and v >= t) or (d == "high→L" and v < t) else -1
                scores.append(s)

            vote = sum(scores)
            if vote >= 2:
                pred = "win"
                decided += 1
            elif vote <= -2:
                pred = "lose"
                decided += 1
            else:
                continue

            if pred == f["cat"]:
                correct += 1

        if decided >= 5:
            acc = correct / decided
            triple_results.append({
                "keys": keys,
                "thresholds": thresholds,
                "directions": directions,
                "correct": correct,
                "decided": decided,
                "accuracy": acc,
            })

    triple_results.sort(key=lambda x: (-x["accuracy"], -x["decided"]))

    print(f"\n找到 {len(triple_results)} 个三因子组合，按准确率排序:")
    print(f"\n{'因子组合':<80} {'正确':>5} {'判定数':>5} {'准确率':>8}")
    print("-" * 110)
    shown = 0
    for tr_item in triple_results:
        if tr_item["accuracy"] < 0.6:
            continue
        keys_str = " + ".join(tr_item["keys"])
        print(f"{keys_str:<80} {tr_item['correct']:>5} {tr_item['decided']:>5} {tr_item['accuracy']*100:>7.1f}%")
        # Print thresholds
        for k, t, d in zip(tr_item["keys"], tr_item["thresholds"], tr_item["directions"]):
            print(f"    {k}: {d} 阈值={t:.3f}")
        shown += 1
        if shown >= 30:
            break

    # ─── 第四步: 最佳组合的详细分析 ──────────────────────────────
    print("\n" + "=" * 120)
    print("第四步: 最佳组合逐票验证")
    print("=" * 120)

    # 取准确率最高的几个三因子组合
    best_triples = [t for t in triple_results if t["accuracy"] >= 0.75 and t["decided"] >= 8][:3]

    if not best_triples:
        best_triples = triple_results[:3]

    for idx, bt in enumerate(best_triples):
        print(f"\n── 最佳组合 #{idx+1}: {' + '.join(bt['keys'])} (准确率: {bt['accuracy']*100:.1f}%) ──")
        print(f"  判定数: {bt['decided']}/{len(all_features)}")

        for k, t, d in zip(bt["keys"], bt["thresholds"], bt["directions"]):
            print(f"    规则: {k} → {d} (阈值: {t:.3f})")

        print(f"\n  {'股票':<14} {'结果':>4}", end="")
        for k in bt["keys"]:
            print(f" {k:>15}", end="")
        print(f" {'投票':>5} {'判断':>6} {'OK':>3}")
        print("  " + "-" * 100)

        for f in all_features:
            vals = [f.get(k) for k in bt["keys"]]
            if any(v is None for v in vals):
                continue

            scores = []
            for v, t, d in zip(vals, bt["thresholds"], bt["directions"]):
                s = 1 if (d == "high→W" and v >= t) or (d == "high→L" and v < t) else -1
                scores.append(s)

            vote = sum(scores)
            if vote >= 2:
                pred = "win"
            elif vote <= -2:
                pred = "lose"
            else:
                pred = "neutral"

            is_win = f["cat"] == "win"
            ok = (pred == "win" and is_win) or (pred == "lose" and not is_win)
            mark = "✓" if ok else ("△" if pred == "neutral" else "✗")

            print(f"  {f['code']} {f['name']:<6} {f['result']:>4}", end="")
            for v in vals:
                if isinstance(v, float):
                    print(f" {v:>15.3f}", end="")
                else:
                    print(f" {str(v):>15}", end="")
            print(f" {vote:>+5} {pred:>6} {mark:>3}")

    # ─── 第五步: 逐票打印所有关键特征值 ──────────────────────────────
    print("\n" + "=" * 120)
    print("第五步: 逐票原始数据对照表 (按涨跌分组)")
    print("=" * 120)

    # 选取top 20最有区分度的特征
    top_keys = [r["key"] for r in results[:30]]

    for label, group in [("涨票", winners), ("跌票", losers)]:
        print(f"\n── {label} ──")
        header = f"{'股票':<12}"
        for k in top_keys[:20]:
            header += f" {k[:12]:>13}"
        print(header)
        print("-" * len(header))

        for f in group:
            line = f"{f['code']} {f['name']:<5}"
            for k in top_keys[:20]:
                v = f.get(k)
                if v is None:
                    line += f" {'N/A':>13}"
                elif isinstance(v, float):
                    line += f" {v:>13.3f}"
                else:
                    line += f" {str(v):>13}"
            print(line)


if __name__ == "__main__":
    main()
