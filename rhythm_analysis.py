"""
Rhythm analysis: Analyze the pre-buy price action patterns (5-10 days before buy)
to determine if the stock is at a good entry point rhythmically.
"""

import os
import numpy as np
import pandas as pd

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


def analyze_rhythm(df, buy_date_str, lookback=10):
    """Analyze price action rhythm before buy date."""
    buy_idx = get_idx(df, buy_date_str)
    if buy_idx is None or buy_idx < lookback + 5:
        return None

    pre = buy_idx - 1  # last closed day before buy

    result = {}

    # ─── 1. 每日涨跌节奏 (前N天) ────────────────────────────────
    daily_pcts = []
    for i in range(pre - lookback + 1, pre + 1):
        daily_pcts.append(df.iloc[i]["pct_change"])

    result["daily_pcts"] = daily_pcts

    # 涨跌天数
    up_days = sum(1 for p in daily_pcts if p > 0)
    dn_days = sum(1 for p in daily_pcts if p < 0)
    result["up_days_10d"] = up_days
    result["dn_days_10d"] = dn_days
    result["up_ratio_10d"] = up_days / len(daily_pcts)

    # ─── 2. 涨跌节奏模式 ─────────────────────────────────────────
    # 最近5天的涨跌模式 (e.g., 涨涨跌涨涨 = +2)
    recent_5 = daily_pcts[-5:]
    pattern_score = sum(1 if p > 0 else -1 for p in recent_5)
    result["pattern_score_5d"] = pattern_score

    # 是否存在"调整后回升"模式 (先跌后涨)
    first_half = daily_pcts[:5]
    second_half = daily_pcts[5:]
    fh_avg = np.mean(first_half)
    sh_avg = np.mean(second_half)
    result["first_half_avg_pct"] = fh_avg
    result["second_half_avg_pct"] = sh_avg
    result["pullback_then_rally"] = 1 if fh_avg < 0 and sh_avg > 0 else 0  # 先跌后涨

    # 是否"持续上涨无调整" (所有5天都是涨)
    all_up_5d = all(p > 0 for p in recent_5)
    result["all_up_5d"] = 1 if all_up_5d else 0

    # ─── 3. 波动节奏 ──────────────────────────────────────────────
    # 5天内的最大回撤
    close_5d = [df.iloc[i]["close"] for i in range(pre - 4, pre + 1)]
    max_dd_5d = 0
    peak = close_5d[0]
    for c in close_5d:
        if c > peak:
            peak = c
        dd = (c - peak) / peak * 100
        if dd < max_dd_5d:
            max_dd_5d = dd
    result["max_dd_5d"] = max_dd_5d

    # 5天内的高点到低点回撤
    max_5d = max(close_5d)
    min_after_max = close_5d[close_5d.index(max_5d):]
    if min_after_max:
        result["pullback_from_peak_5d"] = (min(min_after_max) - max_5d) / max_5d * 100
    else:
        result["pullback_from_peak_5d"] = 0

    # ─── 4. 买入前1天的位置 ──────────────────────────────────────
    # 是处于5天高位、低位还是中间?
    result["close_vs_5d_range"] = (close_5d[-1] - min(close_5d)) / (max(close_5d) - min(close_5d)) * 100 if max(close_5d) != min(close_5d) else 50

    # ─── 5. 量价节奏 ──────────────────────────────────────────────
    vols = [df.iloc[i]["volume"] for i in range(pre - 4, pre + 1)]
    avg_vol = np.mean(vols)

    # 最近3天量变化趋势
    vol_trend = "放量" if np.mean(vols[-3:]) > np.mean(vols[:3]) * 1.2 else ("缩量" if np.mean(vols[-3:]) < np.mean(vols[:3]) * 0.8 else "平稳")
    result["vol_trend_5d"] = vol_trend

    # 上涨放量/下跌缩量天数
    good_vol_days = 0
    for i in range(pre - 4, pre + 1):
        r = df.iloc[i]
        if r["pct_change"] > 0 and r["volume"] > avg_vol:
            good_vol_days += 1
        elif r["pct_change"] < 0 and r["volume"] < avg_vol:
            good_vol_days += 1
    result["good_vol_days_5d"] = good_vol_days

    # ─── 6. 均线节奏 ─────────────────────────────────────────────
    # 最近5天MA5的变化
    ma5_vals = [df.iloc[i].get("ma5", np.nan) for i in range(pre - 4, pre + 1)]
    ma5_valid = [v for v in ma5_vals if not np.isnan(v)]
    if len(ma5_valid) >= 2:
        result["ma5_trend_5d"] = (ma5_valid[-1] - ma5_valid[0]) / ma5_valid[0] * 100
    else:
        result["ma5_trend_5d"] = None

    # ─── 7. 最近N天的K线形态序列 ──────────────────────────────────
    kline_types = []
    for i in range(pre - 4, pre + 1):
        r = df.iloc[i]
        body = r["close"] - r["open"]
        total = r["high"] - r["low"]
        if total == 0:
            kline_types.append("十字")
            continue
        ratio = abs(body) / total
        if ratio < 0.15:
            kline_types.append("十字")
        elif body > 0:
            kline_types.append("阳")
        else:
            kline_types.append("阴")
    result["kline_5d"] = kline_types

    # ─── 8. 高低点节奏 ───────────────────────────────────────────
    highs_10d = [df.iloc[i]["high"] for i in range(pre - 9, pre + 1)]
    lows_10d = [df.iloc[i]["low"] for i in range(pre - 9, pre + 1)]
    max_high = max(highs_10d)
    min_low = min(lows_10d)

    # 收盘价在10天高低区间的位置
    result["close_in_10d_range"] = (df.iloc[pre]["close"] - min_low) / (max_high - min_low) * 100

    # 最高价出现的日期距离 (多少天前)
    max_high_idx = highs_10d.index(max_high)
    result["days_since_high_10d"] = len(highs_10d) - 1 - max_high_idx

    # ─── 9. 买入前的节奏类型判定 ──────────────────────────────────
    close = df.iloc[pre]["close"]
    close_prev3 = df.iloc[pre - 3]["close"] if pre >= 3 else close
    close_prev5 = df.iloc[pre - 5]["close"] if pre >= 5 else close
    ma5 = df.iloc[pre].get("ma5", close)

    # 判定节奏类型
    if all_up_5d:
        rhythm_type = "连续上涨(追高)"
        rhythm_score = -2  # 负面
    elif fh_avg < 0 and sh_avg > 0:
        rhythm_type = "先调后涨(回踩确认)"
        rhythm_score = 3  # 正面
    elif sh_avg > 0 and not all_up_5d:
        rhythm_type = "震荡上行"
        rhythm_score = 1
    elif fh_avg > 0 and sh_avg < 0:
        rhythm_type = "先涨后调(可能见顶)"
        rhythm_score = -1
    elif sh_avg < 0:
        rhythm_type = "持续下跌"
        rhythm_score = -2
    else:
        rhythm_type = "横盘震荡"
        rhythm_score = 0

    result["rhythm_type"] = rhythm_type
    result["rhythm_score"] = rhythm_score

    # ─── 10. 综合节奏评估 ────────────────────────────────────────
    # 理想节奏: 近期有小幅回调后重新企稳, 不在连续上涨高点
    eval_score = 0

    # 回调幅度适中 (3天内小回调 -2%~-5% 后企稳)
    cum3 = (close / close_prev3 - 1) * 100
    if -5 < cum3 < -1:
        eval_score += 2  # 适度回调
    elif cum3 > 5:
        eval_score -= 1  # 短期涨幅过大

    # 不在5天最高点
    if result["close_vs_5d_range"] < 80:
        eval_score += 1  # 不在高点
    elif result["close_vs_5d_range"] > 95:
        eval_score -= 1  # 在高点

    # 量价配合
    if good_vol_days >= 3:
        eval_score += 1

    result["rhythm_eval"] = eval_score

    return result


def main():
    print("=" * 110)
    print("买入前节奏分析 (Pre-buy Rhythm Analysis)")
    print("=" * 110)

    all_rhythms = []

    for buy_date, stocks in TRADES.items():
        for code, info in stocks.items():
            df = load_stock(code)
            if df is None:
                continue

            r = analyze_rhythm(df, buy_date)
            if r is None:
                continue

            buy_idx = get_idx(df, buy_date)
            buy_open = df.iloc[buy_idx]["open"]
            ret_5d = (df.iloc[buy_idx + 5]["close"] / buy_open - 1) * 100 if buy_idx + 5 < len(df) else None
            ret_10d = (df.iloc[buy_idx + 10]["close"] / buy_open - 1) * 100 if buy_idx + 10 < len(df) else None

            r["code"] = code
            r["name"] = info["name"]
            r["result"] = info["result"]
            r["cat"] = info["cat"]
            r["buy_date"] = buy_date
            r["ret_5d"] = ret_5d
            r["ret_10d"] = ret_10d

            all_rhythms.append(r)

    # Sort by rhythm_score
    all_rhythms.sort(key=lambda x: x["rhythm_score"], reverse=True)

    # Print individual analysis
    for r in all_rhythms:
        marker = "▲" if r["cat"] == "win" else "▼"
        print(f"\n{marker} {r['code']} {r['name']} ({r['buy_date']}) | 结果: {r['result']}")
        print(f"  节奏类型: {r['rhythm_type']} (评分: {r['rhythm_score']})")
        print(f"  综合节奏评估: {r['rhythm_eval']}")

        # Daily pattern
        pcts = r["daily_pcts"]
        pcts_str = " ".join([f"{'+' if p > 0 else ''}{p:.1f}%" for p in pcts])
        print(f"  10日涨跌: {pcts_str}")

        klines = r["kline_5d"]
        print(f"  近5日K线: {' '.join(klines)}")

        print(f"  涨跌天: {r['up_days_10d']}涨 / {r['dn_days_10d']}跌 | 5日模式分: {r['pattern_score_5d']}")
        print(f"  前半/后半均幅: {r['first_half_avg_pct']:.2f}% / {r['second_half_avg_pct']:.2f}%")
        print(f"  先调后涨: {'是' if r['pullback_then_rally'] else '否'} | 连涨5日: {'是' if r['all_up_5d'] else '否'}")
        print(f"  5日最大回撤: {r['max_dd_5d']:.2f}% | 峰值回撤: {r['pullback_from_peak_5d']:.2f}%")
        print(f"  收盘在5日区间位置: {r['close_vs_5d_range']:.0f}%")
        print(f"  量价配合天数: {r['good_vol_days_5d']}/5 | 量趋势: {r['vol_trend_5d']}")
        print(f"  MA5趋势(5日): {r.get('ma5_trend_5d', 'N/A')}")
        print(f"  10日高低位置: {r['close_in_10d_range']:.0f}% | 距高点天数: {r['days_since_high_10d']}")
        _fmt = lambda v: f"{v:.1f}%" if v is not None else "N/A"
        print(f"  实际: 5d={_fmt(r['ret_5d'])}, 10d={_fmt(r['ret_10d'])}")

    # ─── 统计对比 ────────────────────────────────────────────────
    print("\n" + "=" * 110)
    print("节奏因子统计对比")
    print("=" * 110)

    wins = [r for r in all_rhythms if r["cat"] == "win"]
    loses = [r for r in all_rhythms if r["cat"] == "lose"]

    metrics = [
        ("rhythm_score", "节奏评分"),
        ("rhythm_eval", "节奏评估"),
        ("up_ratio_10d", "10日涨天比例"),
        ("pattern_score_5d", "5日模式分"),
        ("pullback_then_rally", "先调后涨"),
        ("all_up_5d", "连续5涨"),
        ("close_vs_5d_range", "5日区间位置"),
        ("max_dd_5d", "5日最大回撤"),
        ("good_vol_days_5d", "量价配合天"),
        ("close_in_10d_range", "10日区间位置"),
        ("days_since_high_10d", "距高点天数"),
    ]

    print(f"\n{'指标':<20} {'涨票均值':>10} {'跌票均值':>10} {'差异':>10} {'方向':>8}")
    print("-" * 65)

    for key, label in metrics:
        w = [r[key] for r in wins if r.get(key) is not None]
        l = [r[key] for r in loses if r.get(key) is not None]
        if not w or not l:
            continue
        wm = np.mean(w)
        lm = np.mean(l)
        diff = wm - lm
        direction = "W>L" if diff > 0 else "L>W"
        print(f"{label:<20} {wm:>10.3f} {lm:>10.3f} {diff:>10.3f} {direction:>8}")

    # ─── 节奏类型分布 ────────────────────────────────────────────
    print("\n── 节奏类型分布 ──")
    from collections import Counter
    win_types = Counter([r["rhythm_type"] for r in wins])
    lose_types = Counter([r["rhythm_type"] for r in loses])

    print(f"\n涨票节奏类型: {dict(win_types)}")
    print(f"跌票节奏类型: {dict(lose_types)}")

    # ─── 综合节奏+技术面复合模型 ──────────────────────────────────
    print("\n" + "=" * 110)
    print("综合节奏+技术面复合判断")
    print("=" * 110)

    # Now integrate rhythm with the key tech indicators from previous analysis
    for r in all_rhythms:
        df = load_stock(r["code"])
        if df is None:
            continue
        buy_idx = get_idx(df, r["buy_date"])
        pre = buy_idx - 1
        row = df.iloc[pre]

        # Key tech signals
        k = row.get("kdj_k", 80)
        kdj_ob = 1 if k > 80 else 0
        ma_order = 1 if (row.get("ma5", 0) > row.get("ma10", 0) > row.get("ma20", 0)) else 0

        body = abs(row["close"] - row["open"])
        total_range = row["high"] - row["low"]
        lower_shadow = (min(row["close"], row["open"]) - row["low"]) / total_range if total_range > 0 else 0

        # Composite scoring
        comp = 0
        reasons = []

        # Rhythm component (40%)
        comp += r["rhythm_score"]
        reasons.append(f"节奏={r['rhythm_type']}")

        # KDJ component (30%)
        if k < 75:
            comp += 3
            reasons.append(f"KDJ适中({k:.0f})")
        elif k > 85:
            comp -= 3
            reasons.append(f"KDJ过高({k:.0f})")
        elif k > 80:
            comp -= 1

        # Rhythm eval (30%)
        comp += r["rhythm_eval"]

        # MA alignment
        if ma_order:
            comp += 1
        else:
            comp -= 2
            reasons.append("均线非多头")

        # Lower shadow
        if lower_shadow > 0.4:
            comp -= 2
            reasons.append(f"长下影({lower_shadow:.2f})")

        # Not at 5d high
        if r["close_vs_5d_range"] > 95:
            comp -= 1
            reasons.append("5日高位")
        elif r["close_vs_5d_range"] < 50:
            comp += 1
            reasons.append("5日低位(有空间)")

        r["comp_v2"] = comp
        r["comp_reasons"] = reasons

    all_rhythms.sort(key=lambda x: x["comp_v2"], reverse=True)

    correct = 0
    total = len(all_rhythms)
    print(f"\n{'股票':<14} {'结果':>4} {'复合分':>6} {'判断':>6} {'OK':>3} {'关键因素'}")
    print("-" * 90)

    for r in all_rhythms:
        is_win = r["cat"] == "win"
        s = r["comp_v2"]
        judge = "买入" if s >= 3 else ("回避" if s <= -1 else "中性")
        ok = (s >= 3 and is_win) or (s <= -1 and not is_win)
        if ok:
            correct += 1
        mark = "✓" if ok else ("△" if -1 < s < 3 else "✗")
        r_str = " | ".join(r.get("comp_reasons", [])[:5])
        print(f"{r['code']} {r['name']:<6} {r['result']:>4} {s:>6} {judge:>6} {mark:>3} {r_str}")

    print(f"\n复合模型准确率: {correct}/{total} = {correct/total*100:.1f}%")


if __name__ == "__main__":
    main()
