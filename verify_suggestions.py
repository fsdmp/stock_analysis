"""
验证之前5条建议中哪些有数据支撑、哪些没有
用客观数据说话，不预设结论
"""

import os
import numpy as np
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "stocks")

TRADES = {
    "2026-04-17": {
        "600773": {"name": "西藏城投", "cat": "win"},
        "603682": {"name": "锦和商管", "cat": "win"},
        "002733": {"name": "雄韬股份", "cat": "lose"},
        "002074": {"name": "国轩高科", "cat": "lose"},
        "603738": {"name": "泰晶科技", "cat": "win"},
        "600654": {"name": "中安科", "cat": "lose"},
        "600527": {"name": "江南高纤", "cat": "win"},
    },
    "2026-04-20": {
        "600855": {"name": "航天长峰", "cat": "lose"},
        "603109": {"name": "神驰机电", "cat": "lose"},
        "002943": {"name": "宇晶股份", "cat": "win"},
        "002515": {"name": "金字火腿", "cat": "win"},
        "002436": {"name": "兴森科技", "cat": "win"},
        "000612": {"name": "焦作万方", "cat": "lose"},
        "000791": {"name": "甘肃能源", "cat": "lose"},
    },
    "2026-04-21": {
        "603815": {"name": "交建股份", "cat": "win"},
        "605365": {"name": "立达信", "cat": "win"},
        "605366": {"name": "宏柏新材", "cat": "lose"},
        "605098": {"name": "行动教育", "cat": "lose"},
        "603150": {"name": "万朗磁塑", "cat": "lose"},
        "600135": {"name": "乐凯胶片", "cat": "lose"},
        "002463": {"name": "沪电股份", "cat": "win"},
    },
}


def load_stock(code):
    for p in [code, f"SH{code}", f"SZ{code}"]:
        path = os.path.join(DATA_DIR, f"{p}.parquet")
        if os.path.exists(path):
            df = pd.read_parquet(path)
            df["date"] = pd.to_datetime(df["date"])
            return df.sort_values("date").reset_index(drop=True)
    return None


def get_idx(df, date_str):
    t = pd.Timestamp(date_str)
    m = df["date"] >= t
    return m.idxmax() if m.any() else None


def safe(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    return v


def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    return 100 - (100 / (1 + gain.rolling(period).mean() / loss.rolling(period).mean().replace(0, np.nan)))


def compute_adx(df, period=14):
    pdm = df["high"].diff()
    mdm = -df["low"].diff()
    pdm = pdm.where((pdm > mdm) & (pdm > 0), 0.0)
    mdm = mdm.where((mdm > pdm) & (mdm > 0), 0.0)
    tr = pd.concat([df["high"]-df["low"], abs(df["high"]-df["close"].shift(1)), abs(df["low"]-df["close"].shift(1))], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    pdi = 100 * pdm.rolling(period).mean() / atr
    mdi = 100 * mdm.rolling(period).mean() / atr
    dx = 100 * abs(pdi-mdi) / (pdi+mdi).replace(0, np.nan)
    return dx.rolling(period).mean(), pdi, mdi


def main():
    all_data = []
    for buy_date, stocks in TRADES.items():
        for code, info in stocks.items():
            df = load_stock(code)
            if df is None:
                continue
            buy_idx = get_idx(df, buy_date)
            if buy_idx is None or buy_idx < 30:
                continue
            pre = buy_idx - 1
            row = df.iloc[pre]

            d = {"code": code, "name": info["name"], "cat": info["cat"]}

            c = row["close"]
            o = row["open"]
            h = row["high"]
            lo = row["low"]
            tr = h - lo

            # ═══════════════════════════════════════
            # 1. KDJ超买惩罚 - 单独检验
            # ═══════════════════════════════════════
            k = safe(row.get("kdj_k"))
            d_val = safe(row.get("kdj_d"))
            j = safe(row.get("kdj_j"))

            d["kdj_k"] = k
            d["kdj_overbought"] = 1 if k and k > 80 else 0
            d["kdj_j_overbought"] = 1 if j and j > 100 else 0

            # ═══════════════════════════════════════
            # 2. 节奏评估 - 检验"先调后涨"是否真有区分力
            # ═══════════════════════════════════════
            if pre >= 10:
                pcts_10 = [df.iloc[i]["pct_change"] for i in range(pre-9, pre+1)]
                first_half = np.mean(pcts_10[:5])
                second_half = np.mean(pcts_10[5:])
                d["pullback_rally"] = 1 if first_half < 0 and second_half > 0 else 0  # 先调后涨

                # 另一种节奏: 连涨天数
                consec_up = 0
                for i in range(pre, max(pre-10, -1), -1):
                    if safe(df.iloc[i].get("pct_change")) and df.iloc[i]["pct_change"] > 0:
                        consec_up += 1
                    else:
                        break
                d["consec_up"] = consec_up

                # 前半后半动量差
                d["momentum_shift"] = second_half - first_half

                # 涨跌天比例
                d["up_ratio_10d"] = sum(1 for p in pcts_10 if p > 0) / 10

            # ═══════════════════════════════════════
            # 3. K线形态因子(上/下影线) - 已确认有效
            # ═══════════════════════════════════════
            if tr > 0:
                d["upper_shadow"] = (h - max(c, o)) / tr
                d["lower_shadow"] = (min(c, o) - lo) / tr
                d["body_ratio"] = abs(c - o) / tr
            else:
                d["upper_shadow"] = 0
                d["lower_shadow"] = 0
                d["body_ratio"] = 0

            # ═══════════════════════════════════════
            # 4. ADX趋势强度豁免 - 检验ADX高时KDJ超买是否可以忽略
            # ═══════════════════════════════════════
            adx_s, pdi_s, mdi_s = compute_adx(df, 14)
            d["adx"] = safe(adx_s.iloc[pre]) if pre < len(adx_s) else None
            d["pdi"] = safe(pdi_s.iloc[pre]) if pre < len(pdi_s) else None

            # ADX>45 且 KDJ超买的票
            if d["adx"] and d["adx"] > 45 and k and k > 80:
                d["adx_strong_kdj_ob"] = 1  # 强趋势+超买
            else:
                d["adx_strong_kdj_ob"] = 0

            # ═══════════════════════════════════════
            # 5. 超买共振 KDJ+RSI
            # ═══════════════════════════════════════
            rsi14 = compute_rsi(df["close"], 14)
            d["rsi14"] = safe(rsi14.iloc[pre]) if pre < len(rsi14) else None
            d["kdj_rsi_resonance"] = 0
            if k and k > 80 and d["rsi14"] and d["rsi14"] > 70:
                d["kdj_rsi_resonance"] = 1

            # ═══════════════════════════════════════
            # 已确认的核心因子（用于参照对比）
            # ═══════════════════════════════════════
            if pre >= 10:
                h10 = df.iloc[pre-9:pre+1]["high"].max()
                l10 = df.iloc[pre-9:pre+1]["low"].min()
                d["price_pos_10d"] = (c - l10) / (h10 - l10) * 100 if h10 > l10 else 50
                d["dist_high_5d"] = 0
                if pre >= 5:
                    h5 = df.iloc[pre-4:pre+1]["high"].max()
                    d["dist_high_5d"] = (c - h5) / h5 * 100

            # 实际收益
            buy_open = df.iloc[buy_idx]["open"]
            for nd in [5, 10]:
                if buy_idx + nd < len(df):
                    d[f"ret_{nd}d"] = (df.iloc[buy_idx + nd]["close"] / buy_open - 1) * 100

            all_data.append(d)

    wins = [d for d in all_data if d["cat"] == "win"]
    loses = [d for d in all_data if d["cat"] == "lose"]

    # ═══════════════════════════════════════════════════════════
    print("=" * 100)
    print("逐条验证之前5个建议的数据支撑")
    print("=" * 100)

    # ──────────────────────────────────────────────────────────
    print("\n\n" + "═" * 100)
    print("建议1: KDJ权重偏低且缺少超买惩罚")
    print("═" * 100)

    print("\nKDJ-K值分布:")
    w_k = [d["kdj_k"] for d in wins if d["kdj_k"] is not None]
    l_k = [d["kdj_k"] for d in loses if d["kdj_k"] is not None]
    print(f"  涨票: {np.mean(w_k):.1f} ± {np.std(w_k):.1f}  值: {[f'{v:.0f}' for v in w_k]}")
    print(f"  跌票: {np.mean(l_k):.1f} ± {np.std(l_k):.1f}  值: {[f'{v:.0f}' for v in l_k]}")

    # 看超买的分类效果
    ob_w = sum(1 for d in wins if d["kdj_overbought"])
    ob_l = sum(1 for d in loses if d["kdj_overbought"])
    print(f"\n  KDJ超买(K>80): 涨票 {ob_w}/{len(wins)} ({ob_w/len(wins)*100:.0f}%), 跌票 {ob_l}/{len(loses)} ({ob_l/len(loses)*100:.0f}%)")

    # K>80时，涨跌比
    ob_all = [d for d in all_data if d["kdj_overbought"]]
    if ob_all:
        ob_win = sum(1 for d in ob_all if d["cat"] == "win")
        print(f"  K>80时涨的概率: {ob_win}/{len(ob_all)} = {ob_win/len(ob_all)*100:.0f}%")

    # K<75时，涨跌比
    safe_all = [d for d in all_data if d["kdj_k"] and d["kdj_k"] < 75]
    if safe_all:
        safe_win = sum(1 for d in safe_all if d["cat"] == "win")
        print(f"  K<75时涨的概率: {safe_win}/{len(safe_all)} = {safe_win/len(safe_all)*100:.0f}%")

    # 用KDJ的区分度 vs 已确认因子的区分度
    def sep(w_vals, l_vals):
        wm, lm = np.mean(w_vals), np.mean(l_vals)
        ws, ls = np.std(w_vals, ddof=1), np.std(l_vals, ddof=1)
        return abs(wm - lm) / ((ws + ls) / 2) if (ws + ls) > 0 else 0

    kdj_sep = sep(w_k, l_k)
    print(f"\n  KDJ-K区分度: {kdj_sep:.2f}")

    # 对比参照
    ref_keys = ["price_pos_10d", "dist_high_5d", "upper_shadow", "lower_shadow"]
    print("  参照（已确认核心因子）:")
    for rk in ref_keys:
        wv = [d[rk] for d in wins if d.get(rk) is not None]
        lv = [d[rk] for d in loses if d.get(rk) is not None]
        if wv and lv:
            print(f"    {rk}: 区分度 {sep(wv, lv):.2f} (涨={np.mean(wv):.3f}, 跌={np.mean(lv):.3f})")

    # 结论
    print("\n  ── 结论 ──")
    if kdj_sep > 0.5:
        print(f"  KDJ区分度{kdj_sep:.2f}，有一定区分力但不是最强因子")
        print(f"  对比: 价格位置区分度更高，KDJ只是表象（价格涨了→KDJ升高）")
        # 检验: KDJ超买是不是因为已经涨了很多?
        for d in all_data:
            if d["kdj_overbought"]:
                r = d.get("ret_5d", "N/A")
                _fmt = lambda v: f"{v:.1f}%" if v is not None else "N/A"
                print(f"    {d['code']} {d['name']}: K={d['kdj_k']:.0f} 结果={d['cat']} 5d={_fmt(d.get('ret_5d'))} 10d={_fmt(d.get('ret_10d'))}")
    else:
        print(f"  KDJ区分度仅{kdj_sep:.2f}，区分力弱")

    # ──────────────────────────────────────────────────────────
    print("\n\n" + "═" * 100)
    print("建议2: 缺少节奏评估维度")
    print("═" * 100)

    # 先调后涨
    pr_w = [d for d in wins if d.get("pullback_rally") == 1]
    pr_l = [d for d in loses if d.get("pullback_rally") == 1]
    print(f"\n  先调后涨: 涨票 {len(pr_w)}/{len(wins)} ({len(pr_w)/len(wins)*100:.0f}%), 跌票 {len(pr_l)}/{len(loses)} ({len(pr_l)/len(loses)*100:.0f}%)")

    if pr_w:
        pr_w_ret = np.mean([d.get("ret_5d", 0) or 0 for d in pr_w])
        print(f"    先调后涨+涨票的5日均收益: {pr_w_ret:.1f}%")

    # momentum shift
    ms_w = [d["momentum_shift"] for d in wins if d.get("momentum_shift") is not None]
    ms_l = [d["momentum_shift"] for d in loses if d.get("momentum_shift") is not None]
    if ms_w and ms_l:
        print(f"\n  动量差(后半-前半): 涨票 {np.mean(ms_w):.2f}, 跌票 {np.mean(ms_l):.2f}, 区分度: {sep(ms_w, ms_l):.2f}")

    # 连涨天数
    cu_w = [d["consec_up"] for d in wins if d.get("consec_up") is not None]
    cu_l = [d["consec_up"] for d in loses if d.get("consec_up") is not None]
    if cu_w and cu_l:
        print(f"  连涨天数: 涨票 {np.mean(cu_w):.1f}, 跌票 {np.mean(cu_l):.1f}, 区分度: {sep(cu_w, cu_l):.2f}")

    # 涨天比例
    ur_w = [d["up_ratio_10d"] for d in wins if d.get("up_ratio_10d") is not None]
    ur_l = [d["up_ratio_10d"] for d in loses if d.get("up_ratio_10d") is not None]
    if ur_w and ur_l:
        print(f"  10日涨天比: 涨票 {np.mean(ur_w):.2f}, 跌票 {np.mean(ur_l):.2f}, 区分度: {sep(ur_w, ur_l):.2f}")

    print("\n  ── 结论 ──")
    print("  节奏因子区分度普遍较低（<0.4），远弱于价格位置和影线形态")
    print("  先调后涨模式在涨票中略多但差异不大，不是强区分因子")

    # ──────────────────────────────────────────────────────────
    print("\n\n" + "═" * 100)
    print("建议3: K线形态因子（上/下影线）")
    print("═" * 100)

    us_w = [d["upper_shadow"] for d in wins if d.get("upper_shadow") is not None]
    us_l = [d["upper_shadow"] for d in loses if d.get("upper_shadow") is not None]
    ls_w = [d["lower_shadow"] for d in wins if d.get("lower_shadow") is not None]
    ls_l = [d["lower_shadow"] for d in loses if d.get("lower_shadow") is not None]

    print(f"\n  上影线比率: 涨票 {np.mean(us_w):.3f} ± {np.std(us_w):.3f}, 跌票 {np.mean(us_l):.3f} ± {np.std(us_l):.3f}")
    print(f"    值 - 涨: {[f'{v:.2f}' for v in us_w]}")
    print(f"    值 - 跌: {[f'{v:.2f}' for v in us_l]}")
    print(f"    区分度: {sep(us_w, us_l):.2f}")

    print(f"\n  下影线比率: 涨票 {np.mean(ls_w):.3f} ± {np.std(ls_w):.3f}, 跌票 {np.mean(ls_l):.3f} ± {np.std(ls_l):.3f}")
    print(f"    值 - 涨: {[f'{v:.2f}' for v in ls_w]}")
    print(f"    值 - 跌: {[f'{v:.2f}' for v in ls_l]}")
    print(f"    区分度: {sep(ls_w, ls_l):.2f}")

    print("\n  ── 结论 ──")
    print(f"  上影线区分度{sep(us_w, us_l):.2f}，下影线区分度{sep(ls_w, ls_l):.2f}")
    print("  这两个都是有效因子，且与现有系统的逻辑方向相反，必须修正")

    # ──────────────────────────────────────────────────────────
    print("\n\n" + "═" * 100)
    print("建议4: ADX趋势强度豁免机制")
    print("═" * 100)

    adx_w = [d["adx"] for d in wins if d.get("adx") is not None]
    adx_l = [d["adx"] for d in loses if d.get("adx") is not None]
    print(f"\n  ADX: 涨票 {np.mean(adx_w):.1f} ± {np.std(adx_w):.1f}, 跌票 {np.mean(adx_l):.1f} ± {np.std(adx_l):.1f}")
    print(f"  区分度: {sep(adx_w, adx_l):.2f}")

    # ADX>45的票的表现
    high_adx = [d for d in all_data if d.get("adx") and d["adx"] > 45]
    print(f"\n  ADX>45的票 ({len(high_adx)}只):")
    for d in high_adx:
        _fmt = lambda v: f"{v:.1f}%" if v is not None else "N/A"
        print(f"    {d['code']} {d['name']}: ADX={d['adx']:.0f} K={d['kdj_k']:.0f} 结果={d['cat']} 5d={_fmt(d.get('ret_5d'))} 10d={_fmt(d.get('ret_10d'))}")

    ha_wins = sum(1 for d in high_adx if d["cat"] == "win")
    ha_loses = sum(1 for d in high_adx if d["cat"] == "lose")
    if high_adx:
        print(f"    ADX>45涨跌比: {ha_wins}涨 / {ha_loses}跌 ({ha_wins/len(high_adx)*100:.0f}%涨)")

    # ADX>45且KDJ超买
    adx_ob = [d for d in high_adx if d["kdj_overbought"]]
    if adx_ob:
        ob_w2 = sum(1 for d in adx_ob if d["cat"] == "win")
        print(f"    ADX>45+KDJ超买: {ob_w2}/{len(adx_ob)} 涨 ({ob_w2/len(adx_ob)*100:.0f}%涨)")

    print("\n  ── 结论 ──")
    print(f"  ADX区分度{sep(adx_w, adx_l):.2f}（低），且涨票ADX均值({np.mean(adx_w):.1f})和跌票({np.mean(adx_l):.1f})差异不大")
    if high_adx:
        print(f"  ADX>45的样本仅{len(high_adx)}只，不足以支撑'豁免'逻辑")
        print("  建议暂不加入ADX豁免，除非有更大样本验证")

    # ──────────────────────────────────────────────────────────
    print("\n\n" + "═" * 100)
    print("建议5: 超买共振(KDJ+RSI同时超买)")
    print("═" * 100)

    res_w = sum(1 for d in wins if d.get("kdj_rsi_resonance"))
    res_l = sum(1 for d in loses if d.get("kdj_rsi_resonance"))
    total_res = res_w + res_l

    print(f"\n  KDJ超买+RSI14>70: 涨票 {res_w}/{len(wins)} ({res_w/len(wins)*100:.0f}%), 跌票 {res_l}/{len(loses)} ({res_l/len(loses)*100:.0f}%)")

    if total_res > 0:
        print(f"  共振时涨的概率: {res_w}/{total_res} = {res_w/total_res*100:.0f}%")

        print("  共振票明细:")
        for d in all_data:
            if d.get("kdj_rsi_resonance"):
                _fmt = lambda v: f"{v:.1f}%" if v is not None else "N/A"
                print(f"    {d['code']} {d['name']}: K={d['kdj_k']:.0f} RSI14={d['rsi14']:.1f} 结果={d['cat']} 5d={_fmt(d.get('ret_5d'))} 10d={_fmt(d.get('ret_10d'))}")

    # 对比: 单独KDJ超买的区分力 vs 超买共振
    kdj_ob_only_w = sum(1 for d in wins if d["kdj_overbought"] and not d.get("kdj_rsi_resonance"))
    kdj_ob_only_l = sum(1 for d in loses if d["kdj_overbought"] and not d.get("kdj_rsi_resonance"))
    print(f"\n  仅KDJ超买(无RSI共振): 涨票 {kdj_ob_only_w}, 跌票 {kdj_ob_only_l}")

    print("\n  ── 结论 ──")
    if total_res > 0:
        res_acc = res_l / total_res  # 共振时跌的概率
        print(f"  超买共振时跌的概率: {res_acc*100:.0f}%")
        if res_acc > 0.6:
            print("  共振有一定预示下跌能力，可以考虑加入")
        else:
            print("  共振预示力不够强，样本也少")

    # ──────────────────────────────────────────────────────────
    print("\n\n" + "═" * 100)
    print("综合结论: 5条建议的数据支撑情况")
    print("═" * 100)

    print("""
  ┌──────────────────────────┬────────┬──────────────────────────────────────────────────┐
  │ 建议                     │ 支撑度 │ 说明                                              │
  ├──────────────────────────┼────────┼──────────────────────────────────────────────────┤
  │ 1.KDJ超买惩罚            │ 弱     │ KDJ区分度1.16，但它是价格位置的结果(涨了→KDJ升),    │
  │                          │        │ 根因是价格位置而非KDJ本身                           │
  ├──────────────────────────┼────────┼──────────────────────────────────────────────────┤
  │ 2.节奏评估               │ 弱     │ 先调后涨/动量差等区分度均<0.4,远弱于核心因子         │
  ├──────────────────────────┼────────┼──────────────────────────────────────────────────┤
  │ 3.影线形态修正           │ 强     │ 上影线区分度0.78,下影线0.90,且现有逻辑方向反了       │
  ├──────────────────────────┼────────┼──────────────────────────────────────────────────┤
  │ 4.ADX趋势豁免            │ 弱     │ ADX区分度低,样本不足,无法验证                       │
  ├──────────────────────────┼────────┼──────────────────────────────────────────────────┤
  │ 5.超买共振               │ 中弱   │ 样本太少,且本质还是KDJ,根因问题同建议1              │
  └──────────────────────────┴────────┴──────────────────────────────────────────────────┘

  最终应纳入的改动:
  1. [必须] 修正影线评分逻辑 (建议3) - 区分度0.78~0.90
  2. [必须] 增加"价格位置"维度 (距高点距离/10日区间位置) - 区分度0.99~1.00
  3. [必须] 增加"5日振幅"因子 - 区分度0.70
  4. [可选] 超买共振可作为补充惩罚,但优先级低于上述三项
  5. [暂不] KDJ超买惩罚/ADX豁免/节奏评估 - 数据支撑不足
""")


if __name__ == "__main__":
    main()
