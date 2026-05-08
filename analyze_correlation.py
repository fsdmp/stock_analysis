"""
综合分析: 21只验证票 + 4月自选股的评分与收益相关性
目标: 找出提高相关性的方向
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import numpy as np
import pandas as pd
from stock_data.scoring import calc_score
import json

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "stocks")

# 21只验证票
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

# 自选中4月的涨停回踩均线组
WATCHLIST_04 = {
    "2026-04-26": {
        "60涨停回踩均线0426": ["600261", "600678", "600699", "603788", "603815", "603898"],
        "00涨停回踩均线0426": ["000404", "002492", "002803", "002885"],
    },
    "2026-04-27": {
        "00涨停回踩均线0427": ["000539", "000690", "000802", "002752", "002952"],
        "60涨停回踩均线0427": ["600105", "600446", "600487", "600869", "601061", "603496", "603637", "603906", "605376"],
    },
    "2026-04-28": {
        "00涨停回踩均线0428": ["000811", "000990", "002256", "002397", "002443", "002636"],
        "60涨停回踩均线0428": ["600310", "601016", "601778", "603538", "603709"],
    },
    "2026-04-29": {
        "60涨停回踩均线0429": ["600370", "600499", "600527", "603066", "603520", "603599", "603680"],
        "00涨停回踩均线0429": ["000890", "001267", "001314", "003022"],
    },
    "2026-04-30": {
        "60涨停回踩均线0430": ["600693", "600830", "603110", "603125", "603211", "603579", "603610", "603626", "603657", "605118", "605138"],
        "00涨停回踩均线0430": ["000546", "000906", "001339", "002795", "002812"],
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


def main():
    # 加载股票名称
    names_path = os.path.join(os.path.dirname(__file__), "data", "stock_names.json")
    if os.path.exists(names_path):
        with open(names_path) as f:
            stock_names = json.load(f)
    else:
        stock_names = {}

    records = []

    # ═══ 21只验证票 ═══
    for buy_date, stocks in TRADES.items():
        for code, info in stocks.items():
            df = load_stock(code)
            if df is None:
                continue
            buy_idx = get_idx(df, buy_date)
            if buy_idx is None or buy_idx < 30:
                continue
            sub = df.iloc[:buy_idx].copy()
            result = calc_score(sub)

            buy_open = df.iloc[buy_idx]["open"]
            ret_5d = (df.iloc[buy_idx + 5]["close"] / buy_open - 1) * 100 if buy_idx + 5 < len(df) else None
            ret_3d = (df.iloc[buy_idx + 3]["close"] / buy_open - 1) * 100 if buy_idx + 3 < len(df) else None

            # 5日内最高收益
            max_ret = None
            for k in range(1, 6):
                if buy_idx + k < len(df):
                    r = (df.iloc[buy_idx + k]["high"] / buy_open - 1) * 100
                    if max_ret is None or r > max_ret:
                        max_ret = r

            dim_scores = {}
            for d in result["dimensions"]:
                dim_scores[d["name"]] = d["score"]

            records.append({
                "code": code, "name": info.get("name", stock_names.get(code, "")),
                "cat": info.get("cat"), "source": "验证票",
                "total": result["total"], "action": result["action"],
                "ret_3d": ret_3d, "ret_5d": ret_5d, "max_ret_5d": max_ret,
                **dim_scores,
            })

    # ═══ 4月自选票 ═══
    for buy_date, groups in WATCHLIST_04.items():
        for group_name, codes in groups.items():
            for code in codes:
                df = load_stock(code)
                if df is None:
                    continue
                buy_idx = get_idx(df, buy_date)
                if buy_idx is None or buy_idx < 30:
                    continue
                sub = df.iloc[:buy_idx].copy()
                result = calc_score(sub)

                buy_open = df.iloc[buy_idx]["open"]
                ret_5d = (df.iloc[buy_idx + 5]["close"] / buy_open - 1) * 100 if buy_idx + 5 < len(df) else None
                ret_3d = (df.iloc[buy_idx + 3]["close"] / buy_open - 1) * 100 if buy_idx + 3 < len(df) else None

                max_ret = None
                for k in range(1, 6):
                    if buy_idx + k < len(df):
                        r = (df.iloc[buy_idx + k]["high"] / buy_open - 1) * 100
                        if max_ret is None or r > max_ret:
                            max_ret = r

                if ret_5d is not None:
                    cat = "win" if ret_5d > 0 else "lose"
                else:
                    cat = "unknown"

                dim_scores = {}
                for d in result["dimensions"]:
                    dim_scores[d["name"]] = d["score"]

                records.append({
                    "code": code, "name": stock_names.get(code, ""),
                    "cat": cat, "source": "自选04",
                    "total": result["total"], "action": result["action"],
                    "ret_3d": ret_3d, "ret_5d": ret_5d, "max_ret_5d": max_ret,
                    **dim_scores,
                })

    df_res = pd.DataFrame(records)
    valid5 = df_res[df_res["ret_5d"].notna()].copy()
    valid3 = df_res[df_res["ret_3d"].notna()].copy()

    print("=" * 90)
    print(f"综合分析: {len(valid5)}只票 (21验证票 + {len(valid5)-21}只自选票) 有5日收益数据")
    print("=" * 90)

    # ═══ 1. 总分与收益相关性 ═══
    print("\n── 1. 总分与收益相关性 ──")
    for col, label in [("ret_3d", "3日收益"), ("ret_5d", "5日收益"), ("max_ret_5d", "5日最高")]:
        v = df_res[df_res[col].notna()]
        if len(v) >= 5:
            c = np.corrcoef(v["total"], v[col])[0, 1]
            print(f"  总分 vs {label}: r={c:+.3f} (n={len(v)})")

    # ═══ 2. 各维度与5日收益相关性 ═══
    print("\n── 2. 各维度与5日收益相关性 ──")
    sample = load_stock("600773")
    dim_names = [d["name"] for d in calc_score(sample.iloc[:60])["dimensions"]]

    dim_corrs = {}
    for dn in dim_names:
        if dn in valid5.columns:
            c = np.corrcoef(valid5[dn], valid5["ret_5d"])[0, 1]
            dim_corrs[dn] = c

    print(f"\n{'维度':<12} {'与5日收益r':>10} {'涨票均值':>8} {'跌票均值':>8} {'差值':>7} {'方向'}")
    print("-" * 65)
    for dn, c in sorted(dim_corrs.items(), key=lambda x: abs(x[1]), reverse=True):
        wins = valid5[valid5["ret_5d"] > 0][dn]
        loses = valid5[valid5["ret_5d"] <= 0][dn]
        direction = "正相关" if c > 0 else "负相关"
        sep = wins.mean() - loses.mean()
        print(f"  {dn:<10} {c:>+10.3f} {wins.mean():>+8.1f} {loses.mean():>+8.1f} {sep:>+7.1f}  {direction}")

    # ═══ 3. 按评分分组看收益 ═══
    print("\n── 3. 按评分分桶看5日收益 ──")
    for label, lo, hi in [("强烈买入(≥78)", 78, 200), ("建议买入(63-77)", 63, 78), ("偏多观望(50-62)", 50, 63), ("观望(<50)", 0, 50)]:
        bucket = valid5[(valid5["total"] >= lo) & (valid5["total"] < hi)]
        if len(bucket) > 0:
            mean_ret = bucket["ret_5d"].mean()
            win_rate = (bucket["ret_5d"] > 0).sum() / len(bucket) * 100
            print(f"  {label}: {len(bucket)}只, 均收益={mean_ret:+.2f}%, 胜率={win_rate:.0f}%")

    # ═══ 4. 残差分析 ═══
    print("\n── 4. 残差分析 (总分线性预测 vs 实际5日收益) ──")
    x = valid5["total"].values
    y = valid5["ret_5d"].values
    coeffs = np.polyfit(x, y, 1)
    y_pred = np.polyval(coeffs, x)
    valid5 = valid5.copy()
    valid5["residual"] = y - y_pred
    valid5["pred"] = y_pred

    print(f"\n  线性模型: 收益 = {coeffs[0]:.3f}*评分 + {coeffs[1]:.1f}")
    print(f"\n  {'代码':<8} {'名称':<6} {'评分':>5} {'5日收益':>8} {'预测':>8} {'残差':>8} {'来源':>8}")
    print("  " + "-" * 55)
    for _, r in valid5.sort_values("residual", ascending=False).iterrows():
        marker = "▲" if r["ret_5d"] > 0 else "▼"
        print(f"  {marker}{r['code']:<7} {r['name']:<6} {r['total']:>5} {r['ret_5d']:>+7.1f}% {r['pred']:>+7.1f}% {r['residual']:>+7.1f}% {r['source']:>8}")

    # ═══ 5. 异常票对比分析 ═══
    print("\n── 5. 严重异常票的维度特征 ──")
    threshold = 8
    pos_outliers = valid5[valid5["residual"] > threshold]  # 实际远好于预测
    neg_outliers = valid5[valid5["residual"] < -threshold]  # 实际远差于预测

    if len(pos_outliers) > 0:
        print(f"\n  正残差票(实际远好于预测, 残差>{threshold}%): {len(pos_outliers)}只")
        for _, r in pos_outliers.iterrows():
            dims = {dn: r[dn] for dn in dim_names if dn in r.index}
            sorted_dims = sorted(dims.items(), key=lambda x: x[1], reverse=True)
            top3 = sorted_dims[:3]
            bot3 = sorted_dims[-3:]
            print(f"    {r['code']} {r['name']} 评分={r['total']} 实际={r['ret_5d']:+.1f}% 残差={r['residual']:+.1f}%")
            print(f"      最强: {', '.join(f'{k}={v:+d}' for k, v in top3)}")
            print(f"      最弱: {', '.join(f'{k}={v:+d}' for k, v in bot3)}")

    if len(neg_outliers) > 0:
        print(f"\n  负残差票(实际远差于预测, 残差<-{threshold}%): {len(neg_outliers)}只")
        for _, r in neg_outliers.iterrows():
            dims = {dn: r[dn] for dn in dim_names if dn in r.index}
            sorted_dims = sorted(dims.items(), key=lambda x: x[1], reverse=True)
            top3 = sorted_dims[:3]
            bot3 = sorted_dims[-3:]
            print(f"    {r['code']} {r['name']} 评分={r['total']} 实际={r['ret_5d']:+.1f}% 残差={r['residual']:+.1f}%")
            print(f"      最强: {', '.join(f'{k}={v:+d}' for k, v in top3)}")
            print(f"      最弱: {', '.join(f'{k}={v:+d}' for k, v in bot3)}")

    # ═══ 6. 维度组合优化搜索 ═══
    print("\n── 6. 维度组合与5日收益相关性 ──")

    # 单维度排名
    print("\n  单维度(与总分对比):")
    total_corr = abs(np.corrcoef(valid5["total"], valid5["ret_5d"])[0, 1])
    print(f"    当前总分: |r|={total_corr:.3f}")
    for dn in dim_names:
        if dn in valid5.columns:
            c = abs(np.corrcoef(valid5[dn], valid5["ret_5d"])[0, 1])
            print(f"    {dn:<10}: |r|={c:.3f}")

    # 权重扫描
    print("\n  权重敏感性(调整单个维度权重对|r|的影响):")
    current_weights = {
        "短期动量": 15, "价格位置": 14, "量价配合": 13, "MACD动能": 10,
        "主力行为": 9, "换手率": 8, "MA趋势": 6, "支撑压力": 5,
        "KDJ状态": 3, "均线形态": 2, "背离信号": 2, "趋势确认": 2,
    }

    for dn in dim_names:
        if dn not in current_weights or dn not in valid5.columns:
            continue
        orig_w = current_weights[dn]
        results = []
        for mult in [0.0, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0]:
            total_test = np.zeros(len(valid5))
            total_w = 0
            for d, w in current_weights.items():
                if d in valid5.columns:
                    actual_w = w * mult if d == dn else w
                    total_test += valid5[d].values * actual_w
                    total_w += abs(actual_w)
            total_test = total_test / total_w * 100
            c = abs(np.corrcoef(total_test, valid5["ret_5d"])[0, 1])
            results.append((mult, c))

        best = max(results, key=lambda x: x[1])
        improvement = best[1] - total_corr
        if abs(improvement) > 0.005:
            direction = "↑" if improvement > 0 else "↓"
            print(f"    {dn:<10} 最优倍率={best[0]:.2f}x → |r|={best[1]:.3f} ({direction}{abs(improvement):.3f})")

    # 最优权重全局搜索(简化版: 每个维度从0.5x到3x)
    print("\n  全局权重搜索(随机采样1000组):")
    best_corr_global = total_corr
    best_weights_global = dict(current_weights)

    np.random.seed(42)
    weight_range = [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0]
    for trial in range(2000):
        test_weights = {}
        for dn, w in current_weights.items():
            test_weights[dn] = w * np.random.choice(weight_range)

        total_test = np.zeros(len(valid5))
        total_w = 0
        for d, w in test_weights.items():
            if d in valid5.columns:
                total_test += valid5[d].values * w
                total_w += abs(w)

        if total_w == 0:
            continue
        total_test = total_test / total_w * 100
        c = abs(np.corrcoef(total_test, valid5["ret_5d"])[0, 1])
        if c > best_corr_global:
            best_corr_global = c
            best_weights_global = dict(test_weights)

    if best_corr_global > total_corr:
        print(f"    找到更优权重: |r|={best_corr_global:.3f} (vs 当前{total_corr:.3f})")
        for dn in dim_names:
            if dn in current_weights:
                old = current_weights[dn]
                new = best_weights_global.get(dn, old)
                mult = new / old if old > 0 else 0
                if abs(mult - 1.0) > 0.1:
                    print(f"      {dn:<10}: {old} → {new:.1f} ({mult:.2f}x)")
    else:
        print(f"    未找到更优权重 (当前已是最优: |r|={total_corr:.3f})")

    # ═══ 7. 加入底层特征分析 ═══
    print("\n── 7. 底层特征与5日收益分析 (直接从数据提取) ──")
    all_features = []
    all_records = []

    # 合并所有票
    all_trades = {}
    for buy_date, stocks in TRADES.items():
        for code, info in stocks.items():
            all_trades[code] = (buy_date, info.get("cat"))
    for buy_date, groups in WATCHLIST_04.items():
        for group_name, codes in groups.items():
            for code in codes:
                if code not in all_trades:
                    all_trades[code] = (buy_date, None)

    for code, (buy_date, cat) in all_trades.items():
        df = load_stock(code)
        if df is None:
            continue
        buy_idx = get_idx(df, buy_date)
        if buy_idx is None or buy_idx < 30:
            continue

        buy_open = df.iloc[buy_idx]["open"]
        ret_5d = (df.iloc[buy_idx + 5]["close"] / buy_open - 1) * 100 if buy_idx + 5 < len(df) else None
        if ret_5d is None:
            continue

        c = df.iloc[buy_idx - 1]["close"]
        o = df.iloc[buy_idx - 1]["open"]
        h = df.iloc[buy_idx - 1]["high"]
        lo = df.iloc[buy_idx - 1]["low"]
        v = df.iloc[buy_idx - 1]["volume"]

        tr = max(h - lo, abs(h - c), abs(lo - c)) if lo > 0 else 1

        feat = {"code": code, "ret_5d": ret_5d}

        # K线形态特征
        feat["body_ratio"] = abs(c - o) / tr if tr > 0 else 0
        feat["us_ratio"] = (h - max(c, o)) / tr if tr > 0 else 0
        feat["ls_ratio"] = (min(c, o) - lo) / tr if tr > 0 else 0
        feat["is_yang"] = 1 if c > o else 0

        # 涨跌幅
        pre_c = df.iloc[buy_idx - 2]["close"] if buy_idx >= 2 else c
        c3 = df.iloc[buy_idx - 3]["close"] if buy_idx >= 3 else c
        c5 = df.iloc[buy_idx - 5]["close"] if buy_idx >= 5 else c
        feat["chg_1d"] = (c / pre_c - 1) * 100 if pre_c > 0 else 0
        feat["chg_3d"] = (c / c3 - 1) * 100 if c3 > 0 else 0
        feat["chg_5d"] = (c / c5 - 1) * 100 if c5 > 0 else 0

        # 量比
        if buy_idx >= 6:
            v5 = np.mean([df.iloc[buy_idx - 1 - i]["volume"] for i in range(1, 6)])
            feat["vol_ratio"] = v / v5 if v5 > 0 else 1
        else:
            feat["vol_ratio"] = 1

        # 区间位置
        if buy_idx >= 11:
            h10 = max(df.iloc[buy_idx - 1 - i]["high"] for i in range(10))
            l10 = min(df.iloc[buy_idx - 1 - i]["low"] for i in range(10))
            feat["pos_10d"] = (c - l10) / (h10 - l10) * 100 if h10 > l10 else 50
        else:
            feat["pos_10d"] = 50

        if buy_idx >= 5:
            h5 = max(df.iloc[buy_idx - 1 - i]["high"] for i in range(5))
            l5 = min(df.iloc[buy_idx - 1 - i]["low"] for i in range(5))
            feat["dist_high_5d"] = (c - h5) / h5 * 100 if h5 > 0 else 0
            feat["range_5d"] = (h5 - l5) / l5 * 100 if l5 > 0 else 0
        else:
            feat["dist_high_5d"] = 0
            feat["range_5d"] = 0

        # 买入日跳空
        feat["gap"] = (buy_open / c - 1) * 100 if c > 0 else 0

        # 连涨连跌
        streak = 0
        for i in range(buy_idx - 1, max(0, buy_idx - 10), -1):
            if df.iloc[i]["close"] > df.iloc[i - 1]["close"]:
                streak += 1
            else:
                break
        feat["streak_up"] = streak

        # 量价配合: 放量涨 + 缩量跌
        if buy_idx >= 3:
            chg = (c - pre_c) / pre_c * 100 if pre_c > 0 else 0
            chg_prev = (pre_c - df.iloc[buy_idx - 3]["close"]) / df.iloc[buy_idx - 3]["close"] * 100 if buy_idx >= 3 else 0
            vol_prev = df.iloc[buy_idx - 2]["volume"]
            if chg > 0 and v > vol_prev:
                feat["vol_price_up"] = 1  # 放量涨
            elif chg < 0 and v < vol_prev:
                feat["vol_price_up"] = 2  # 缩量跌
            elif chg > 0 and v < vol_prev:
                feat["vol_price_up"] = 3  # 缩量涨
            else:
                feat["vol_price_up"] = 0  # 放量跌
        else:
            feat["vol_price_up"] = 0

        # 前日是否涨停
        feat["prev_limit_up"] = 1 if feat["chg_1d"] > 9.5 else 0

        # 换手率(如果有)
        if "turnover" in df.columns:
            feat["turnover"] = df.iloc[buy_idx - 1].get("turnover", 0)
            feat["turnover_ma5"] = np.mean([df.iloc[buy_idx - 1 - i].get("turnover", 0) for i in range(5)])
        elif "turnover_rate" in df.columns:
            feat["turnover"] = df.iloc[buy_idx - 1].get("turnover_rate", 0)
            feat["turnover_ma5"] = np.mean([df.iloc[buy_idx - 1 - i].get("turnover_rate", 0) for i in range(5)])

        # 技术指标
        for col_name in ["kdj_k", "kdj_d", "macd", "macd_signal", "macd_hist", "rsi", "adx"]:
            if col_name in df.columns:
                feat[col_name] = df.iloc[buy_idx - 1][col_name]

        all_features.append(feat)

    df_feat = pd.DataFrame(all_features)
    print(f"\n  共{len(df_feat)}只票有底层特征数据")
    print(f"\n  {'特征':<18} {'与5日r':>8} {'涨票均值':>10} {'跌票均值':>10} {'差值':>8}")
    print("  " + "-" * 60)

    feat_corrs = {}
    skip = {"code", "ret_5d"}
    for col in df_feat.columns:
        if col in skip:
            continue
        valid_f = df_feat[[col, "ret_5d"]].dropna()
        if len(valid_f) < 10:
            continue
        c = np.corrcoef(valid_f[col], valid_f["ret_5d"])[0, 1]
        wins = valid_f[valid_f["ret_5d"] > 0][col]
        loses = valid_f[valid_f["ret_5d"] <= 0][col]
        diff = wins.mean() - loses.mean()
        feat_corrs[col] = (c, diff)

    for col, (c, diff) in sorted(feat_corrs.items(), key=lambda x: abs(x[1][0]), reverse=True):
        wins = df_feat[df_feat["ret_5d"] > 0][col].dropna()
        loses = df_feat[df_feat["ret_5d"] <= 0][col].dropna()
        if len(wins) >= 5 and len(loses) >= 3:
            print(f"  {col:<16} {c:>+8.3f} {wins.mean():>+10.2f} {loses.mean():>+10.2f} {diff:>+8.2f}")


if __name__ == "__main__":
    main()
