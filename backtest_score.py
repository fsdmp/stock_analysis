"""评分系统回测：验证评分与次日/3日收益的相关性.

逻辑：
  1. 随机抽样股票，在过去N个月内每隔M个交易日做一次评分
  2. 评分只用该日期之前的数据（无未来函数）
  3. 记录次日/3日实际收益率，按分数段汇总统计
"""

import sys
import random
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

from stock_data.scoring import calc_score

DATA_DIR = Path("data/stocks")

# ---- 参数 ----
SAMPLE_SIZE = 100          # 抽样股票数
MONTHS_BACK = 1            # 回测最近N个月
SCORE_INTERVAL = 5         # 每隔M个交易日评一次分
TOP_N_THRESHOLD = 78       # "强烈买入"阈值
BUY_THRESHOLD = 63         # "建议买入"阈值


def load_all_codes():
    files = sorted(DATA_DIR.glob("*.parquet"))
    return [f.stem for f in files]


def backtest():
    all_codes = load_all_codes()
    random.seed(42)
    codes = random.sample(all_codes, min(SAMPLE_SIZE, len(all_codes)))

    cutoff = datetime(2026, 4, 24) - timedelta(days=MONTHS_BACK * 30)
    cutoff_str = cutoff.strftime("%Y-%m-%d")

    records = []
    total, skipped, errors = 0, 0, 0

    for idx, code in enumerate(codes):
        if (idx + 1) % 50 == 0:
            print(f"  进度: {idx+1}/{len(codes)} 条记录: {len(records)}", file=sys.stderr)

        path = DATA_DIR / f"{code}.parquet"
        df = pd.read_parquet(path)
        if len(df) < 60:
            continue

        # 只取回测窗口内的数据 + 之前30天(评分最低需要)
        df["date_str"] = df["date"].astype(str).str[:10]
        mask = df["date_str"] >= cutoff_str
        window_idx = df[mask].index.tolist()
        if not window_idx:
            continue

        # 每隔 SCORE_INTERVAL 个交易日评一次
        sample_indices = window_idx[::SCORE_INTERVAL]

        for si in sample_indices:
            si = int(si)
            total += 1

            # 未来收益
            if si + 3 >= len(df):
                skipped += 1
                continue

            next1 = (df["close"].iloc[si + 1] / df["close"].iloc[si] - 1) * 100
            next3 = (df["close"].iloc[si + 3] / df["close"].iloc[si] - 1) * 100

            # 用 si 及之前的数据评分 (无未来函数)
            sub_df = df.iloc[: si + 1].copy()
            if len(sub_df) < 30:
                skipped += 1
                continue

            try:
                result = calc_score(sub_df)
            except Exception:
                errors += 1
                continue

            score = result["total"]
            pct = float(df["pct_change"].iloc[si]) if not pd.isna(df["pct_change"].iloc[si]) else 0

            records.append({
                "code": code,
                "date": df["date_str"].iloc[si],
                "score": score,
                "action": result["action"],
                "pct_today": round(pct, 2),
                "ret_1d": round(next1, 2),
                "ret_3d": round(next3, 2),
            })

    print(f"\n统计: 评分{total}次, 有效{len(records)}条, 跳过{skipped}, 错误{errors}", file=sys.stderr)
    return pd.DataFrame(records)


def report(df: pd.DataFrame):
    print("\n" + "=" * 80)
    print("评分系统回测报告")
    print("=" * 80)

    # ---- 1. 按分数段统计 ----
    bins = [0, 25, 40, 50, 63, 78, 101]
    labels = ["<25 强烈卖出", "25-40 建议卖出", "40-50 观望", "50-63 偏多观望", "63-78 建议买入", "78+ 强烈买入"]
    df["bucket"] = pd.cut(df["score"], bins=bins, labels=labels, right=False)

    print("\n## 1. 按分数段统计")
    print(f"{'分数段':<18s} | {'样本数':>6s} | {'次日均收益':>10s} | {'次日胜率':>8s} | {'3日均收益':>10s} | {'3日胜率':>8s}")
    print("-" * 80)

    for label in labels:
        g = df[df["bucket"] == label]
        if len(g) == 0:
            print(f"{label:<18s} | {'0':>6s} | {'-':>10s} | {'-':>8s} | {'-':>10s} | {'-':>8s}")
            continue
        avg1 = g["ret_1d"].mean()
        avg3 = g["ret_3d"].mean()
        wr1 = (g["ret_1d"] > 0).mean() * 100
        wr3 = (g["ret_3d"] > 0).mean() * 100
        print(f"{label:<18s} | {len(g):>6d} | {avg1:>+9.2f}% | {wr1:>7.1f}% | {avg3:>+9.2f}% | {wr3:>7.1f}%")

    # ---- 2. 买入信号分析 ----
    print("\n## 2. 买入信号分析")
    buy = df[df["score"] >= BUY_THRESHOLD]
    strong_buy = df[df["score"] >= TOP_N_THRESHOLD]

    for name, subset in [("建议买入(63+)", buy), ("强烈买入(78+)", strong_buy)]:
        if len(subset) == 0:
            print(f"  {name}: 无样本")
            continue
        avg1 = subset["ret_1d"].mean()
        avg3 = subset["ret_3d"].mean()
        wr1 = (subset["ret_1d"] > 0).mean() * 100
        wr3 = (subset["ret_3d"] > 0).mean() * 100
        med1 = subset["ret_1d"].median()
        print(f"  {name} ({len(subset)}条):")
        print(f"    次日: 均收益{avg1:+.2f}%  中位数{med1:+.2f}%  胜率{wr1:.1f}%")
        print(f"    3日:  均收益{avg3:+.2f}%  中位数{subset['ret_3d'].median():+.2f}%  胜率{wr3:.1f}%")

    # ---- 3. 卖出信号分析 ----
    print("\n## 3. 卖出信号分析")
    sell = df[df["score"] < 40]
    if len(sell) > 0:
        avg1 = sell["ret_1d"].mean()
        avg3 = sell["ret_3d"].mean()
        wr1 = (sell["ret_1d"] > 0).mean() * 100
        print(f"  卖出(<40) ({len(sell)}条):")
        print(f"    次日: 均收益{avg1:+.2f}%  胜率{wr1:.1f}%")
        print(f"    3日:  均收益{avg3:+.2f}%  胜率{(sell['ret_3d']>0).mean()*100:.1f}%")

    # ---- 4. 涨停板评分分析 ----
    print("\n## 4. 涨停板评分分析 (当日pct >= 9.5%)")
    zt = df[df["pct_today"] >= 9.5]
    if len(zt) > 0:
        zt_scores = zt["score"].describe()
        print(f"  样本数: {len(zt)}")
        print(f"  评分分布: 均值{zt_scores['mean']:.1f}  中位{zt_scores['50%']:.1f}  最小{zt_scores['min']:.0f}  最大{zt_scores['max']:.0f}")
        avg1 = zt["ret_1d"].mean()
        avg3 = zt["ret_3d"].mean()
        wr1 = (zt["ret_1d"] > 0).mean() * 100
        print(f"  次日: 均收益{avg1:+.2f}%  胜率{wr1:.1f}%")
        print(f"  3日:  均收益{avg3:+.2f}%  胜率{(zt['ret_3d']>0).mean()*100:.1f}%")
    else:
        print("  无涨停样本")

    # ---- 5. 相关性 ----
    print("\n## 5. 评分与收益的相关性")
    if len(df) > 10:
        corr1 = df["score"].corr(df["ret_1d"])
        corr3 = df["score"].corr(df["ret_3d"])
        print(f"  评分 vs 次日收益: r = {corr1:.4f}")
        print(f"  评分 vs 3日收益:  r = {corr3:.4f}")

        # IC (Information Coefficient): 评分与收益的rank相关系数
        ic1 = df["score"].corr(df["ret_1d"], method="spearman")
        ic3 = df["score"].corr(df["ret_3d"], method="spearman")
        print(f"  Rank IC (次日): {ic1:.4f}")
        print(f"  Rank IC (3日):  {ic3:.4f}")

    # ---- 6. 分数区分度 ----
    print("\n## 6. 分数区分度 (高分-低分收益差)")
    if len(df) > 20:
        top = df.nlargest(max(1, len(df) // 10), "score")
        bot = df.nsmallest(max(1, len(df) // 10), "score")
        diff_1d = top["ret_1d"].mean() - bot["ret_1d"].mean()
        diff_3d = top["ret_3d"].mean() - bot["ret_3d"].mean()
        print(f"  前10%均分: {top['score'].mean():.1f}  次日均收益: {top['ret_1d'].mean():+.2f}%")
        print(f"  后10%均分: {bot['score'].mean():.1f}  次日均收益: {bot['ret_1d'].mean():+.2f}%")
        print(f"  收益差: 次日{diff_1d:+.2f}%  3日{diff_3d:+.2f}%")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    print("开始回测...", file=sys.stderr)
    df = backtest()
    if len(df) == 0:
        print("无有效数据", file=sys.stderr)
        sys.exit(1)
    report(df)
