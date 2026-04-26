"""从涨跌结果反推特征：找赢家和输家的共性，用于优化评分."""

import sys, random, json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from stock_data.scoring import calc_score

DATA_DIR = Path("data/stocks")

SAMPLE_SIZE = 80
MONTHS_BACK = 2
INTERVAL = 3


def backtest():
    files = sorted(DATA_DIR.glob("*.parquet"))
    random.seed(42)
    codes = random.sample([f.stem for f in files], min(SAMPLE_SIZE, len(files)))

    cutoff = (datetime(2026, 4, 24) - timedelta(days=MONTHS_BACK * 30)).strftime("%Y-%m-%d")

    records = []
    for idx, code in enumerate(codes):
        if (idx + 1) % 20 == 0:
            print(f"  {idx+1}/{len(codes)} ...", file=sys.stderr)
        df = pd.read_parquet(DATA_DIR / f"{code}.parquet")
        if len(df) < 60:
            continue
        df["ds"] = df["date"].astype(str).str[:10]
        indices = df[df["ds"] >= cutoff].index.tolist()[::INTERVAL]
        for si in indices:
            si = int(si)
            if si + 3 >= len(df):
                continue
            next1 = (df["close"].iloc[si + 1] / df["close"].iloc[si] - 1) * 100
            next3 = (df["close"].iloc[si + 3] / df["close"].iloc[si] - 1) * 100
            sub = df.iloc[: si + 1].copy()
            if len(sub) < 30:
                continue
            try:
                r = calc_score(sub)
            except Exception:
                continue

            rec = {
                "code": code,
                "date": df["ds"].iloc[si],
                "total_score": r["total"],
                "ret_1d": round(next1, 2),
                "ret_3d": round(next3, 2),
                "pct_today": round(float(sub["pct_change"].iloc[-1] if not pd.isna(sub["pct_change"].iloc[-1]) else 0), 2),
            }
            for d in r["dimensions"]:
                rec[f"dim_{d['name']}_score"] = d["score"]
                rec[f"dim_{d['name']}_detail"] = d["detail"]
            records.append(rec)

    return pd.DataFrame(records)


def analyze(df: pd.DataFrame):
    print("=" * 80)
    print("赢家 vs 输家 特征对比分析")
    print("=" * 80)

    # 定义赢家/输家: 次日收益 top/bottom 20%
    p80, p20 = df["ret_1d"].quantile(0.80), df["ret_1d"].quantile(0.20)
    winners = df[df["ret_1d"] >= p80]
    losers = df[df["ret_1d"] <= p20]
    mid = df[(df["ret_1d"] > p20) & (df["ret_1d"] < p80)]

    print(f"\n样本: 总{len(df)}, 赢家(≥{p80:+.2f}%) {len(winners)}条, 输家(≤{p20:+.2f}%) {len(losers)}条\n")

    # ---- 1. 各维度得分分布对比 ----
    dim_cols = [c for c in df.columns if c.startswith("dim_") and c.endswith("_score")]
    dim_names = [c.replace("dim_", "").replace("_score", "") for c in dim_cols]

    print("## 1. 各维度得分: 赢家 vs 输家 vs 中间")
    print(f"{'维度':<10s} | {'赢家均分':>8s} | {'输家均分':>8s} | {'中间均分':>8s} | {'差值(赢-输)':>10s} | {'区分方向':>8s}")
    print("-" * 75)

    dim_diff = []
    for name, col in zip(dim_names, dim_cols):
        w_avg = winners[col].mean()
        l_avg = losers[col].mean()
        m_avg = mid[col].mean()
        diff = w_avg - l_avg
        direction = "正向" if diff > 0 else "反向"
        dim_diff.append((name, col, diff, abs(diff)))
        print(f"{name:<10s} | {w_avg:>+8.2f} | {l_avg:>+8.2f} | {m_avg:>+8.2f} | {diff:>+10.2f} | {direction:>8s}")

    # ---- 2. 总分分布 ----
    print(f"\n## 2. 总分分布")
    for label, subset in [("赢家", winners), ("输家", losers), ("中间", mid)]:
        s = subset["total_score"]
        print(f"  {label}: 均分{s.mean():.1f}  中位{s.median():.1f}  "
              f"≥63比例{(s >= 63).mean() * 100:.1f}%  ≥78比例{(s >= 78).mean() * 100:.1f}%")

    # ---- 3. 今日涨幅分布 ----
    print(f"\n## 3. 今日涨幅分布")
    for label, subset in [("赢家", winners), ("输家", losers), ("中间", mid)]:
        p = subset["pct_today"]
        print(f"  {label}: 均值{p.mean():+.2f}%  中位{p.median():+.2f}%  "
              f"涨停(≥9.5%)比例{(p >= 9.5).mean() * 100:.1f}%  "
              f"上涨比例{(p > 0).mean() * 100:.1f}%")

    # ---- 4. 特征详情高频词 ----
    print(f"\n## 4. 赢家高频信号 (维度detail中出现频率)")
    detail_cols = [c for c in df.columns if c.startswith("dim_") and c.endswith("_detail")]
    for label, subset in [("赢家", winners), ("输家", losers)]:
        print(f"\n  --- {label} ---")
        all_details = []
        for col in detail_cols:
            all_details.extend(subset[col].dropna().tolist())
        # 统计关键词频率
        keywords = {}
        for detail in all_details:
            for part in str(detail).split(";"):
                part = part.strip()
                if part:
                    keywords[part] = keywords.get(part, 0) + 1
        # 取 top 15
        top = sorted(keywords.items(), key=lambda x: -x[1])[:15]
        for kw, cnt in top:
            pct = cnt / len(subset) * 100
            print(f"    {kw:<40s} {cnt:>4d}次 ({pct:.1f}%)")

    # ---- 5. 组合特征: 哪些维度组合能区分赢家 ----
    print(f"\n## 5. 维度预测力排名 (按赢家/输家区分度)")
    dim_diff.sort(key=lambda x: -x[3])
    for rank, (name, col, diff, ad) in enumerate(dim_diff, 1):
        # 计算该维度的多头胜率
        pos = df[df[col] > 0]
        neg = df[df[col] < 0]
        pos_wr = (pos["ret_1d"] > 0).mean() * 100 if len(pos) > 5 else 0
        neg_wr = (neg["ret_1d"] > 0).mean() * 100 if len(neg) > 5 else 0
        pos_avg = pos["ret_1d"].mean() if len(pos) > 5 else 0
        neg_avg = neg["ret_1d"].mean() if len(neg) > 5 else 0
        corr = df[col].corr(df["ret_1d"])
        print(f"  #{rank} {name:<10s} 区分度={diff:+.2f}  "
              f"多头胜率={pos_wr:.1f}%(均{pos_avg:+.2f}%)  "
              f"空头胜率={neg_wr:.1f}%(均{neg_avg:+.2f}%)  "
              f"r={corr:+.4f}")

    # ---- 6. 输出优化建议 ----
    print(f"\n## 6. 优化建议 (基于数据)")
    # 找出区分度最大和最小的维度
    useful = [(n, d) for n, _, d, _ in dim_diff if d >= 1.0]
    useless = [(n, d) for n, _, d, _ in dim_diff if d < 0.3]
    reverse = [(n, d) for n, _, d, _ in dim_diff if d < -0.5]

    if useful:
        print(f"  有效维度(区分度≥1.0):")
        for n, d in useful:
            print(f"    {n}: {d:+.2f}")
    if reverse:
        print(f"  反向维度(输家分更高):")
        for n, d in reverse:
            print(f"    {n}: {d:+.2f}  ← 需要反转或降权")
    if useless:
        print(f"  无效维度(区分度<0.3):")
        for n, d in useless:
            print(f"    {n}: {d:+.2f}  ← 建议降权")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    print("回测中...", file=sys.stderr)
    df = backtest()
    print(f"共{len(df)}条记录", file=sys.stderr)
    if len(df) > 0:
        analyze(df)
