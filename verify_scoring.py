"""
验证修改后的评分系统对21只股票的区分效果
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import numpy as np
import pandas as pd
from stock_data.scoring import calc_score

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


def main():
    results = []

    for buy_date, stocks in TRADES.items():
        for code, info in stocks.items():
            df = load_stock(code)
            if df is None:
                continue

            buy_idx = get_idx(df, buy_date)
            if buy_idx is None or buy_idx < 30:
                continue

            # Score up to the day BEFORE buy (pre_idx = buy_idx - 1)
            pre_idx = buy_idx  # calc_score uses df up to this row
            sub = df.iloc[:pre_idx].copy()

            result = calc_score(sub)

            buy_open = df.iloc[buy_idx]["open"]
            ret_5d = (df.iloc[buy_idx + 5]["close"] / buy_open - 1) * 100 if buy_idx + 5 < len(df) else None
            ret_10d = (df.iloc[buy_idx + 10]["close"] / buy_open - 1) * 100 if buy_idx + 10 < len(df) else None

            results.append({
                "code": code,
                "name": info["name"],
                "cat": info["cat"],
                "score": result["total"],
                "action": result["action"],
                "dims": result["dimensions"],
                "ret_5d": ret_5d,
                "ret_10d": ret_10d,
            })

    # Sort by score descending
    results.sort(key=lambda x: x["score"], reverse=True)

    print("=" * 110)
    print("修改后评分系统验证 (21只股票)")
    print("=" * 110)

    print(f"\n{'股票':<14} {'结果':>4} {'评分':>5} {'建议':>8} {'5日':>7} {'10日':>7} {'OK':>3}  关键维度")
    print("-" * 110)

    correct = 0
    total = 0

    for r in results:
        is_win = r["cat"] == "win"
        s = r["score"]
        action = r["action"]

        if s >= 63:
            pred = "win"
        elif s < 50:
            pred = "lose"
        else:
            pred = "neutral"

        total += 1
        ok = (pred == "win" and is_win) or (pred == "lose" and not is_win)
        if pred != "neutral":
            correct += 1 if ok else 0

        mark = "✓" if ok else ("△" if pred == "neutral" else "✗")

        _f = lambda v: f"{v:+.1f}%" if v is not None else "N/A"

        # Key dimensions
        price_pos_dim = next((d for d in r["dims"] if d["name"] == "价格位置"), None)
        smart_dim = next((d for d in r["dims"] if d["name"] == "主力行为"), None)

        key_info = ""
        if price_pos_dim:
            key_info += f"[价格位置:{price_pos_dim['score']:+d}] "
        if smart_dim:
            key_info += f"[主力行为:{smart_dim['score']:+d}]"

        print(f"{r['code']} {r['name']:<6} {r['cat']:>4} {s:>5} {action:>8} {_f(r['ret_5d']):>7} {_f(r['ret_10d']):>7} {mark:>3}  {key_info}")

    # Stats
    wins = [r for r in results if r["cat"] == "win"]
    loses = [r for r in results if r["cat"] == "lose"]

    print(f"\n── 统计 ──")
    print(f"涨票评分: {np.mean([r['score'] for r in wins]):.1f} ± {np.std([r['score'] for r in wins]):.1f}")
    print(f"跌票评分: {np.mean([r['score'] for r in loses]):.1f} ± {np.std([r['score'] for r in loses]):.1f}")

    sep_val = abs(np.mean([r['score'] for r in wins]) - np.mean([r['score'] for r in loses])) / \
              ((np.std([r['score'] for r in wins]) + np.std([r['score'] for r in loses])) / 2)
    print(f"评分区分度: {sep_val:.2f}")

    decided = [r for r in results if r["score"] >= 63 or r["score"] < 50]
    if decided:
        d_correct = sum(1 for r in decided
                       if (r["score"] >= 63 and r["cat"] == "win") or
                          (r["score"] < 50 and r["cat"] == "lose"))
        print(f"判定数: {len(decided)}/{total}, 判定准确率: {d_correct}/{len(decided)} = {d_correct/len(decided)*100:.1f}%")

    # Show dimension scores for each stock
    print("\n── 各维度得分明细 ──")
    dim_names = [d["name"] for d in results[0]["dims"]]

    header = f"{'股票':<14}"
    for dn in dim_names:
        header += f" {dn[:6]:>7}"
    print(header)
    print("-" * len(header))

    for r in results:
        line = f"{r['code']} {r['name']:<5} "
        for d in r["dims"]:
            line += f" {d['score']:>+7}"
        print(line)

    # Price position dimension detail
    print("\n── 价格位置维度详情 ──")
    for r in results:
        pp = next((d for d in r["dims"] if d["name"] == "价格位置"), None)
        sm = next((d for d in r["dims"] if d["name"] == "主力行为"), None)
        marker = "▲" if r["cat"] == "win" else "▼"
        print(f"{marker} {r['code']} {r['name']:<5} 评分={r['score']} 价格位置={pp['detail'] if pp else 'N/A'} 主力行为={sm['detail'] if sm else 'N/A'}")


if __name__ == "__main__":
    main()
