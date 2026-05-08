"""
用自选股中0506和0507两组"涨停回踩均线"票验证修改后的评分系统
这些票的买入日就是分组名中的日期(0506=5月6日, 0507=5月7日)
后续1天的走势在5月7日的数据中可以验证
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import numpy as np
import pandas as pd
from stock_data.scoring import calc_score

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "stocks")

# 0506组的票（买入日=5月6日开盘，可看5月7日表现）
GROUPS_0506 = {
    "60涨停回踩均线0506": [
        "600227", "600320", "600488", "600496", "600623", "600744",
        "601177", "601789", "601828", "603259", "603283", "603338", "603779",
    ],
    "00涨停回踩均线0506": [
        "000565", "000952", "001332", "001339", "002029", "002115",
        "002134", "002154", "002484", "002706", "002741",
    ],
}

# 0507组的票（买入日=5月7日开盘，无后续数据但可看评分分布）
GROUPS_0507 = {
    "60涨停回踩均线0507": [
        "600111", "600135", "600186", "600367", "600392", "600397",
        "600606", "600654", "600708", "600881",
        "603013", "603017", "603083", "603139", "603279", "603489", "603508", "603933",
    ],
    "00涨停回踩均线0507": [
        "001298", "001358", "002218", "002297", "002575", "002715",
        "002733", "002821", "003026",
    ],
}

# 持仓
HELD = ["002428", "002730", "600186", "002606", "002407", "603876"]


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


def score_at_date(df, date_str):
    """在指定日期的前一天收盘后计算评分"""
    idx = get_idx(df, date_str)
    if idx is None or idx < 30:
        return None, None
    sub = df.iloc[:idx].copy()
    result = calc_score(sub)
    return result, idx


def main():
    print("=" * 120)
    print("自选股验证: 修改后评分系统在0506/0507组票上的表现")
    print("=" * 120)

    # ── Part 1: 0506组（有5月7日数据可验证）──
    print("\n" + "═" * 120)
    print("Part 1: 0506组 (买入日=5月6日开盘, 可验证5月7日表现)")
    print("═" * 120)

    results_0506 = []
    for group_name, codes in GROUPS_0506.items():
        for code in codes:
            df = load_stock(code)
            if df is None:
                continue

            result, buy_idx = score_at_date(df, "2026-05-06")
            if result is None:
                continue

            buy_open = df.iloc[buy_idx]["open"]
            # 次日(5月7日)收益
            next_idx = buy_idx + 1
            if next_idx < len(df):
                next_close = df.iloc[next_idx]["close"]
                next_high = df.iloc[next_idx]["high"]
                next_low = df.iloc[next_idx]["low"]
                ret_1d = (next_close / buy_open - 1) * 100
                max_ret_1d = (next_high / buy_open - 1) * 100
                max_dd_1d = (next_low / buy_open - 1) * 100
            else:
                ret_1d = max_ret_1d = max_dd_1d = None

            # 也计算5月7日收盘时的评分
            result_next, _ = score_at_date(df, "2026-05-08")

            results_0506.append({
                "code": code,
                "group": group_name[:2],
                "score": result["total"],
                "action": result["action"],
                "dims": result["dimensions"],
                "buy_open": buy_open,
                "ret_1d": ret_1d,
                "max_ret_1d": max_ret_1d,
                "max_dd_1d": max_dd_1d,
                "score_next": result_next["total"] if result_next else None,
            })

    results_0506.sort(key=lambda x: x["score"], reverse=True)

    _f = lambda v: f"{v:+.2f}%" if v is not None else "N/A"

    print(f"\n{'代码':<8} {'市场':>4} {'评分':>5} {'建议':>8} {'买入价':>8} {'次日收益':>8} {'次日最高':>8} {'次日最低':>8} {'次日评分':>8}  价格位置详情")
    print("-" * 130)

    for r in results_0506:
        pp = next((d for d in r["dims"] if d["name"] == "价格位置"), None)
        sm = next((d for d in r["dims"] if d["name"] == "主力行为"), None)
        pp_detail = pp["detail"] if pp else "N/A"
        sm_score = f"主力:{sm['score']:+d}" if sm else ""

        print(f"{r['code']:<8} {r['group']:>4} {r['score']:>5} {r['action']:>8} {r['buy_open']:>8.2f} {_f(r['ret_1d']):>8} {_f(r['max_ret_1d']):>8} {_f(r['max_dd_1d']):>8} {r['score_next'] or 'N/A':>8}  {pp_detail} {sm_score}")

    # 统计: 高评分 vs 低评分的表现差异
    if results_0506:
        median_score = np.median([r["score"] for r in results_0506])
        high = [r for r in results_0506 if r["score"] >= median_score]
        low = [r for r in results_0506 if r["score"] < median_score]

        high_rets = [r["ret_1d"] for r in high if r["ret_1d"] is not None]
        low_rets = [r["ret_1d"] for r in low if r["ret_1d"] is not None]

        print(f"\n── 统计 (中位数={median_score:.0f}) ──")
        if high_rets:
            print(f"  高分组(≥{median_score:.0f}, {len(high_rets)}只): 次日均收益 {np.mean(high_rets):+.2f}%, 胜率 {sum(1 for r in high_rets if r > 0)/len(high_rets)*100:.0f}%")
        if low_rets:
            print(f"  低分组(<{median_score:.0f}, {len(low_rets)}只): 次日均收益 {np.mean(low_rets):+.2f}%, 胜率 {sum(1 for r in low_rets if r > 0)/len(low_rets)*100:.0f}%")

        # 评分与收益相关性
        scores = [r["score"] for r in results_0506 if r["ret_1d"] is not None]
        rets = [r["ret_1d"] for r in results_0506 if r["ret_1d"] is not None]
        if len(scores) >= 3:
            corr = np.corrcoef(scores, rets)[0, 1]
            print(f"  评分与次日收益相关系数: {corr:.3f}")

    # ── Part 2: 0507组 ──
    print("\n" + "═" * 120)
    print("Part 2: 0507组 (买入日=5月7日, 查看评分分布)")
    print("═" * 120)

    results_0507 = []
    for group_name, codes in GROUPS_0507.items():
        for code in codes:
            df = load_stock(code)
            if df is None:
                continue
            result, idx = score_at_date(df, "2026-05-07")
            if result is None:
                continue

            buy_open = df.iloc[idx]["open"]

            results_0507.append({
                "code": code,
                "group": group_name[:2],
                "score": result["total"],
                "action": result["action"],
                "dims": result["dimensions"],
                "buy_open": buy_open,
            })

    results_0507.sort(key=lambda x: x["score"], reverse=True)

    print(f"\n{'代码':<8} {'市场':>4} {'评分':>5} {'建议':>8} {'买入价':>8}  价格位置详情")
    print("-" * 100)

    for r in results_0507:
        pp = next((d for d in r["dims"] if d["name"] == "价格位置"), None)
        sm = next((d for d in r["dims"] if d["name"] == "主力行为"), None)
        pp_detail = pp["detail"] if pp else "N/A"
        sm_score = f"主力:{sm['score']:+d}" if sm else ""

        print(f"{r['code']:<8} {r['group']:>4} {r['score']:>5} {r['action']:>8} {r['buy_open']:>8.2f}  {pp_detail} {sm_score}")

    # ── Part 3: 持仓股 ──
    print("\n" + "═" * 120)
    print("Part 3: 当前持仓")
    print("═" * 120)

    for code in HELD:
        df = load_stock(code)
        if df is None:
            continue
        # 用最新数据评分
        result = calc_score(df)
        pp = next((d for d in result["dimensions"] if d["name"] == "价格位置"), None)
        sm = next((d for d in result["dimensions"] if d["name"] == "主力行为"), None)

        last_date = df.iloc[-1]["date"].strftime("%Y-%m-%d")
        last_close = df.iloc[-1]["close"]

        print(f"  {code} | 评分={result['total']} {result['action']} | 最新: {last_date} 收盘{last_close:.2f}")
        if pp:
            print(f"    价格位置: {pp['detail']}")
        if sm:
            print(f"    主力行为: {sm['detail']}")

    # ── Part 4: 综合统计 ──
    print("\n" + "═" * 120)
    print("综合统计")
    print("═" * 120)

    all_test = results_0506 + results_0507
    if all_test:
        scores = [r["score"] for r in all_test]
        print(f"\n  总票数: {len(all_test)}")
        print(f"  评分分布: {np.min(scores):.0f} / {np.percentile(scores, 25):.0f} / {np.median(scores):.0f} / {np.percentile(scores, 75):.0f} / {np.max(scores):.0f}")
        print(f"  均值: {np.mean(scores):.1f}, 标准差: {np.std(scores):.1f}")

        # 按评分分桶统计
        for label, lo, hi in [("强烈买入(≥78)", 78, 200), ("建议买入(63-77)", 63, 78), ("偏多观望(50-62)", 50, 63), ("观望(<50)", 0, 50)]:
            bucket = [r for r in all_test if lo <= r["score"] < hi]
            if bucket:
                print(f"\n  {label}: {len(bucket)}只")
                for r in sorted(bucket, key=lambda x: -x["score"]):
                    pp = next((d for d in r["dims"] if d["name"] == "价格位置"), None)
                    sm = next((d for d in r["dims"] if d["name"] == "主力行为"), None)
                    ret_str = ""
                    if r.get("ret_1d") is not None:
                        ret_str = f" 次日:{r['ret_1d']:+.2f}%"
                    print(f"    {r['code']} {r['score']} | 价格位置={pp['score'] if pp else 'N/A':+} 主力={sm['score'] if sm else 'N/A':+}{ret_str}")


if __name__ == "__main__":
    main()
