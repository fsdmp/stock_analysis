"""基于评分系统的回测脚本.

规则:
  - 买入信号: score >= 63 (建议买入/强烈买入) → 次日开盘价买入
  - 卖出信号: score < 40 (建议卖出/强烈卖出) → 次日开盘价卖出
  - 完全基于评分信号，不做任何主观判断
"""

import sys
import time
import numpy as np
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# 确保项目根目录在 path 中
sys.path.insert(0, str(Path(__file__).resolve().parent))

from stock_data.scoring import calc_score
from stock_data.config import DATA_DIR

# ---------------------------------------------------------------------------
# 参数
# ---------------------------------------------------------------------------
BUY_THRESHOLD = 63   # >= 63 → 买入
SELL_THRESHOLD = 40   # < 40  → 卖出
MIN_HISTORY = 150     # 计算score需要的最少历史数据量
START_OFFSET = 30     # 前30天数据用于指标预热,不参与评分
WINDOW_SIZE = 150     # 每次传给calc_score的窗口大小(兼顾速度与准确性)
SAMPLE_SIZE = 200     # 随机抽样股票数 (None=全部)
DATE_START = "2024-01-01"  # 回测起始日
DATE_END = "2026-04-27"    # 回测结束日
WORKERS = 6           # 并行进程数


def load_stock_list() -> list[str]:
    """获取所有可用股票代码."""
    files = sorted(DATA_DIR.glob("*.parquet"))
    codes = [f.stem for f in files]
    return codes


def backtest_single_stock(code: str) -> dict | None:
    """对单只股票执行回测,返回交易记录列表和统计."""
    path = DATA_DIR / f"{code}.parquet"
    if not path.exists():
        return None

    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])

    # 日期过滤
    mask = (df["date"] >= DATE_START) & (df["date"] <= DATE_END)
    df_range = df[mask].reset_index(drop=True)
    if len(df_range) < 50:
        return None

    # 获取在日期范围内的起始行号 (需要前面的历史数据来计算score)
    start_idx_in_full = df.index[df["date"] >= DATE_START][0]
    # 实际开始计算score的位置: 需要至少 MIN_HISTORY 行历史
    calc_start = max(0, start_idx_in_full - MIN_HISTORY)

    # 交易日列表 (日期范围内的行)
    trade_dates = df_range["date"].tolist()
    n_trade = len(trade_dates)

    trades = []  # [{buy_date, buy_price, sell_date, sell_price, hold_days, return_pct}]
    holding = False
    buy_price = 0
    buy_date = None
    buy_idx = None

    for t in range(n_trade):
        # 计算该交易日对应的完整df中的行号
        full_idx = start_idx_in_full + t
        if full_idx < START_OFFSET:
            continue

        # 取窗口数据用于评分
        win_start = max(0, full_idx - WINDOW_SIZE + 1)
        df_win = df.iloc[win_start:full_idx + 1].copy()

        if len(df_win) < START_OFFSET:
            continue

        # 计算评分
        try:
            result = calc_score(df_win)
            score = result["total"]
        except Exception:
            continue

        current_date = trade_dates[t]
        # 次日开盘价 (t+1)
        if t + 1 >= n_trade:
            # 最后一天,无法次日操作
            # 如果持仓,按收盘价平仓
            if holding:
                sell_price = float(df_range.iloc[t]["close"])
                sell_date = current_date
                hold_days = (sell_date - buy_date).days
                ret = (sell_price - buy_price) / buy_price * 100
                trades.append({
                    "code": code,
                    "buy_date": str(buy_date.date()) if hasattr(buy_date, 'date') else str(buy_date),
                    "buy_price": round(buy_price, 4),
                    "sell_date": str(sell_date.date()) if hasattr(sell_date, 'date') else str(sell_date),
                    "sell_price": round(sell_price, 4),
                    "hold_days": hold_days,
                    "return_pct": round(ret, 2),
                    "exit_reason": "end_of_data",
                })
                holding = False
            break

        next_open = float(df_range.iloc[t + 1]["open"])
        next_date = trade_dates[t + 1]

        if not holding:
            # 等待买入信号
            if score >= BUY_THRESHOLD:
                holding = True
                buy_price = next_open
                buy_date = next_date
                buy_idx = t + 1
        else:
            # 持仓中,等待卖出信号
            if score < SELL_THRESHOLD:
                holding = False
                sell_price = next_open
                sell_date = next_date
                hold_days = (sell_date - buy_date).days
                ret = (sell_price - buy_price) / buy_price * 100
                trades.append({
                    "code": code,
                    "buy_date": str(buy_date.date()) if hasattr(buy_date, 'date') else str(buy_date),
                    "buy_price": round(buy_price, 4),
                    "sell_date": str(sell_date.date()) if hasattr(sell_date, 'date') else str(sell_date),
                    "sell_price": round(sell_price, 4),
                    "hold_days": hold_days,
                    "return_pct": round(ret, 2),
                    "exit_reason": "signal",
                })

    # 如果最后仍持仓, 用最后一天收盘价平仓
    if holding and n_trade > 0:
        sell_price = float(df_range.iloc[-1]["close"])
        sell_date = trade_dates[-1]
        hold_days = (sell_date - buy_date).days
        ret = (sell_price - buy_price) / buy_price * 100
        trades.append({
            "code": code,
            "buy_date": str(buy_date.date()) if hasattr(buy_date, 'date') else str(buy_date),
            "buy_price": round(buy_price, 4),
            "sell_date": str(sell_date.date()) if hasattr(sell_date, 'date') else str(sell_date),
            "sell_price": round(sell_price, 4),
            "hold_days": hold_days,
            "return_pct": round(ret, 2),
            "exit_reason": "end_of_data",
        })

    if not trades:
        return None

    return {"code": code, "trades": trades, "n_trade_days": n_trade}


def print_report(all_trades: list[dict], n_stocks: int, n_days: int, elapsed: float):
    """打印回测报告."""
    if not all_trades:
        print("\n无交易记录!")
        return

    df = pd.DataFrame(all_trades)
    print("\n" + "=" * 80)
    print("                    评分系统回测报告")
    print("=" * 80)
    print(f"回测区间: {DATE_START} ~ {DATE_END}")
    print(f"抽样股票: {n_stocks} 只")
    print(f"买入阈值: score >= {BUY_THRESHOLD}")
    print(f"卖出阈值: score < {SELL_THRESHOLD}")
    print(f"计算耗时: {elapsed:.1f}s")
    print("-" * 80)

    # === 基本统计 ===
    total_trades = len(df)
    win_trades = len(df[df["return_pct"] > 0])
    loss_trades = len(df[df["return_pct"] <= 0])
    win_rate = win_trades / total_trades * 100 if total_trades > 0 else 0

    avg_return = df["return_pct"].mean()
    median_return = df["return_pct"].median()
    avg_hold = df["hold_days"].mean()
    median_hold = df["hold_days"].median()

    total_return_sum = df["return_pct"].sum()
    # 计算复合收益率
    compound = (1 + df["return_pct"] / 100).prod() - 1

    print(f"\n{'总交易次数':>12}: {total_trades}")
    print(f"{'盈利次数':>12}: {win_trades}  ({win_rate:.1f}%)")
    print(f"{'亏损次数':>12}: {loss_trades}  ({100 - win_rate:.1f}%)")
    print(f"{'平均收益率':>12}: {avg_return:.2f}%")
    print(f"{'中位数收益率':>12}: {median_return:.2f}%")
    print(f"{'平均持仓天数':>12}: {avg_hold:.1f} 天")
    print(f"{'中位持仓天数':>12}: {median_hold:.1f} 天")

    # === 收益分布 ===
    print(f"\n{'─' * 40}")
    print("收益分布:")
    bins = [(-999, -10), (-10, -5), (-5, -3), (-3, 0), (0, 3), (3, 5), (5, 10), (10, 999)]
    labels = ["<-10%", "-10~-5%", "-5~-3%", "-3~0%", "0~3%", "3~5%", "5~10%", ">10%"]
    for (lo, hi), label in zip(bins, labels):
        count = len(df[(df["return_pct"] >= lo) & (df["return_pct"] < hi)])
        pct = count / total_trades * 100
        bar = "█" * int(pct / 2)
        print(f"  {label:>10}: {count:>4} ({pct:>5.1f}%) {bar}")

    # === 持仓天数分布 ===
    print(f"\n{'─' * 40}")
    print("持仓天数分布:")
    hold_bins = [(0, 3), (3, 7), (7, 14), (14, 30), (30, 60), (60, 999)]
    hold_labels = ["1-3天", "4-7天", "8-14天", "15-30天", "31-60天", ">60天"]
    for (lo, hi), label in zip(hold_bins, hold_labels):
        subset = df[(df["hold_days"] > lo) & (df["hold_days"] <= hi)]
        count = len(subset)
        pct = count / total_trades * 100
        if count > 0:
            avg_r = subset["return_pct"].mean()
            wr = len(subset[subset["return_pct"] > 0]) / count * 100
            print(f"  {label:>10}: {count:>4} ({pct:>5.1f}%)  平均收益:{avg_r:>6.2f}%  胜率:{wr:.0f}%")
        else:
            print(f"  {label:>10}: {count:>4} ({pct:>5.1f}%)")

    # === 按月统计 ===
    print(f"\n{'─' * 40}")
    print("按月统计:")
    df["sell_month"] = pd.to_datetime(df["sell_date"]).dt.to_period("M")
    monthly = df.groupby("sell_month").agg(
        trades=("return_pct", "count"),
        avg_return=("return_pct", "mean"),
        win_rate=("return_pct", lambda x: (x > 0).mean() * 100),
        total_return=("return_pct", "sum"),
    )
    for month, row in monthly.iterrows():
        print(f"  {month}: {row['trades']:>3.0f}笔  平均:{row['avg_return']:>+6.2f}%  "
              f"胜率:{row['win_rate']:>5.1f}%  累计:{row['total_return']:>+7.1f}%")

    # === 按评分等级分析 (买入时score信息) ===
    # 我们没有保存买入时的score, 所以基于收益结果分析

    # === 退出原因分析 ===
    print(f"\n{'─' * 40}")
    print("退出原因分析:")
    for reason in ["signal", "end_of_data"]:
        subset = df[df["exit_reason"] == reason]
        if len(subset) > 0:
            wr = len(subset[subset["return_pct"] > 0]) / len(subset) * 100
            avg_r = subset["return_pct"].mean()
            print(f"  {reason:>15}: {len(subset):>4}笔  平均收益:{avg_r:>+6.2f}%  胜率:{wr:.0f}%")

    # === 极端案例 ===
    print(f"\n{'─' * 40}")
    print("最佳交易 Top 5:")
    top5 = df.nlargest(5, "return_pct")[["code", "buy_date", "sell_date", "hold_days", "return_pct"]]
    for _, row in top5.iterrows():
        print(f"  {row['code']} {row['buy_date']}→{row['sell_date']} "
              f"({row['hold_days']}天) {row['return_pct']:+.2f}%")

    print("\n最差交易 Top 5:")
    bot5 = df.nsmallest(5, "return_pct")[["code", "buy_date", "sell_date", "hold_days", "return_pct"]]
    for _, row in bot5.iterrows():
        print(f"  {row['code']} {row['buy_date']}→{row['sell_date']} "
              f"({row['hold_days']}天) {row['return_pct']:+.2f}%")

    # === 综合评价 ===
    print(f"\n{'=' * 80}")
    print("综合评价:")
    print(f"  胜率: {win_rate:.1f}%")
    print(f"  平均每笔收益: {avg_return:.2f}%")
    print(f"  期望值(平均每笔): {avg_return:.2f}%")
    # 盈亏比
    if win_trades > 0 and loss_trades > 0:
        avg_win = df[df["return_pct"] > 0]["return_pct"].mean()
        avg_loss = abs(df[df["return_pct"] <= 0]["return_pct"].mean())
        profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else float('inf')
        print(f"  平均盈利: {avg_win:.2f}%  平均亏损: -{avg_loss:.2f}%")
        print(f"  盈亏比: {profit_loss_ratio:.2f}")

    # 年化估算
    years = (pd.Timestamp(DATE_END) - pd.Timestamp(DATE_START)).days / 365
    ann_ret = (1 + compound) ** (1 / years) - 1 if years > 0 else 0
    print(f"  复合收益率(连乘): {compound * 100:.2f}%")
    print(f"  估算年化: {ann_ret * 100:.2f}%")
    print("=" * 80)


def main():
    codes = load_stock_list()
    print(f"共发现 {len(codes)} 只股票数据")

    # 随机抽样
    if SAMPLE_SIZE and len(codes) > SAMPLE_SIZE:
        np.random.seed(42)
        codes = list(np.random.choice(codes, SAMPLE_SIZE, replace=False))
        print(f"随机抽样 {SAMPLE_SIZE} 只进行回测")

    print(f"开始回测... (买入>= {BUY_THRESHOLD}, 卖出< {SELL_THRESHOLD})")
    t0 = time.time()

    all_trades = []
    processed = 0

    # 并行处理
    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(backtest_single_stock, code): code for code in codes}

        for future in as_completed(futures):
            code = futures[future]
            processed += 1
            if processed % 20 == 0:
                elapsed = time.time() - t0
                eta = elapsed / processed * (len(codes) - processed)
                print(f"  进度: {processed}/{len(codes)} ({elapsed:.0f}s, 预计还需 {eta:.0f}s)")

            try:
                result = future.result()
                if result and result["trades"]:
                    all_trades.extend(result["trades"])
            except Exception as e:
                print(f"  [ERROR] {code}: {e}")

    elapsed = time.time() - t0
    print(f"\n回测完成! 共处理 {processed} 只股票, 产生 {len(all_trades)} 笔交易, 耗时 {elapsed:.1f}s")

    print_report(all_trades, processed, 0, elapsed)

    # 保存交易明细
    if all_trades:
        out_path = Path(__file__).parent / "backtest_trades.csv"
        pd.DataFrame(all_trades).to_csv(out_path, index=False)
        print(f"\n交易明细已保存到: {out_path}")


if __name__ == "__main__":
    main()
