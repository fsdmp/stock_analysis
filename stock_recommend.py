#!/usr/bin/env python3
"""
股票技术面分析与推荐脚本

基于涨停回踩均线策略，对指定股票池进行多维度技术分析并给出买入推荐。

功能模式:
  1. 批量分析模式:  对一组股票进行综合评分排序，给出买入推荐
  2. 深度分析模式:  对指定股票输出详细K线和技术指标
  3. 对比分析模式:  对两只股票进行多维度对比，判断确定性高低
  4. 自选股模式:    读取 watchlist.json 中指定分组的股票进行分析

用法:
  python3.10 stock_recommend.py --stocks 002498,000404,603711         # 批量分析
  python3.10 stock_recommend.py --deep 002498                         # 深度分析
  python3.10 stock_recommend.py --compare 000404,002498               # 对比分析
  python3.10 stock_recommend.py --watchlist "00涨停回踩均线0508"       # 按自选股分组
  python3.10 stock_recommend.py --watchlist-all 0508                  # 按日期获取所有分组
  python3.10 stock_recommend.py --watchlist-all 0508 --compare-top 2  # 分析+对比前2名
"""

import argparse
import json
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

# ============================================================================
# 路径配置
# ============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", "stocks")
WATCHLIST_PATH = os.path.join(BASE_DIR, "data", "watchlist.json")
STOCK_NAMES_PATH = os.path.join(BASE_DIR, "data", "stock_names.json")

# ============================================================================
# 评分权重配置
# ============================================================================
SCORE_WEIGHTS = {
    "ma_support_5": 3,       # 回踩MA5附近
    "ma_support_7": 2,       # MA7支撑
    "ma_support_10": 2,      # 回踩MA10附近
    "ma_bullish_align": 3,   # 均线多头排列
    "macd_golden_cross": 4,  # MACD金叉
    "macd_red_expand": 2,    # MACD红柱放大
    "macd_bullish": 1,       # MACD多头
    "kdj_golden_cross": 4,   # KDJ金叉
    "kdj_bullish": 2,        # KDJ多头(K<80)
    "volume_shrink_heavy": 3, # 极度缩量(<0.6)
    "volume_shrink": 2,       # 缩量(<0.8)
    "volume_mild": 1,         # 温和量(<1.0)
    "window_best": 3,         # 涨停后3-4天(最佳窗口)
    "window_ok": 1,           # 涨停后5天
    "pullback_healthy": 2,    # 健康回调(3%-12%)
    "consolidation_end": 1,   # 调整末期(小阴小阳)
    "ma5_up": 1,              # MA5趋势向上
}

RATING_LEVELS = [
    (18, "强力推荐买入"),
    (15, "重点关注，可考虑买入"),
    (12, "可以关注，等待更好时机"),
    (8,  "观望为主，信号不够强"),
    (0,  "暂不建议买入"),
]


# ============================================================================
# 数据加载
# ============================================================================
def load_stock_names():
    """加载股票名称映射"""
    if not os.path.exists(STOCK_NAMES_PATH):
        return {}
    with open(STOCK_NAMES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_stock(code):
    """加载单只股票K线数据"""
    path = os.path.join(DATA_DIR, f"{code}.parquet")
    if not os.path.exists(path):
        return None
    df = pd.read_parquet(path)
    df = df.sort_values("date").reset_index(drop=True)
    return df


def load_watchlist():
    """加载自选股配置"""
    if not os.path.exists(WATCHLIST_PATH):
        return {}
    with open(WATCHLIST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_stocks_by_group(group_name):
    """按分组名称获取股票列表"""
    wl = load_watchlist()
    for g in wl.get("groups", []):
        if group_name in g["name"]:
            return g["stocks"], g["name"]
    return None, None


def get_stocks_by_date_keyword(keyword):
    """按日期关键词获取所有匹配分组的股票(去重)"""
    wl = load_watchlist()
    stocks = []
    groups = []
    for g in wl.get("groups", []):
        if keyword in g["name"]:
            groups.append(g["name"])
            for s in g["stocks"]:
                if s not in stocks:
                    stocks.append(s)
    return stocks, groups


def resolve_stock_code(name_or_code, stock_names):
    """将股票名称解析为代码"""
    if name_or_code.isdigit() and len(name_or_code) == 6:
        return name_or_code
    # 反向查找
    inv = {v: k for k, v in stock_names.items()}
    return inv.get(name_or_code, name_or_code)


# ============================================================================
# 工具函数
# ============================================================================
def deviation(price, ma):
    """计算价格偏离均线的百分比"""
    if pd.isna(ma) or ma == 0:
        return np.nan
    return (price - ma) / ma * 100


def fmt(val, fmt_str=".2f", prefix="", suffix="", na="N/A"):
    """格式化数值, 处理NaN"""
    if pd.isna(val):
        return na
    return f"{prefix}{val:{fmt_str}}{suffix}"


# ============================================================================
# 核心分析引擎
# ============================================================================
def analyze_stock(code, stock_names=None):
    """
    对单只股票进行完整的技术分析。

    返回 dict，包含所有技术指标和分析结果。
    """
    if stock_names is None:
        stock_names = load_stock_names()

    name = stock_names.get(code, code)
    df = load_stock(code)
    if df is None or len(df) < 30:
        return None

    df = df.tail(30).reset_index(drop=True)
    last = df.iloc[-1]
    prev = df.iloc[-2]

    close = last["close"]
    pct = last.get("pct_change", 0)
    volume = last["volume"]
    amplitude = last.get("amplitude", 0)
    turnover = last.get("turnover", 0)

    # 均线
    ma5 = last.get("ma5", np.nan)
    ma7 = last.get("ma7", np.nan)
    ma10 = last.get("ma10", np.nan)
    ma20 = last.get("ma20", np.nan)
    ma5_prev = prev.get("ma5", np.nan)
    ma10_prev = prev.get("ma10", np.nan)
    ma20_prev = prev.get("ma20", np.nan)

    # MACD
    dif = last.get("macd_dif", np.nan)
    dea = last.get("macd_dea", np.nan)
    macd_hist = last.get("macd_hist", np.nan)
    dif_prev = prev.get("macd_dif", np.nan)
    dea_prev = prev.get("macd_dea", np.nan)
    macd_hist_prev = prev.get("macd_hist", np.nan)

    # KDJ
    kdj_k = last.get("kdj_k", np.nan)
    kdj_d = last.get("kdj_d", np.nan)
    kdj_j = last.get("kdj_j", np.nan)
    kdj_k_prev = prev.get("kdj_k", np.nan)
    kdj_d_prev = prev.get("kdj_d", np.nan)

    # MACD状态判断
    macd_cross = _judge_cross(dif, dea, dif_prev, dea_prev)
    macd_hist_trend = _judge_hist_trend(df)

    # KDJ状态判断
    kdj_cross = _judge_cross(kdj_k, kdj_d, kdj_k_prev, kdj_d_prev)
    kdj_zone = _judge_kdj_zone(kdj_k)

    # 量比
    avg_vol_5 = df.tail(6).head(5)["volume"].mean()
    vol_ratio = volume / avg_vol_5 if avg_vol_5 > 0 else 0

    # 均线偏离
    dev_ma5 = deviation(close, ma5)
    dev_ma7 = deviation(close, ma7)
    dev_ma10 = deviation(close, ma10)
    dev_ma20 = deviation(close, ma20)

    # 均线趋势
    ma5_trend = _judge_trend(ma5, ma5_prev)
    ma10_trend = _judge_trend(ma10, ma10_prev)
    ma20_trend = _judge_trend(ma20, ma20_prev)

    # 均线排列
    ma_align = _judge_ma_align(ma5, ma10, ma20)

    # 涨幅统计
    chg_3 = (close / df.iloc[-4]["close"] - 1) * 100 if len(df) >= 4 else 0
    chg_5 = (close / df.iloc[-6]["close"] - 1) * 100 if len(df) >= 6 else 0
    chg_10 = (close / df.iloc[-11]["close"] - 1) * 100 if len(df) >= 11 else 0

    # 涨停回踩分析
    limit_up = _find_limit_up(df)

    # 评分与信号
    signals, risks, score = _evaluate_signals(
        dev_ma5=dev_ma5, dev_ma7=dev_ma7, dev_ma10=dev_ma10,
        ma_align=ma_align, ma5_trend=ma5_trend,
        macd_cross=macd_cross, macd_hist_trend=macd_hist_trend,
        macd_hist=macd_hist, macd_hist_prev=macd_hist_prev,
        kdj_cross=kdj_cross, kdj_k=kdj_k,
        vol_ratio=vol_ratio,
        limit_up_days=limit_up["days_ago"],
        pullback_pct=limit_up["pullback_pct"],
        pct=pct, chg_3=chg_3, chg_5=chg_5,
        ma5=ma5, ma5_prev=ma5_prev,
    )

    rating = _get_rating(score)

    return {
        "code": code, "name": name, "close": close,
        "pct": pct, "amplitude": amplitude, "turnover": turnover,
        "vol_ratio": vol_ratio,
        "ma5": ma5, "ma7": ma7, "ma10": ma10, "ma20": ma20,
        "dev_ma5": dev_ma5, "dev_ma7": dev_ma7,
        "dev_ma10": dev_ma10, "dev_ma20": dev_ma20,
        "ma5_trend": ma5_trend, "ma10_trend": ma10_trend, "ma20_trend": ma20_trend,
        "ma_align": ma_align,
        "dif": dif, "dea": dea, "macd_hist": macd_hist,
        "macd_cross": macd_cross, "macd_hist_trend": macd_hist_trend,
        "kdj_k": kdj_k, "kdj_d": kdj_d, "kdj_j": kdj_j,
        "kdj_cross": kdj_cross, "kdj_zone": kdj_zone,
        "chg_3d": chg_3, "chg_5d": chg_5, "chg_10d": chg_10,
        "limit_up": limit_up,
        "score": score, "signals": signals, "risks": risks, "rating": rating,
        "df_tail": df,  # 保留最后30天数据供深度分析用
    }


def _judge_cross(val1, val2, val1_prev, val2_prev):
    """判断金叉/死叉/多头/空头"""
    if not (pd.notna(val1) and pd.notna(val2) and pd.notna(val1_prev) and pd.notna(val2_prev)):
        return "未知"
    if val1_prev <= val2_prev and val1 > val2:
        return "金叉"
    if val1_prev >= val2_prev and val1 < val2:
        return "死叉"
    if val1 > val2:
        return "多头"
    return "空头"


def _judge_hist_trend(df):
    """判断MACD柱变化趋势"""
    if len(df) < 3:
        return "数据不足"
    h1 = df.iloc[-1].get("macd_hist", np.nan)
    h2 = df.iloc[-2].get("macd_hist", np.nan)
    h3 = df.iloc[-3].get("macd_hist", np.nan)
    if not (pd.notna(h1) and pd.notna(h2) and pd.notna(h3)):
        return "数据不足"
    if h1 > h2 > h3 and h1 > 0:
        return "红柱连续放大(强)"
    if h1 > h2 and h1 > 0:
        return "红柱开始放大"
    if h1 < h2 < h3 and h1 < 0:
        return "绿柱连续放大(弱)"
    if h1 < h2 and h1 < 0:
        return "绿柱开始放大"
    if h1 > 0 and h1 < h2:
        return "红柱缩短"
    if h1 < 0 and h1 > h2:
        return "绿柱缩短"
    return "震荡"


def _judge_kdj_zone(k):
    """判断KDJ所在区域"""
    if pd.isna(k):
        return "未知"
    if k > 80:
        return "超买区(>80)"
    if k > 50:
        return "强势区(50-80)"
    if k > 20:
        return "弱势区(20-50)"
    return "超卖区(<20)"


def _judge_trend(val, val_prev):
    """判断趋势方向"""
    if pd.isna(val) or pd.isna(val_prev):
        return "未知"
    if val > val_prev:
        return "上升"
    if val < val_prev:
        return "下降"
    return "走平"


def _judge_ma_align(ma5, ma10, ma20):
    """判断均线排列"""
    if not (pd.notna(ma5) and pd.notna(ma10) and pd.notna(ma20)):
        return "数据不足"
    if ma5 > ma10 > ma20:
        return "多头排列"
    if ma5 < ma10 < ma20:
        return "空头排列"
    return "交叉整理"


def _find_limit_up(df, lookback=15):
    """查找最近的涨停日"""
    result = {"days_ago": None, "close": None, "high": None,
              "pullback_pct": 0, "current_pullback": 0}
    for i in range(len(df) - 1, max(len(df) - lookback - 1, -1), -1):
        p = df.iloc[i].get("pct_change", 0)
        if pd.notna(p) and p >= 9.8:
            result["days_ago"] = len(df) - 1 - i
            result["close"] = df.iloc[i]["close"]
            result["high"] = df.iloc[i]["high"]
            if result["days_ago"] > 0:
                pullback_low = df.iloc[i + 1:]["low"].min()
                result["pullback_pct"] = (result["high"] - pullback_low) / result["high"] * 100
                result["current_pullback"] = (result["high"] - df.iloc[-1]["close"]) / result["high"] * 100
            break
    return result


def _evaluate_signals(**kw):
    """评估买入信号和风险，返回 (signals, risks, score)"""
    signals = []
    risks = []
    score = 0
    W = SCORE_WEIGHTS

    # 1. 均线支撑
    dev5 = kw["dev_ma5"]
    if pd.notna(dev5) and abs(dev5) <= 1.5:
        signals.append(f"精准回踩MA5(偏差{dev5:+.1f}%)")
        score += W["ma_support_5"]
    elif pd.notna(dev5) and abs(dev5) <= 3:
        signals.append(f"接近MA5(偏差{dev5:+.1f}%)")
        score += 1

    dev7 = kw["dev_ma7"]
    if pd.notna(dev7) and abs(dev7) <= 2:
        signals.append(f"MA7支撑(偏差{dev7:+.1f}%)")
        score += W["ma_support_7"]

    dev10 = kw["dev_ma10"]
    if pd.notna(dev10) and abs(dev10) <= 2:
        signals.append(f"回踩MA10附近(偏差{dev10:+.1f}%)")
        score += W["ma_support_10"]

    # 2. 均线排列
    if kw["ma_align"] == "多头排列":
        signals.append("均线多头排列")
        score += W["ma_bullish_align"]
    elif kw["ma_align"] == "空头排列":
        risks.append("均线空头排列，趋势偏弱")

    # 3. MACD
    mc = kw["macd_cross"]
    ht = kw["macd_hist_trend"]
    if mc == "金叉":
        signals.append("MACD金叉")
        score += W["macd_golden_cross"]
    elif "红柱连续放大" in ht or "红柱开始放大" in ht:
        signals.append(f"MACD{ht}")
        score += W["macd_red_expand"]
    elif mc == "多头":
        mh, mhp = kw["macd_hist"], kw["macd_hist_prev"]
        if pd.notna(mh) and pd.notna(mhp) and mh > mhp:
            signals.append("MACD红柱放大")
            score += W["macd_red_expand"]
        elif pd.notna(mh) and mh > 0:
            signals.append("MACD多头")
            score += W["macd_bullish"]
    elif mc == "死叉":
        risks.append("MACD刚死叉，短期转弱")
    elif mc == "空头":
        risks.append("MACD空头")

    # 4. KDJ
    kc = kw["kdj_cross"]
    kk = kw["kdj_k"]
    if kc == "金叉":
        signals.append("KDJ金叉")
        score += W["kdj_golden_cross"]
    elif kc == "多头" and pd.notna(kk):
        if kk < 80:
            signals.append(f"KDJ多头(K={kk:.0f})")
            score += W["kdj_bullish"]
        else:
            risks.append(f"KDJ超买(K={kk:.0f})，注意回调")
    elif kc == "死叉":
        risks.append("KDJ刚死叉")
    elif kc == "空头" and pd.notna(kk):
        if kk < 30:
            signals.append(f"KDJ超卖区(K={kk:.0f})，可能反弹")
            score += 2
        else:
            risks.append("KDJ空头")

    # 5. 量能
    vr = kw["vol_ratio"]
    if vr < 0.6:
        signals.append(f"极度缩量(量比{vr:.2f})，抛压极轻")
        score += W["volume_shrink_heavy"]
    elif vr < 0.8:
        signals.append(f"缩量(量比{vr:.2f})")
        score += W["volume_shrink"]
    elif vr < 1.0:
        signals.append(f"温和量(量比{vr:.2f})")
        score += W["volume_mild"]
    elif vr > 1.5:
        risks.append(f"放量过大(量比{vr:.2f})，注意是否出货")

    # 6. 涨停回踩窗口
    ld = kw["limit_up_days"]
    if ld in [3, 4]:
        signals.append(f"涨停后第{ld}天(最佳回踩窗口)")
        score += W["window_best"]
    elif ld == 5:
        signals.append("涨停后第5天")
        score += W["window_ok"]
    elif ld == 2:
        risks.append("涨停后仅第2天，回踩可能不充分")

    # 7. 回撤幅度
    pb = kw["pullback_pct"]
    if 3 <= pb <= 12:
        signals.append(f"健康回调{pb:.1f}%")
        score += W["pullback_healthy"]
    elif pb > 15:
        risks.append(f"回撤过深({pb:.1f}%)，趋势可能转弱")

    # 8. 当日状态
    p = kw["pct"]
    if -2 <= p <= 0.5:
        signals.append("调整末期(小阴小阳)")
        score += W["consolidation_end"]

    # 9. MA5向上
    ma5, ma5p = kw["ma5"], kw["ma5_prev"]
    if pd.notna(ma5) and pd.notna(ma5p) and ma5 > ma5p:
        score += W["ma5_up"]

    # 10. 短期涨幅过大风险
    c3, c5 = kw["chg_3"], kw["chg_5"]
    if c3 > 15:
        risks.append(f"3日涨幅过大({c3:.1f}%)，短期获利盘多")
    if c5 > 25:
        risks.append(f"5日涨幅过大({c5:.1f}%)，追高风险极大")

    return signals, risks, score


def _get_rating(score):
    """根据分数获取评级"""
    for threshold, label in RATING_LEVELS:
        if score >= threshold:
            return label
    return RATING_LEVELS[-1][1]


# ============================================================================
# 输出格式化
# ============================================================================
def print_batch_analysis(results, title="股票技术分析"):
    """打印批量分析结果（按评分排序）"""
    results.sort(key=lambda x: (-x["score"], -x["pct"]))

    sep = "=" * 90
    print(f"\n{sep}")
    print(f"  {title} - 按综合评分排序")
    print(f"  分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(sep)

    for i, r in enumerate(results):
        stars = _score_stars(r["score"])
        lim = r["limit_up"]["days_ago"]
        lim_str = f"{lim}天" if lim is not None else "无"

        print(f"\n  [{i+1:02d}] {r['code']} {r['name']} | 评分: {r['score']}分 {stars}")
        print(f"       {r['rating']}")
        print(f"       收盘: {r['close']:.2f} | 涨跌: {r['pct']:+.2f}% | "
              f"3日: {r['chg_3d']:+.2f}% | 5日: {r['chg_5d']:+.2f}%")
        print(f"       MA5={r['ma5']:.2f}({fmt(r['dev_ma5'],'+.1f','%')}) "
              f"MA10={r['ma10']:.2f}({fmt(r['dev_ma10'],'+.1f','%')}) "
              f"MA20={r['ma20']:.2f}({fmt(r['dev_ma20'],'+.1f','%')})")
        print(f"       MACD: {r['macd_cross']} | {r['macd_hist_trend']} | "
              f"KDJ: {r['kdj_cross']} K={fmt(r['kdj_k'],'.0f')} "
              f"D={fmt(r['kdj_d'],'.0f')} J={fmt(r['kdj_j'],'.0f')}")
        print(f"       涨停距今: {lim_str} | 回撤: {r['limit_up']['pullback_pct']:.1f}% | "
              f"量比: {r['vol_ratio']:.2f} | 换手: {r['turnover']:.2f}%")
        print(f"       信号: {' | '.join(r['signals']) if r['signals'] else '无明确信号'}")
        if r["risks"]:
            print(f"       风险: {' | '.join(r['risks'])}")

    # 汇总推荐
    print(f"\n{sep}")
    print("  【推荐汇总】")
    print(sep)

    top = [r for r in results if r["score"] >= 15]
    good = [r for r in results if 12 <= r["score"] < 15]
    watch = [r for r in results if 8 <= r["score"] < 12]
    avoid = [r for r in results if r["score"] < 8]

    if top:
        print(f"\n  >>> 强力推荐/重点关注 (>=15分):")
        for r in top:
            print(f"      {r['code']} {r['name']} - {r['score']}分 | "
                  f"涨停后第{r['limit_up']['days_ago']}天 | "
                  f"回撤{r['limit_up']['pullback_pct']:.1f}% | "
                  f"量比{r['vol_ratio']:.2f}")

    if good:
        print(f"\n  >>> 可以关注 (12-14分):")
        for r in good:
            print(f"      {r['code']} {r['name']} - {r['score']}分 | {r['pct']:+.2f}%")

    if watch:
        print(f"\n  >>> 观望为主 (8-11分):")
        for r in watch:
            print(f"      {r['code']} {r['name']} - {r['score']}分 | {r['pct']:+.2f}%")

    if avoid:
        print(f"\n  >>> 暂不建议 (<8分):")
        for r in avoid:
            print(f"      {r['code']} {r['name']} - {r['score']}分 | {r['pct']:+.2f}%")

    print()


def print_deep_analysis(r):
    """打印单只股票的深度分析"""
    sep = "=" * 95
    print(f"\n{sep}")
    print(f"  {r['code']} {r['name']} 深度分析")
    print(sep)

    # K线表
    df = r["df_tail"].tail(12).reset_index(drop=True)
    print(f"\n  {'日期':>12} {'开盘':>7} {'最高':>7} {'最低':>7} {'收盘':>7} "
          f"{'涨跌幅':>8} {'成交量':>12} {'换手率':>6} {'MA5':>7} {'MA10':>7} "
          f"{'MA20':>7} {'DIF':>7} {'DEA':>7} {'MACD柱':>8} {'K':>5} {'D':>5} {'J':>5}")
    print("  " + "-" * 92)
    for _, row in df.iterrows():
        d = str(row.get("date", ""))[:10]
        print(f"  {d:>12} {row['open']:>7.2f} {row['high']:>7.2f} {row['low']:>7.2f} "
              f"{row['close']:>7.2f} {row.get('pct_change',0):>+7.2f}% "
              f"{row['volume']:>12.0f} {row.get('turnover',0):>5.2f}% "
              f"{row.get('ma5',0):>7.2f} {row.get('ma10',0):>7.2f} {row.get('ma20',0):>7.2f} "
              f"{row.get('macd_dif',0):>7.3f} {row.get('macd_dea',0):>7.3f} "
              f"{row.get('macd_hist',0):>8.4f} {row.get('kdj_k',0):>5.1f} "
              f"{row.get('kdj_d',0):>5.1f} {row.get('kdj_j',0):>5.1f}")

    # 综合分析
    print(f"\n  --- 综合分析 ---")
    print(f"  最新收盘: {r['close']:.2f}  当日涨跌: {r['pct']:+.2f}%  "
          f"振幅: {r['amplitude']:.2f}%  换手率: {r['turnover']:.2f}%")
    print(f"  量比(相对5日均量): {r['vol_ratio']:.2f}")
    print(f"  涨幅: 3日 {r['chg_3d']:+.2f}% | 5日 {r['chg_5d']:+.2f}% | "
          f"10日 {r['chg_10d']:+.2f}%")
    print()
    print(f"  [均线] 排列: {r['ma_align']}")
    print(f"    MA5  = {r['ma5']:.2f} ({r['ma5_trend']})  偏差: {fmt(r['dev_ma5'],'+.2f','%')}"
          f"  {'<< 股价在此附近' if pd.notna(r['dev_ma5']) and abs(r['dev_ma5']) <= 2 else ''}")
    print(f"    MA7  = {r['ma7']:.2f}  偏差: {fmt(r['dev_ma7'],'+.2f','%')}"
          f"  {'<< 股价在此附近' if pd.notna(r['dev_ma7']) and abs(r['dev_ma7']) <= 2 else ''}")
    print(f"    MA10 = {r['ma10']:.2f} ({r['ma10_trend']})  偏差: {fmt(r['dev_ma10'],'+.2f','%')}"
          f"  {'<< 股价在此附近' if pd.notna(r['dev_ma10']) and abs(r['dev_ma10']) <= 2 else ''}")
    print(f"    MA20 = {r['ma20']:.2f} ({r['ma20_trend']})  偏差: {fmt(r['dev_ma20'],'+.2f','%')}")
    print()
    print(f"  [MACD] {r['macd_cross']} | {r['macd_hist_trend']}")
    print(f"    DIF={r['dif']:.4f} DEA={r['dea']:.4f} MACD柱={r['macd_hist']:.4f}")
    print()
    print(f"  [KDJ] {r['kdj_cross']} | {r['kdj_zone']}")
    print(f"    K={fmt(r['kdj_k'],'.1f')} D={fmt(r['kdj_d'],'.1f')} J={fmt(r['kdj_j'],'.1f')}")

    # 涨停回踩
    lu = r["limit_up"]
    if lu["days_ago"] is not None:
        print(f"\n  [涨停回踩]")
        print(f"    最近涨停: {lu['days_ago']}天前 (涨停价{lu['close']:.2f} 高点{lu['high']:.2f})")
        print(f"    涨停后最大回撤: {lu['pullback_pct']:.1f}%")
        print(f"    当前距涨停高点: {lu['current_pullback']:+.1f}%")

    # 综合评价
    stars = _score_stars(r["score"])
    print(f"\n  [综合评分] {r['score']}分 {stars}")
    print(f"  评级: {r['rating']}")
    print(f"  买入信号: {' | '.join(r['signals']) if r['signals'] else '无明显买入信号'}")
    print(f"  风险提示: {' | '.join(r['risks']) if r['risks'] else '暂无明显风险'}")

    # 操作建议
    print(f"\n  [操作建议]")
    if r["score"] >= 15:
        print(f"    周一可在MA5({r['ma5']:.2f})附近低吸, 止损设在MA10({r['ma10']:.2f})下方。")
        if lu["days_ago"] and lu["days_ago"] <= 4:
            print(f"    涨停后第{lu['days_ago']}天回踩, 短线反弹概率较高。")
    elif r["score"] >= 10:
        print(f"    建议先观察, 若低开回踩到均线支撑位({r['ma5']:.2f})可轻仓试探。")
    else:
        print(f"    当前技术面信号不足, 建议继续观望。")

    print(f"\n{sep}\n")


def print_comparison(r1, r2):
    """打印两只股票的对比分析"""
    sep = "=" * 100

    # K线数据
    for r in [r1, r2]:
        print(f"\n{sep}")
        print(f"  {r['code']} {r['name']} 最近10日K线")
        print(sep)
        df = r["df_tail"].tail(10).reset_index(drop=True)
        print(f"  {'日期':>12} {'开盘':>7} {'最高':>7} {'最低':>7} {'收盘':>7} "
              f"{'涨跌幅':>8} {'成交量':>12} {'换手':>6} {'MA5':>7} {'MA10':>7} "
              f"{'MA20':>7} {'K':>5} {'D':>5} {'J':>5}")
        print("  " + "-" * 90)
        for _, row in df.iterrows():
            d = str(row.get("date", ""))[:10]
            print(f"  {d:>12} {row['open']:>7.2f} {row['high']:>7.2f} {row['low']:>7.2f} "
                  f"{row['close']:>7.2f} {row.get('pct_change',0):>+7.2f}% "
                  f"{row['volume']:>12.0f} {row.get('turnover',0):>5.2f}% "
                  f"{row.get('ma5',0):>7.2f} {row.get('ma10',0):>7.2f} {row.get('ma20',0):>7.2f} "
                  f"{row.get('kdj_k',0):>5.1f} {row.get('kdj_d',0):>5.1f} {row.get('kdj_j',0):>5.1f}")

    # 多维度对比
    print(f"\n{sep}")
    print(f"  {r1['code']} {r1['name']} vs {r2['code']} {r2['name']} 多维度对比")
    print(sep)

    comparisons = [
        ("涨停距今",
         f"{r1['limit_up']['days_ago']}天" if r1['limit_up']['days_ago'] else "无",
         f"{r2['limit_up']['days_ago']}天" if r2['limit_up']['days_ago'] else "无"),
        ("涨停后回撤幅度",
         f"{r1['limit_up']['pullback_pct']:.1f}%",
         f"{r2['limit_up']['pullback_pct']:.1f}%"),
        ("最佳窗口期(3-4天)",
         "是" if r1['limit_up']['days_ago'] in [3, 4] else "否",
         "是" if r2['limit_up']['days_ago'] in [3, 4] else "否"),
        ("均线多头排列", r1['ma_align'], r2['ma_align']),
        ("回踩MA5偏差",
         fmt(r1['dev_ma5'], '+.2f', '%'), fmt(r2['dev_ma5'], '+.2f', '%')),
        ("回踩MA10偏差",
         fmt(r1['dev_ma10'], '+.2f', '%'), fmt(r2['dev_ma10'], '+.2f', '%')),
        ("量比(缩量程度)",
         f"{r1['vol_ratio']:.2f}", f"{r2['vol_ratio']:.2f}"),
        ("换手率",
         f"{r1['turnover']:.2f}%", f"{r2['turnover']:.2f}%"),
        ("MACD方向", r1['macd_cross'], r2['macd_cross']),
        ("MACD柱趋势", r1['macd_hist_trend'], r2['macd_hist_trend']),
        ("KDJ状态",
         f"{r1['kdj_cross']} K={fmt(r1['kdj_k'],'.0f')}",
         f"{r2['kdj_cross']} K={fmt(r2['kdj_k'],'.0f')}"),
        ("KDJ位置", r1['kdj_zone'], r2['kdj_zone']),
        ("近3日涨幅", f"{r1['chg_3d']:+.2f}%", f"{r2['chg_3d']:+.2f}%"),
        ("近5日涨幅", f"{r1['chg_5d']:+.2f}%", f"{r2['chg_5d']:+.2f}%"),
        ("综合评分",
         f"{r1['score']}分 {_score_stars(r1['score'])}",
         f"{r2['score']}分 {_score_stars(r2['score'])}"),
    ]

    print(f"\n  {'维度':<20} {r1['code']+' '+r1['name']:<30} {r2['code']+' '+r2['name']:<30}")
    print("  " + "-" * 80)
    for label, v1, v2 in comparisons:
        print(f"  {label:<20} {v1:<30} {v2:<30}")

    # 确定性分析
    print(f"\n{sep}")
    print(f"  【确定性分析】")
    print(sep)

    for r in [r1, r2]:
        print(f"\n  {r['code']} {r['name']} (评分{r['score']}分):")
        print(f"    优势: {' | '.join(r['signals'][:5]) if r['signals'] else '无明显信号'}")
        print(f"    劣势: {' | '.join(r['risks'][:3]) if r['risks'] else '暂无明显风险'}")

    # 最终结论
    winner = r1 if r1["score"] > r2["score"] else (r2 if r2["score"] > r1["score"] else None)
    print(f"\n  >>> 结论: ", end="")
    if winner:
        print(f"{winner['code']} {winner['name']} 确定性更高 (评分{winner['score']}分)")
        print(f"      买入逻辑: {' -> '.join(winner['signals'][:5])}")
        loser = r2 if winner == r1 else r1
        print(f"      {loser['code']} {loser['name']} 需要: {' | '.join(loser['risks'][:3]) if loser['risks'] else '进一步确认'}")
    else:
        print("两只股票确定性接近，可结合板块和个人偏好选择")

    print(f"\n{sep}\n")


def _score_stars(score):
    """评分星级"""
    if score >= 18:
        return "★★★"
    if score >= 15:
        return "★★"
    if score >= 12:
        return "★"
    if score >= 8:
        return "△"
    return "✕"


# ============================================================================
# CLI 入口
# ============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="股票技术面分析与推荐工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s --stocks 002498,000404,603711           批量分析指定股票
  %(prog)s --deep 002498                           深度分析单只股票
  %(prog)s --compare 000404,002498                 对比分析两只股票
  %(prog)s --watchlist "00涨停回踩均线0508"         按自选股分组名称分析
  %(prog)s --watchlist-all 0508                    按日期关键词获取所有分组
  %(prog)s --watchlist-all 0508 --compare-top 2    分析后对比前2名
  %(prog)s --stocks 002498,000404 --deep           批量分析+深度详情
        """,
    )
    parser.add_argument("--stocks", type=str, help="股票代码列表, 逗号分隔")
    parser.add_argument("--deep", type=str, nargs="?", const="__batch__",
                        help="深度分析; 单独使用时传入股票代码; 配合--stocks时显示所有详情")
    parser.add_argument("--compare", type=str, help="对比两只股票, 逗号分隔")
    parser.add_argument("--watchlist", type=str, help="自选股分组名称(支持模糊匹配)")
    parser.add_argument("--watchlist-all", type=str, metavar="KEYWORD",
                        help="按关键词获取所有匹配的watchlist分组")
    parser.add_argument("--compare-top", type=int, metavar="N",
                        help="配合其他模式使用, 分析后对比前N名")

    args = parser.parse_args()
    stock_names = load_stock_names()

    # 没有参数时显示帮助
    if not any([args.stocks, args.deep, args.compare, args.watchlist, args.watchlist_all]):
        parser.print_help()
        return

    # ===== 对比模式: 独立路径 =====
    if args.compare:
        c_codes = [c.strip() for c in args.compare.split(",")]
        c_codes = [resolve_stock_code(c, stock_names) for c in c_codes]
        if len(c_codes) < 2:
            print("对比分析需要两只股票, 用逗号分隔")
            return
        r1 = analyze_stock(c_codes[0], stock_names)
        r2 = analyze_stock(c_codes[1], stock_names)
        if r1 and r2:
            print_deep_analysis(r1)
            print_deep_analysis(r2)
            print_comparison(r1, r2)
        else:
            missing = [c for c in c_codes[:2] if analyze_stock(c, stock_names) is None]
            print(f"数据不足: {', '.join(missing)}")
        return

    # ===== 解析股票列表 =====
    codes = []

    if args.watchlist:
        stocks, group_name = get_stocks_by_group(args.watchlist)
        if stocks is None:
            print(f"未找到匹配 '{args.watchlist}' 的自选股分组")
            return
        codes = stocks
        print(f"  自选股分组: {group_name} ({len(stocks)}只)")

    elif args.watchlist_all:
        stocks, groups = get_stocks_by_date_keyword(args.watchlist_all)
        if not stocks:
            print(f"未找到包含 '{args.watchlist_all}' 的自选股分组")
            return
        codes = stocks
        print(f"  匹配分组: {', '.join(groups)}")
        print(f"  共 {len(stocks)} 只股票(去重)")

    elif args.deep and args.deep != "__batch__":
        codes = [resolve_stock_code(args.deep, stock_names)]

    elif args.stocks:
        codes = [c.strip() for c in args.stocks.split(",") if c.strip()]

    if not codes:
        print("未指定任何股票")
        return

    # 解析名称
    codes = [resolve_stock_code(c, stock_names) for c in codes]

    # 执行分析
    results = []
    for code in codes:
        r = analyze_stock(code, stock_names)
        if r is None:
            print(f"  警告: {code} 数据不足, 跳过")
            continue
        results.append(r)

    if not results:
        print("没有可分析的股票数据")
        return

    # ===== 输出模式 =====
    if args.deep == "__batch__" and args.stocks:
        # --stocks xxx --deep: 批量+深度
        print_batch_analysis(results, "批量分析")
        for r in results:
            print_deep_analysis(r)

    elif args.deep and args.deep != "__batch__":
        # --deep xxx: 单只深度
        print_deep_analysis(results[0])

    else:
        # 默认: 批量分析
        print_batch_analysis(results, "技术分析推荐")

    # --compare-top
    if args.compare_top and len(results) >= 2:
        results_sorted = sorted(results, key=lambda x: -x["score"])
        top_n = results_sorted[:min(args.compare_top, len(results_sorted))]
        if len(top_n) >= 2:
            print_comparison(top_n[0], top_n[1])


if __name__ == "__main__":
    main()
