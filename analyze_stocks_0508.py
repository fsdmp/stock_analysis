#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析自选0508的42只股票的技术指标
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("请先安装必要的库: pip3 install pandas pyarrow numpy")
    sys.exit(1)


# 配置
DATA_DIR = Path("/home/admin/sa/stock_analysis/data/stocks")
STOCK_NAMES_FILE = Path("/home/admin/sa/stock_analysis/data/stock_names.json")

# 股票列表
GROUP1 = [
    "000551", "000657", "000692", "000716", "000831", "000901", "001336", "001396",
    "002079", "002111", "002136", "002174", "002348", "002354", "002498", "002522",
    "002786", "002851", "002875", "002969"
]

GROUP2 = [
    "600052", "600158", "600322", "600540", "601086", "601106", "601588", "603030",
    "603119", "603131", "603178", "603190", "603375", "603687", "603698", "603711",
    "603717", "603897", "605123", "605222", "605376", "605389"
]

ALL_STOCKS = GROUP1 + GROUP2


def load_stock_names() -> Dict[str, str]:
    """加载股票名称映射"""
    try:
        with open(STOCK_NAMES_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"加载股票名称失败: {e}")
        return {}


def load_stock_data(code: str) -> pd.DataFrame:
    """加载单个股票数据"""
    path = DATA_DIR / f"{code}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"数据文件不存在: {path}")

    df = pd.read_parquet(path)
    df['date'] = pd.to_datetime(df['date'])
    return df.sort_values('date').reset_index(drop=True)


def calc_ma(df: pd.DataFrame, periods: List[int]) -> pd.DataFrame:
    """计算均线"""
    for p in periods:
        df[f'ma{p}'] = df['close'].rolling(window=p, min_periods=1).mean()
        df[f'v_ma{p}'] = df['volume'].rolling(window=p, min_periods=1).mean()
    return df


def calc_ema(series: pd.Series, span: int) -> pd.Series:
    """计算指数移动平均"""
    return series.ewm(span=span, adjust=False).mean()


def calc_macd(df: pd.DataFrame) -> pd.DataFrame:
    """计算MACD"""
    ema_fast = calc_ema(df['close'], 12)
    ema_slow = calc_ema(df['close'], 26)

    df['macd_dif'] = ema_fast - ema_slow
    df['macd_dea'] = calc_ema(df['macd_dif'], 9)
    df['macd_hist'] = 2 * (df['macd_dif'] - df['macd_dea'])
    return df


def calc_kdj(df: pd.DataFrame) -> pd.DataFrame:
    """计算KDJ"""
    low_n = df['low'].rolling(window=9, min_periods=1).min()
    high_n = df['high'].rolling(window=9, min_periods=1).max()

    rsv = ((df['close'] - low_n) / (high_n - low_n) * 100).fillna(50)

    k = np.zeros(len(df))
    d = np.zeros(len(df))
    k[0] = 50
    d[0] = 50

    for i in range(1, len(df)):
        k[i] = (2/3) * k[i-1] + (1/3) * rsv.iloc[i]
        d[i] = (2/3) * d[i-1] + (1/3) * k[i]

    df['kdj_k'] = k
    df['kdj_d'] = d
    df['kdj_j'] = 3 * k - 2 * d
    return df


def calc_bollinger(df: pd.DataFrame) -> pd.DataFrame:
    """计算布林带"""
    ma = df['close'].rolling(window=20, min_periods=1).mean()
    std = df['close'].rolling(window=20, min_periods=1).std()

    df['bb_upper'] = ma + 2 * std
    df['bb_middle'] = ma
    df['bb_lower'] = ma - 2 * std
    return df


def get_trend(values: pd.Series, days: int = 3) -> str:
    """判断趋势"""
    if len(values) < days:
        return "未知"

    recent = values.tail(days).values
    if all(recent[i] < recent[i+1] for i in range(len(recent)-1)):
        return "上升"
    elif all(recent[i] > recent[i+1] for i in range(len(recent)-1)):
        return "下降"
    else:
        return "走平"


def get_price_position(price: float, ma: float, threshold: float = 0.02) -> str:
    """判断价格相对于均线的位置"""
    if ma == 0:
        return "未知"

    ratio = (price - ma) / ma
    if ratio > threshold:
        return "上方"
    elif ratio < -threshold:
        return "下方"
    else:
        return "附近"


def analyze_stock(code: str, stock_names: Dict[str, str]) -> Dict:
    """分析单个股票"""
    try:
        df = load_stock_data(code)
    except FileNotFoundError as e:
        return {"error": str(e), "code": code}

    if len(df) < 20:
        return {"error": "数据不足", "code": code}

    # 计算技术指标
    df = calc_ma(df, [5, 7, 10, 20])
    df = calc_macd(df)
    df = calc_kdj(df)
    df = calc_bollinger(df)

    # 计算涨跌幅和振幅
    df['pct_change'] = df['close'].pct_change() * 100
    df['amplitude'] = ((df['high'] - df['low']) / df['close'].shift(1) * 100)

    # 计算量比
    df['volume_ratio'] = df['volume'] / df['v_ma5']

    # 获取最近数据
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else latest
    latest_3 = df.tail(3)
    latest_5 = df.tail(5)

    # 计算近3日和5日涨幅
    gain_3d = ((latest['close'] - df.iloc[-4]['close']) / df.iloc[-4]['close'] * 100) if len(df) >= 4 else 0
    gain_5d = ((latest['close'] - df.iloc[-6]['close']) / df.iloc[-6]['close'] * 100) if len(df) >= 6 else 0

    # 判断MACD金叉/死叉
    macd_signal = ""
    if latest['macd_dif'] > latest['macd_dea'] and prev['macd_dif'] <= prev['macd_dea']:
        macd_signal = "金叉"
    elif latest['macd_dif'] < latest['macd_dea'] and prev['macd_dif'] >= prev['macd_dea']:
        macd_signal = "死叉"
    elif latest['macd_dif'] > latest['macd_dea']:
        macd_signal = "多头"
    else:
        macd_signal = "空头"

    # 判断KDJ金叉/死叉
    kdj_signal = ""
    if latest['kdj_k'] > latest['kdj_d'] and prev['kdj_k'] <= prev['kdj_d']:
        kdj_signal = "金叉"
    elif latest['kdj_k'] < latest['kdj_d'] and prev['kdj_k'] >= prev['kdj_d']:
        kdj_signal = "死叉"
    elif latest['kdj_k'] > latest['kdj_d']:
        kdj_signal = "多头"
    else:
        kdj_signal = "空头"

    # 判断成交量
    volume_status = "放量" if latest['volume_ratio'] > 1.2 else ("缩量" if latest['volume_ratio'] < 0.8 else "平量")

    # 判断是否回踩均线
    ma_touch = []
    for ma_period in [5, 10, 20]:
        pos = get_price_position(latest['close'], latest[f'ma{ma_period}'], 0.015)
        if pos == "附近":
            ma_touch.append(f"MA{ma_period}")

    # 获取股票名称
    name = stock_names.get(code, "未知")

    result = {
        "code": code,
        "name": name,
        "date": str(latest['date'].date()),
        "close": round(latest['close'], 2),
        "pct_change": round(latest['pct_change'], 2),
        "volume": int(latest['volume']),
        "volume_ratio": round(latest['volume_ratio'], 2),
        "volume_status": volume_status,
        "amplitude": round(latest['amplitude'], 2) if not pd.isna(latest['amplitude']) else 0,
        "ma5": round(latest['ma5'], 2),
        "ma7": round(latest['ma7'], 2),
        "ma10": round(latest['ma10'], 2),
        "ma20": round(latest['ma20'], 2),
        "ma5_trend": get_trend(latest_5['ma5']),
        "ma10_trend": get_trend(latest_5['ma10']),
        "ma20_trend": get_trend(latest_5['ma20']),
        "ma5_pos": get_price_position(latest['close'], latest['ma5']),
        "ma10_pos": get_price_position(latest['close'], latest['ma10']),
        "ma20_pos": get_price_position(latest['close'], latest['ma20']),
        "macd_dif": round(latest['macd_dif'], 4),
        "macd_dea": round(latest['macd_dea'], 4),
        "macd_hist": round(latest['macd_hist'], 4),
        "macd_signal": macd_signal,
        "kdj_k": round(latest['kdj_k'], 2),
        "kdj_d": round(latest['kdj_d'], 2),
        "kdj_j": round(latest['kdj_j'], 2),
        "kdj_signal": kdj_signal,
        "bb_upper": round(latest['bb_upper'], 2),
        "bb_middle": round(latest['bb_middle'], 2),
        "bb_lower": round(latest['bb_lower'], 2),
        "ma_touch": ", ".join(ma_touch) if ma_touch else "",
        "gain_3d": round(gain_3d, 2),
        "gain_5d": round(gain_5d, 2),
    }

    return result


def print_summary(results: List[Dict]):
    """打印汇总表格"""
    print("\n" + "="*150)
    print(f"{'代码':<6} {'名称':<8} {'日期':<12} {'收盘价':<8} {'涨跌幅%':<8} {'成交量':<10} {'量比':<6} {'振幅%':<6} {'MA5':<8} {'MA10':<8} {'MA20':<8} {'MACD':<12} {'KDJ':<12} {'信号':<20}")
    print("="*150)

    # 按涨跌幅排序
    sorted_results = sorted(results, key=lambda x: x.get('pct_change', 0), reverse=True)

    for r in sorted_results:
        if 'error' in r:
            print(f"{r['code']:<6} {'错误':<8} {r['error']}")
            continue

        macd_info = f"DIF:{r['macd_dif']:.3f} {r['macd_signal']}"
        kdj_info = f"K:{r['kdj_k']:.1f} {r['kdj_signal']}"

        signals = []
        if r['ma_touch']:
            signals.append(f"回踩{r['ma_touch']}")
        if r['macd_signal'] == '金叉':
            signals.append("MACD金叉")
        if r['kdj_signal'] == '金叉':
            signals.append("KDJ金叉")
        if r['volume_status'] == '放量':
            signals.append("放量")

        signal_str = ", ".join(signals) if signals else "无明显信号"

        print(f"{r['code']:<6} {r['name']:<8} {r['date']:<12} {r['close']:<8.2f} {r['pct_change']:>7.2f}% {r['volume']:<10,} {r['volume_ratio']:>5.2f} {r['amplitude']:>5.2f}% {r['ma5']:<8.2f} {r['ma10']:<8.2f} {r['ma20']:<8.2f} {macd_info:<12} {kdj_info:<12} {signal_str:<20}")

    print("="*150)


def print_detailed_analysis(results: List[Dict]):
    """打印详细分析"""
    print("\n\n" + "="*150)
    print("详细技术分析")
    print("="*150)

    # 按涨跌幅排序
    sorted_results = sorted(results, key=lambda x: x.get('pct_change', 0), reverse=True)

    for i, r in enumerate(sorted_results, 1):
        if 'error' in r:
            continue

        print(f"\n【{i}】{r['code']} - {r['name']} ({r['date']})")
        print(f"  {'─'*100}")
        print(f"  价格信息: 收盘价 {r['close']:.2f} | 涨跌幅 {r['pct_change']:+.2f}% | 振幅 {r['amplitude']:.2f}%")
        print(f"  成交量: {r['volume']:,} | 量比 {r['volume_ratio']:.2f} ({r['volume_status']})")
        print(f"  近期涨幅: 近3日 {r['gain_3d']:+.2f}% | 近5日 {r['gain_5d']:+.2f}%")
        print()
        print(f"  均线系统:")
        print(f"    MA5:  {r['ma5']:.2f} ({r['ma5_trend']}) - 股价位置: {r['ma5_pos']}")
        print(f"    MA7:  {r['ma7']:.2f}")
        print(f"    MA10: {r['ma10']:.2f} ({r['ma10_trend']}) - 股价位置: {r['ma10_pos']}")
        print(f"    MA20: {r['ma20']:.2f} ({r['ma20_trend']}) - 股价位置: {r['ma20_pos']}")
        if r['ma_touch']:
            print(f"    回踩情况: {r['ma_touch']}")
        print()
        print(f"  MACD指标: DIF={r['macd_dif']:.4f} | DEA={r['macd_dea']:.4f} | MACD柱={r['macd_hist']:.4f} ({r['macd_signal']})")
        print(f"  KDJ指标:  K={r['kdj_j']:.1f} | D={r['kdj_d']:.1f} | J={r['kdj_j']:.1f} ({r['kdj_signal']})")
        print()
        print(f"  布林带: 上轨={r['bb_upper']:.2f} | 中轨={r['bb_middle']:.2f} | 下轨={r['bb_lower']:.2f}")
        print(f"  收盘价位置: {r['bb_lower']:.2f} < {r['close']:.2f} < {r['bb_upper']:.2f}")


def print_group_summary(results: List[Dict], group_name: str, group_codes: List[str]):
    """打印分组汇总"""
    print(f"\n\n{'='*150}")
    print(f"{group_name} - 技术面汇总")
    print(f"{'='*150}")

    group_results = [r for r in results if r.get('code') in group_codes and 'error' not in r]

    if not group_results:
        print("无有效数据")
        return

    # 统计
    rising = len([r for r in group_results if r['pct_change'] > 0])
    falling = len([r for r in group_results if r['pct_change'] < 0])
    avg_change = sum(r['pct_change'] for r in group_results) / len(group_results)

    macd_gold = len([r for r in group_results if r['macd_signal'] == '金叉'])
    kdj_gold = len([r for r in group_results if r['kdj_signal'] == '金叉'])
    volume_heavy = len([r for r in group_results if r['volume_status'] == '放量'])

    ma5_above = len([r for r in group_results if r['ma5_pos'] == '上方'])
    ma5_near = len([r for r in group_results if r['ma5_pos'] == '附近'])
    ma5_touch = len([r for r in group_results if 'MA5' in r['ma_touch']])

    print(f"  总数: {len(group_results)} 只")
    print(f"  涨跌: 上涨 {rising} 只 | 下跌 {falling} 只 | 平均涨跌幅 {avg_change:+.2f}%")
    print()
    print(f"  技术信号:")
    print(f"    MACD金叉: {macd_gold} 只 | KDJ金叉: {kdj_gold} 只")
    print(f"    放量: {volume_heavy} 只")
    print()
    print(f"  均线位置:")
    print(f"    MA5上方: {ma5_above} 只 | MA5附近: {ma5_near} 只 | 回踩MA5: {ma5_touch} 只")

    # 列出强势股票
    top_gainers = sorted(group_results, key=lambda x: x['pct_change'], reverse=True)[:5]
    print(f"\n  涨幅前5:")
    for r in top_gainers:
        print(f"    {r['code']} - {r['name']}: {r['pct_change']:+.2f}% ({r['close']:.2f}元)")

    # 列出有金叉的股票
    gold_cross = [r for r in group_results if r['macd_signal'] == '金叉' or r['kdj_signal'] == '金叉']
    if gold_cross:
        print(f"\n  出现金叉的股票:")
        for r in gold_cross:
            signals = []
            if r['macd_signal'] == '金叉':
                signals.append("MACD金叉")
            if r['kdj_signal'] == '金叉':
                signals.append("KDJ金叉")
            print(f"    {r['code']} - {r['name']}: {', '.join(signals)}, 涨跌幅 {r['pct_change']:+.2f}%")


def main():
    """主函数"""
    print("开始分析自选0508股票...")
    print(f"总计: {len(ALL_STOCKS)} 只股票")

    # 加载股票名称
    stock_names = load_stock_names()
    print(f"已加载 {len(stock_names)} 个股票名称映射")

    # 分析所有股票
    results = []
    for i, code in enumerate(ALL_STOCKS, 1):
        print(f"\r处理进度: {i}/{len(ALL_STOCKS)} - {code}", end='', flush=True)
        result = analyze_stock(code, stock_names)
        results.append(result)

    print(f"\r处理完成! {' '*50}")

    # 打印结果
    print_summary(results)
    print_detailed_analysis(results)

    # 打印分组汇总
    print_group_summary(results, "00涨停回踩均线0508", GROUP1)
    print_group_summary(results, "60涨停回踩均线0508", GROUP2)

    # 整体汇总
    print(f"\n\n{'='*150}")
    print("整体汇总 - 所有42只股票")
    print(f"{'='*150}")

    all_valid = [r for r in results if 'error' not in r]
    if all_valid:
        rising = len([r for r in all_valid if r['pct_change'] > 0])
        falling = len([r for r in all_valid if r['pct_change'] < 0])
        flat = len([r for r in all_valid if r['pct_change'] == 0])
        avg_change = sum(r['pct_change'] for r in all_valid) / len(all_valid)

        print(f"  有效数据: {len(all_valid)} 只")
        print(f"  涨跌分布: 上涨 {rising} 只 | 下跌 {falling} 只 | 平盘 {flat} 只")
        print(f"  平均涨跌幅: {avg_change:+.2f}%")
        print()

        # 推荐关注
        recommended = []
        for r in all_valid:
            score = 0
            reasons = []

            # 金叉加分
            if r['macd_signal'] == '金叉':
                score += 3
                reasons.append("MACD金叉")
            if r['kdj_signal'] == '金叉':
                score += 2
                reasons.append("KDJ金叉")

            # 回踩均线加分
            if r['ma_touch']:
                score += 2
                reasons.append(f"回踩{r['ma_touch']}")

            # 放量加分
            if r['volume_status'] == '放量':
                score += 1
                reasons.append("放量")

            # 均线趋势加分
            if r['ma5_trend'] == '上升' and r['ma10_trend'] == '上升':
                score += 2
                reasons.append("均线多头")

            # 股价在均线上方加分
            if r['ma5_pos'] == '上方' and r['ma10_pos'] == '上方':
                score += 1
                reasons.append("站稳均线")

            if score >= 5:
                recommended.append((r, score, reasons))

        recommended.sort(key=lambda x: x[1], reverse=True)

        if recommended:
            print(f"\n  【推荐关注】(技术面强势):")
            for r, score, reasons in recommended[:10]:
                print(f"    {r['code']} - {r['name']}: {r['close']:.2f}元 ({r['pct_change']:+.2f}%)")
                print(f"      理由: {', '.join(reasons)} | MA5:{r['ma5']:.2f} MA10:{r['ma10']:.2f} MA20:{r['ma20']:.2f}")

    # 保存结果到JSON
    output_file = Path("/home/admin/sa/stock_analysis/analysis_result_0508.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n\n分析结果已保存至: {output_file}")


if __name__ == "__main__":
    main()
