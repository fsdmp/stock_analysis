#!/usr/bin/env python3.10
"""Analyze anomalous stock scores to understand what drove their extreme ratings."""

import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '/home/admin/sa/stock_analysis')

from stock_data.scoring import (
    _extract_cols, _detect_trend, _score_momentum, _score_price_position,
    _score_volume, _score_macd, _score_smart_money, _score_turnover,
    _score_ma_trend, _score_kdj, _score_sr, calc_support_resistance, calc_signals
)


def analyze_stock(code, target_date='2026-05-06'):
    """Analyze a single stock's scoring dimensions before target_date."""
    print(f"\n{'='*80}")
    print(f"STOCK {code} - Analysis for trading day BEFORE {target_date}")
    print(f"{'='*80}")

    # Load data
    df = pd.read_parquet(f'/home/admin/sa/stock_analysis/data/stocks/{code}.parquet')

    # Convert date if string
    if df['date'].dtype == 'object':
        df['date'] = pd.to_datetime(df['date'])

    # Find the row for target_date
    target_dt = pd.to_datetime(target_date)
    target_idx = None

    for i, dt in enumerate(df['date']):
        if dt >= target_dt:
            target_idx = i
            break

    if target_idx is None or target_idx < 30:
        print(f"ERROR: Not enough data for {code} (target_idx={target_idx})")
        return

    print(f"\nTarget index: {target_idx} (date: {df['date'].iloc[target_idx-1]})")

    # Show last 15 rows of data before target_date
    print(f"\n--- Last 15 rows of data BEFORE {target_date} ---")
    display_cols = ['date', 'close', 'open', 'high', 'low', 'volume', 'pct_change', 'turnover',
                    'ma5', 'ma10', 'ma20', 'kdj_k', 'kdj_d', 'kdj_j', 'macd_dif', 'macd_dea', 'macd_hist']
    available_cols = [c for c in display_cols if c in df.columns]
    print(df[available_cols].iloc[max(0, target_idx-15):target_idx].to_string())

    # Prepare columns for scoring
    n = target_idx
    cols = _extract_cols(df.iloc[:n])

    # Detect trend
    trend = _detect_trend(cols, n)
    trend_names = {2: "强势上升", 1: "上升", 0: "震荡", -1: "下降", -2: "强势下降"}
    print(f"\n--- Detected Trend: {trend_names.get(trend, 'Unknown')} ({trend}) ---")

    # Calculate key raw metrics
    print(f"\n--- Key Raw Metrics ---")
    cp = cols['close'][n-1]
    op = cols['open'][n-1]
    hi = cols['high'][n-1]
    lo = cols['low'][n-1]
    vol = cols['volume'][n-1]
    pct = cols['pct_change'][n-1]
    turnover = cols.get('turnover', [None]*n)[n-1] if 'turnover' in cols else None

    print(f"Close: {cp:.2f}, Open: {op:.2f}, High: {hi:.2f}, Low: {lo:.2f}")
    print(f"Pct Change: {pct:+.2f}%")
    print(f"Volume: {vol:,.0f}")
    if turnover:
        print(f"Turnover: {turnover:.2f}%")

    # Price position metrics
    if n >= 11:
        h10 = max(cols['close'][i] for i in range(n-10, n))
        l10 = min(cols['close'][i] for i in range(n-10, n))
        pos_10d = (cp - l10) / (h10 - l10) * 100 if h10 > l10 else 50
        dist_from_low = (cp - l10) / l10 * 100 if l10 > 0 else 0
        print(f"\n10-day High: {h10:.2f}, Low: {l10:.2f}")
        print(f"Price position in 10-day range: {pos_10d:.1f}% (0%=at low, 100%=at high)")
        print(f"Distance from 10-day low: {dist_from_low:+.2f}%")

    if n >= 6:
        h5 = max(cols['high'][i] for i in range(n-5, n))
        l5 = min(cols['low'][i] for i in range(n-5, n))
        range_5d = (h5 - l5) / l5 * 100 if l5 > 0 else 0
        dist_high_5d = (cp - h5) / h5 * 100 if h5 > 0 else 0
        print(f"\n5-day High: {h5:.2f}, Low: {l5:.2f}")
        print(f"5-day range: {range_5d:.1f}%")
        print(f"Distance from 5-day high: {dist_high_5d:+.2f}%")

    # Candlestick analysis
    body = abs(cp - op)
    us = hi - max(cp, op)
    ls = min(cp, op) - lo
    tr = hi - lo
    if tr > 0:
        us_ratio = us / tr
        ls_ratio = ls / tr
        body_ratio = body / tr
        print(f"\nCandlestick:")
        print(f"  Total Range: {tr:.2f}")
        print(f"  Body: {body:.2f} ({body_ratio*100:.1f}% of range)")
        print(f"  Upper Shadow: {us:.2f} ({us_ratio*100:.1f}% of range)")
        print(f"  Lower Shadow: {ls:.2f} ({ls_ratio*100:.1f}% of range)")

    # MA analysis
    ma5 = cols.get('ma5', [None]*n)[n-1] if 'ma5' in cols else None
    ma10 = cols.get('ma10', [None]*n)[n-1] if 'ma10' in cols else None
    ma20 = cols.get('ma20', [None]*n)[n-1] if 'ma20' in cols else None
    if ma5 and ma10 and ma20:
        print(f"\nMA5: {ma5:.2f}, MA10: {ma10:.2f}, MA20: {ma20:.2f}")
        print(f"  MA5 > MA10: {ma5 > ma10}")
        print(f"  MA10 > MA20: {ma10 > ma20}")
        print(f"  Close vs MA5: {(cp-ma5)/ma5*100:+.2f}%")
        print(f"  Close vs MA20: {(cp-ma20)/ma20*100:+.2f}%")

    # Volume analysis
    if n >= 5:
        va5 = np.mean(cols['volume'][max(0, n-5):n])
        vr = vol / va5 if va5 > 0 else 1
        print(f"\nVolume Analysis:")
        print(f"  5-day avg volume: {va5:,.0f}")
        print(f"  Volume ratio: {vr:.2f}x")

    # Now calculate dimension scores
    print(f"\n{'='*80}")
    print("DIMENSION SCORES")
    print(f"{'='*80}")

    zones = calc_support_resistance(df.iloc[:n], _cols=cols)
    signals = calc_signals(df.iloc[:n], _cols=cols)

    dimensions = [
        ("短期动量", _score_momentum, (cols, n, trend)),
        ("价格位置", _score_price_position, (cols, n, trend)),
        ("量价配合", _score_volume, (cols, n, trend)),
        ("MACD动能", _score_macd, (cols, n, trend)),
        ("主力行为", _score_smart_money, (cols, n, trend)),
        ("换手率", _score_turnover, (cols, n, trend)),
        ("MA趋势", _score_ma_trend, (cols, n, trend)),
        ("KDJ状态", _score_kdj, (cols, n, trend)),
    ]

    raw_scores = []
    for name, fn, args in dimensions:
        score, detail = fn(*args)
        raw_scores.append(score)
        print(f"\n{name}: {score:+3d} - {detail}")

    # Handle support/resistance separately
    score_sr, detail_sr = _score_sr(cols, n, zones, trend)
    raw_scores.append(score_sr)
    print(f"\n支撑压力: {score_sr:+3d} - {detail_sr}")

    print(f"\n{'='*80}")
    print(f"Raw score sum: {sum(raw_scores):+d}")
    print(f"{'='*80}")


def main():
    stocks = ['600488', '600623', '601828']

    print("ANOMALOUS STOCK SCORING ANALYSIS")
    print("="*80)
    print("\nThis script analyzes why these stocks had extreme but incorrect scores:")
    print("1. 600488 - Scored 25 (建议卖出) but next day rose +4.74%")
    print("2. 600623 - Scored 57 (偏多观望) but next day dropped -5.10%")
    print("3. 601828 - Scored 90 (最高分) but next day dropped -1.13%")

    for code in stocks:
        analyze_stock(code)

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)


if __name__ == '__main__':
    main()
