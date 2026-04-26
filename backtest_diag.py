"""轻量诊断: 只用30只股票,1个月,快速出结果."""
import pandas as pd, numpy as np, random, sys
from pathlib import Path
from datetime import datetime, timedelta
from stock_data.scoring import calc_score

random.seed(42)
files = sorted(Path('data/stocks').glob('*.parquet'))
codes = random.sample([f.stem for f in files], 30)
cutoff = (datetime(2026,4,24) - timedelta(days=30)).strftime('%Y-%m-%d')

records = []
for code in codes:
    df = pd.read_parquet(f'data/stocks/{code}.parquet')
    if len(df) < 60: continue
    df['ds'] = df['date'].astype(str).str[:10]
    indices = df[df['ds'] >= cutoff].index.tolist()[::5]
    for si in indices:
        si = int(si)
        if si + 3 >= len(df): continue
        next1 = (df['close'].iloc[si+1] / df['close'].iloc[si] - 1) * 100
        sub = df.iloc[:si+1].copy()
        if len(sub) < 30: continue
        try: r = calc_score(sub)
        except: continue
        rec = {'score': r['total'], 'ret_1d': next1,
               'pct_today': round(float(sub['pct_change'].iloc[-1] if not pd.isna(sub['pct_change'].iloc[-1]) else 0), 2),
               'close': float(sub['close'].iloc[-1]),
               'ma20': float(sub['ma20'].iloc[-1]) if not pd.isna(sub['ma20'].iloc[-1]) else None,
               'vol_ratio': float(sub['vol_ratio'].iloc[-1]) if 'vol_ratio' in sub.columns and not pd.isna(sub['vol_ratio'].iloc[-1]) else None}
        if si >= 3 and sub['close'].iloc[si-3] > 0:
            rec['cum3'] = round((sub['close'].iloc[si] / sub['close'].iloc[si-3] - 1) * 100, 2)
        if rec['ma20'] and rec['ma20'] > 0:
            rec['dev_ma20'] = round((rec['close'] - rec['ma20']) / rec['ma20'] * 100, 2)
        records.append(rec)

rdf = pd.DataFrame(records)
print(f'样本: {len(rdf)}\n')

print('=== 今日涨幅 vs 次日收益 ===')
for lo, hi in [(0,1),(1,3),(3,5),(5,9.5),(9.5,100)]:
    sub = rdf[(rdf['pct_today'] >= lo) & (rdf['pct_today'] < hi)]
    if len(sub) < 3: continue
    print(f'  今日{lo:+.1f}%~{hi:+.1f}%: {len(sub):>3d}条  次日均收={sub["ret_1d"].mean():+.2f}%  胜率={((sub["ret_1d"]>0).mean()*100):.1f}%')

print('\n=== 距MA20偏离 vs 次日收益 ===')
for lo, hi, label in [(-100,-5,'深跌'),(-5,-2,'偏低'),(-2,2,'正常'),(2,5,'偏高'),(5,100,'严重偏高')]:
    sub = rdf[(rdf['dev_ma20'] >= lo) & (rdf['dev_ma20'] < hi)]
    if len(sub) < 3: continue
    print(f'  {label}: {len(sub):>3d}条  次日均收={sub["ret_1d"].mean():+.2f}%  胜率={((sub["ret_1d"]>0).mean()*100):.1f}%')

print('\n=== 3日累计涨幅 vs 次日收益 ===')
for lo, hi in [(-100,-5),(-5,-2),(-2,2),(2,5),(5,10),(10,100)]:
    sub = rdf[(rdf['cum3'] >= lo) & (rdf['cum3'] < hi)]
    if len(sub) < 3: continue
    print(f'  3日{lo:+.0f}%~{hi:+.0f}%: {len(sub):>3d}条  次日均收={sub["ret_1d"].mean():+.2f}%  胜率={((sub["ret_1d"]>0).mean()*100):.1f}%')

print('\n=== 高分(78+) vs 中分(63-78) vs 低分(<40) ===')
for label, sub in [('78+', rdf[rdf['score']>=78]), ('63-78', rdf[(rdf['score']>=63)&(rdf['score']<78)]), ('<40', rdf[rdf['score']<40])]:
    if len(sub) < 3: continue
    print(f'  {label}: {len(sub):>3d}条  今日涨={sub["pct_today"].mean():+.2f}%  3日涨={sub["cum3"].dropna().mean():+.2f}%  距MA20={sub["dev_ma20"].dropna().mean():+.2f}%  次日={sub["ret_1d"].mean():+.2f}%  胜率={((sub["ret_1d"]>0).mean()*100):.1f}%')
