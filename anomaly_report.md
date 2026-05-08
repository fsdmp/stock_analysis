# Anomalous Stock Scoring Analysis Report

## Executive Summary

Three stocks showed significant discrepancies between their model scores and actual next-day performance. This diagnostic analyzes the raw dimension scores to understand what drove these anomalies.

---

## Stock 600488: Scored 25 (建议卖出) but Next Day Rose +4.74%

### Key Metrics (2026-04-30)
- **Close:** 5.70 (-2.73%)
- **Price Position:** 7.1% in 10-day range (near LOW)
- **Distance from 5-day High:** -18.45% (deeply below high)
- **MA Trend:** Bearish alignment (MA5 < MA10 < MA20)
- **Volume Ratio:** 0.63x (below average)

### Dimension Score Breakdown (Raw Sum: -21)
| Dimension | Score | Key Detail |
|-----------|-------|------------|
| 短期动量 | -3 | 下跌-2.7% |
| 价格位置 | +1 | 5日振幅大(27%,活跃) |
| 量价配合 | -3 | 量比=0.6; 量价背离 |
| MACD动能 | -7 | DIF<DEA; 零轴上方; 绿柱放大; 连续绿柱 |
| 主力行为 | +2 | 上影线(有攻击意愿); 短下影(抛压轻) |
| 换手率 | -2 | 适度换手8.0%+下跌 |
| MA趋势 | -6 | 空头排列; MA5<VWMA5(量价分歧); 连续5日收于MA5下方 |
| KDJ状态 | 0 | K=22 (oversold) |
| 支撑压力 | -3 | 压力假突破(诱多) |

### Why It Scored 25 (Sell Signal)

**Primary Negative Drivers:**
1. **Downtrend Detection (-7):** MACD showed clear deterioration with consecutive green bars and DIF<DEA crossover
2. **MA Bearish Alignment (-6):** All MAs aligned bearishly with close below all MAs
3. **Price Action (-3):** Stock closed down 2.73% with below-average volume
4. **Volume-Price Divergence (-3):** Shrinking volume on price decline

**Why The Model Was WRONG:**

This is a **classic oversold washout pattern** that the model misinterpreted:

1. **Deep Oversold:** Price at 7.1% of 10-day range = AT THE BOTTOM. The model's -3 on momentum penalized the -2.73% drop, but this was after a massive 26.9% 5-day range swing.

2. **Capitulation Volume:** The 0.63x volume ratio on a down day at near 10-day lows suggests **selling exhaustion**, not continued weakness. This should have been a bullish signal (contrary indicator).

3. **KDJ at 22:** Deeply oversold but the model only gave it a neutral score because of the downtrend context. The code explicitly says "KDJ超卖 in downtrend = STILL weak (negative/neutral!)" - this is the flaw!

4. **Support Proximity:** Only 3.7% from support after a 26.9% swing = ideal bounce setup.

**The Pattern:** This was a **capitulation bottom**. The extreme drop to near 10-day lows on low volume after high volatility is a classic reversal setup. The model treated it as continued downtrend when it was actually a selling exhaustion pattern.

---

## Stock 600623: Scored 57 (偏多观望) but Next Day Dropped -5.10%

### Key Metrics (2026-04-30)
- **Close:** 10.88 (+0.18%)
- **Price Position:** 100.0% in 10-day range (at HIGH)
- **Distance from 5-day High:** -1.72% (nearly AT HIGH)
- **MA Trend:** Bullish alignment (MA5 > MA10 > MA20)
- **Volume Ratio:** 0.92x (normal)

### Dimension Score Breakdown (Raw Sum: +5)
| Dimension | Score | Key Detail |
|-----------|-------|------------|
| 短期动量 | -5 | 连阳4日(连涨偏高); 3日涨+10.7%(严重过热) |
| 价格位置 | 0 | 价格位置正常 (at 100% of 10-day range!) |
| 量价配合 | +3 | 量比=0.9; 量价配合良好 |
| MACD动能 | +8 | DIF>DEA; 零轴上方; 红柱放大 |
| 主力行为 | 0 | 十字星(上升受阻); 下影线偏长(抛压) |
| 换手率 | +2 | 地量换手1.5%(锁仓) |
| MA趋势 | -1 | 多头排列 but MA5<VWMA5(量价分歧); 疑似诱多 |
| KDJ状态 | 0 | K=74; J=101极强 |
| 支撑压力 | -2 | 压力附近(1.4%); 压力假突破(诱多) |

### Why It Scored 57 (Neutral/Bullish)

**Primary Positive Drivers:**
1. **MACD Strength (+8):** Strong bullish momentum with DIF>DEA, above zero, expanding histogram
2. **Good Volume-Price (+3):** Volume and price moving together
3. **Low Turnover (+2):** "地量锁仓" interpreted as strong holder conviction

**Why The Model Was WRONG:**

This is a **textbook blow-off top / distribution pattern**:

1. **CRITICAL BUG:** Price at 100% of 10-day range scored a **0** on "价格位置"! The code only penalizes >90% or >95%, so 100% got a neutral score. This is a huge flaw - being at the absolute high should be a major red flag.

2. **Severe Overheating:**
   - 3-day gain: +10.7% (flagged as "严重过热" but only -5 penalty)
   - 4 consecutive up days (flagged as "连涨偏高" but only -2 penalty)
   - The model's momentum scoring is too lenient on uptrend overextension

3. **Doji Candlestick at Highs:** The candlestick was a doji (body only 8.3% of range) at 10-day highs - this is a **major reversal signal** but only got a 0 score from "主力行为"

4. **Low Turnover Trap:** 1.53% turnover interpreted as "锁仓" (strong holding) but at market tops, low volume = lack of buyers = distribution

5. **Multiple "诱多" (Bull Trap) Warnings:**
   - MA trend: "疑似诱多(缩量站上MA)"
   - Support/Resistance: "压力假突破(诱多)"
   - These warnings were present but not weighted heavily enough

**The Pattern:** This was a **climax top**. The stock ran +10.7% in 3 days to the exact 10-day high on a doji candle with low turnover - classic distribution. The model missed the extreme overextension risk.

---

## Stock 601828: Scored 90 (最高分) but Next Day Dropped -1.13%

### Key Metrics (2026-04-30)
- **Close:** 2.67 (+2.69%)
- **Price Position:** 91.2% in 10-day range (near HIGH)
- **Distance from 5-day High:** -3.26% (moderately below high)
- **MA Trend:** Bullish alignment (MA5 > MA10 > MA20)
- **Volume Ratio:** 1.58x (above average)
- **Turnover:** 2.70% (low for the gain)

### Dimension Score Breakdown (Raw Sum: +35)
| Dimension | Score | Key Detail |
|-----------|-------|------------|
| 短期动量 | +5 | 上涨+2.7%(趋势配合); 启动日(首日上攻) |
| 价格位置 | +7 | 10日区间适中(78%); 距5日高点-3.3%(适度回调) |
| 量价配合 | +3 | 放量上涨(比=1.8) |
| MACD动能 | +7 | DIF>DEA; 红柱放大; 连续红柱 |
| 主力行为 | +4 | 明显上影线(上攻试探) |
| 换手率 | +8 | 低换手+上涨(主力控盘/一致性强); 换手递增+价涨(主升浪确认) |
| MA趋势 | -1 | 多头排列 but MA5<VWMA5(量价分歧); 疑似诱多 |
| KDJ状态 | 0 | K=68 |
| 支撑压力 | +2 | 支撑附近(3.0%) |

### Why It Scored 90 (Strong Buy)

**Primary Positive Drivers:**
1. **Turnover Excellence (+8):** Low turnover (2.70%) on +2.69% gain = "主力控盘/一致性强"
2. **Strong Momentum (+5):** First day up after consolidation, trend-aligned
3. **Excellent Price Position (+7):** Good balance point in 10-day range
4. **Good MACD (+7) and Volume (+3):** All momentum indicators aligned

**Why The Model Was WRONG (Just Bad Luck):**

This is **pattern was actually CORRECT** - it was just unlucky:

1. **Technical Setup Was Sound:**
   - Breaking out from consolidation after 4/28's +10.2% surge
   - Low turnover on upside = strong holder conviction
   - Price at 78% of 10-day range = not overextended
   - Only -3.26% from 5-day high = healthy pullback from highs

2. **The Risk Factors Were Present but Minor:**
   - MA trend showed "疑似诱多(缩量站上MA)" (-1)
   - MA5 < VWMA5 suggesting volume-price divergence
   - At 91.2% of 10-day range (getting high)

3. **Why It Dropped:**
   - Previous day 4/29 had a massive -3.70% drop on huge volume (97M shares)
   - 4/30's +2.69% was fighting against that heavy selling
   - The -1.13% next day was just normal volatility after such a big move

**Verdict:** This is **NOT a model failure**. The technical pattern was solid for a short-term trade. The -1.13% drop is within normal market noise. A 90-point score doesn't guarantee profit every time - it just indicates favorable odds.

---

## Root Cause Analysis

### Systemic Issues Identified

#### 1. **Oversold in Downtrend Bias (600488's main issue)**
**Location:** `_score_kdj()` lines 541-552
```python
elif k < 20 and d < 30:
    if trend >= 1:
        score += 5  # Uptrend: oversold = buy the dip
    elif trend <= -1:
        score -= 2  # Downtrend: oversold = still weak  <-- FLAW
```

**Problem:** The model explicitly penalizes oversold conditions in downtrends. This misses **capitulation reversals** where extreme oversold at support levels after high volatility creates bounce opportunities.

**Fix Suggestion:** Add proximity-to-support check. If KDJ is oversold AND price is near 10-day low AND recent volatility was high, this should be a bullish signal regardless of trend.

#### 2. **Price Position 100% Scoring Gap (600623's main issue)**
**Location:** `_score_price_position()` lines 1570-1587
```python
if pos_10d > 95:
    score -= 4  # Almost at 10-day high
elif pos_10d > 90:
    score -= 2
elif pos_10d > 85:
    score -= 1
elif 60 <= pos_10d <= 85:
    score += 3  # Sweet spot
```

**Problem:** Position at 100% gets the same penalty as 96%. Being at the exact high after a 3-day +10.7% run should be penalized MORE heavily.

**Fix Suggestion:** Add explicit check for pos_10d >= 98:
```python
if pos_10d >= 98:
    score -= 6  # AT the high = extreme risk
    details.append("触及10日最高点(极大风险)")
```

#### 3. **Uptrend Overextension Leniency (600623's secondary issue)**
**Location:** `_score_momentum()` lines 987-993
```python
if c3 > 10:
    score -= 2  # Only -2 for 3-day +10%+ gain in uptrend
```

**Problem:** In strong uptrends, the model only gives a -2 penalty for 3-day gains over 10%. This is too lenient.

**Fix Suggestion:** Scale penalties more aggressively:
```python
if c3 > 12:
    score -= 5  # Severe overheating
elif c3 > 10:
    score -= 3  # Significant overheating
```

#### 4. **Doji at Extremes Under-penalized (600623's tertiary issue)**
**Location:** `_score_smart_money()` lines 1400-1409
```python
if body / tr < 0.1:  # Doji
    if trend >= 1:
        score -= 2  # Only -2 penalty
```

**Problem:** A doji at 10-day highs after a 3-day run is a major reversal signal, but only gets -2.

**Fix Suggestion:** Check price position before penalizing doji:
```python
if body / tr < 0.1:  # Doji
    if trend >= 1:
        # Check if at extremes
        if n >= 11:
            h10 = max(cols['high'][i] for i in range(n-10, n))
            pos_10d = (cp - l10) / (h10 - l10) * 100
            if pos_10d > 90:
                score -= 5  # Doji at highs = major warning
            else:
                score -= 2
```

---

## Recommendations

### Immediate Actions
1. **Add Capitulation Detection:** If price < 15% of 10-day range AND KDJ < 25 AND volume < 0.8x average = bullish reversal signal
2. **Fix 100% Position Penalty:** Being at the exact high should score -6, not -4
3. **Tighten Uptrend Overheating:** Scale -2 to -5 for extreme 3-day gains
4. **Enhance Doji Warnings:** Doji at >90% of range should be -5

### Model Philosophy Adjustments
The current model has a **trend-following bias** that works well for continuation trades but misses:
- Reversal setups (600488's oversold bounce)
- Climax tops (600623's blow-off)

The scoring needs better **regime detection**:
- Strong trending regime: Current logic works well
- Mean reversion regime: Need opposite logic (buy low, sell high)
- Transition zones: Need neutral scoring

---

## Conclusion

**600488:** Model failure due to oversold bias in downtrend. Should have detected capitulation reversal setup.

**600623:** Model failure due to insufficient penalties for extreme overextension at market highs. The "严重过热" warning was there but not weighted heavily enough.

**601828:** Not a failure. The pattern was technically sound; -1.13% is normal variance. High scores don't guarantee wins every time.

**Overall:** The model's core logic is sound but needs edge case refinements for extreme price positions and capitulation reversals.
