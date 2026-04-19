"""Next-day trading score engine for short-term / ultra-short-term decisions.

Core principles:
  1. Trend context determines indicator interpretation
     - KDJ overbought in uptrend = strong momentum, NOT a sell signal
     - KDJ oversold in downtrend = weakness, NOT a buy signal
  2. Short-term focused (1-3 day horizon)
     - Weight momentum and volume-price heavily
     - MA20 is already "long-term" for short-term traders
  3. Aggressive scoring for differentiation
     - tanh normalization spreads scores, avoids clustering at 50

Dimensions (weighted for short-term):
  1. 短期动量  (20)  - most important for 1-3 day decisions
  2. 量价配合  (18)  - validates all moves
  3. MA趋势   (12)  - with VWMA anti-trap
  4. 主力行为  (12)  - smart money detection
  5. MACD动能  (10)  - useful but lagging
  6. KDJ状态   (10)  - trend-contextualized
  7. 支撑压力   (8)  - near-term levels
  8. 背离信号   (5)  - divergence
  9. 均线形态   (3)  - setup indicator
 10. 趋势确认   (2)  - meta filter
"""

import numpy as np
import pandas as pd

from stock_data.analysis import calc_support_resistance, calc_signals


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _v(val) -> bool:
    return val is not None and np.isfinite(val)


def _safe(df, idx, col):
    if idx < 0 or idx >= len(df):
        return None
    val = df.iloc[idx].get(col)
    return val if _v(val) else None


def _clamp(x, lo=-10, hi=10):
    return max(lo, min(hi, x))


# ---------------------------------------------------------------------------
# Trend Detection  (short-term, 3-10 day window)
# ---------------------------------------------------------------------------

def _detect_trend(df, n):
    """Detect short-term trend for indicator contextualization.

    Returns: +2 strong up, +1 up, 0 sideways, -1 down, -2 strong down
    """
    close = df["close"].values
    vol = df["volume"].fillna(0).values.astype(np.float64)
    ma5 = _safe(df, n - 1, "ma5")
    ma10 = _safe(df, n - 1, "ma10")

    sc = 0

    # 1. 3-day cumulative momentum (one factor, not dominant)
    if n >= 4 and close[n - 4] > 0:
        cum3 = (close[n - 1] / close[n - 4] - 1) * 100
        if cum3 > 6:
            sc += 1.5
        elif cum3 > 3:
            sc += 1
        elif cum3 > 0:
            sc += 0.5
        elif cum3 < -6:
            sc -= 1.5
        elif cum3 < -3:
            sc -= 1
        elif cum3 < 0:
            sc -= 0.5

    # 6. Price position vs 10-day low (how far extended from support)
    if n >= 11:
        low10 = min(close[n - 11:n])
        if low10 > 0:
            dist_from_low = (close[n - 1] - low10) / low10 * 100
            if dist_from_low > 12:
                sc += 1
            elif dist_from_low > 6:
                sc += 0.5
            elif dist_from_low < -5:
                sc -= 1
            elif dist_from_low < 0:
                sc -= 0.5

    # 2. Price vs MA5
    if _v(ma5) and ma5 > 0:
        dev5 = (close[n - 1] - ma5) / ma5 * 100
        if dev5 > 2:
            sc += 1
        elif dev5 > 0:
            sc += 0.5
        elif dev5 < -2:
            sc -= 1
        elif dev5 < 0:
            sc -= 0.5

    # 3. MA5 vs MA10
    if _v(ma5) and _v(ma10) and ma10 > 0:
        if ma5 > ma10 * 1.005:
            sc += 1
        elif ma5 < ma10 * 0.995:
            sc -= 1

    # 4. MA5 slope (3-day)
    if n >= 4:
        ma5_3d = _safe(df, n - 4, "ma5")
        if _v(ma5_3d) and ma5_3d > 0 and _v(ma5):
            slope = (ma5 - ma5_3d) / ma5_3d * 100
            if slope > 1.5:
                sc += 1
            elif slope < -1.5:
                sc -= 1

    # 5. Volume confirms direction
    if n >= 5:
        rv = np.mean(vol[n - 3:n])
        pv = np.mean(vol[max(0, n - 6):n - 3])
        if pv > 0:
            vr = rv / pv
            if vr > 1.5 and close[n - 1] > close[n - 4]:
                sc += 1
            elif vr > 1.5 and close[n - 1] < close[n - 4]:
                sc -= 1

    if sc >= 5:
        return 2
    if sc >= 2:
        return 1
    if sc <= -5:
        return -2
    if sc <= -2:
        return -1
    return 0


# ---------------------------------------------------------------------------
# MA break quality & close confirmation (used by _score_ma_trend)
# ---------------------------------------------------------------------------

def _eval_ma_break_quality(df, n):
    """评估最近MA突破的质量.

    返回 -5 ~ +5 的质量分:
    - 放量突破 + 实体饱满 = 高质量
    - 缩量穿越 + 长影线 = 低质量 / 诱多诱空
    """
    if n < 5:
        return 0

    cp = df["close"].values[n - 1]
    op = df["open"].values[n - 1]
    hi = df["high"].values[n - 1]
    lo = df["low"].values[n - 1]
    vol = df["volume"].fillna(0).values.astype(np.float64)

    ma5 = _safe(df, n - 1, "ma5")
    ma5_prev = _safe(df, n - 2, "ma5")
    if not _v(ma5) or not _v(ma5_prev):
        return 0

    prev_close = df["close"].values[n - 2]
    broke_up = prev_close <= ma5_prev and cp > ma5
    broke_down = prev_close >= ma5_prev and cp < ma5

    if not broke_up and not broke_down:
        return 0

    direction = 1 if broke_up else -1
    quality = 0

    # Volume ratio
    va5 = np.mean(vol[max(0, n - 6):n - 1])
    if va5 > 0:
        vr = vol[n - 1] / va5
        if vr >= 2.0:
            quality += 2
        elif vr >= 1.3:
            quality += 1
        elif vr < 0.7:
            quality -= 3
        else:
            quality -= 1

    # Body ratio
    total_range = hi - lo
    if total_range > 0:
        body = abs(cp - op)
        body_ratio = body / total_range
        if body_ratio > 0.7:
            quality += 1
        elif body_ratio < 0.3:
            quality -= 2

    # Penetration depth
    if _v(ma5) and ma5 > 0:
        penetration = abs(cp - ma5) / ma5 * 100
        if penetration > 2:
            quality += 1
        elif penetration < 0.3:
            quality -= 2

    return _clamp(direction * quality, -5, 5)


def _check_ma_close_confirmation(df, n):
    """检查连续收盘站稳MA5的确认机制.

    返回正数=连续站稳上方天数, 负数=连续收于下方天数, 0=刚穿越或不满足.
    """
    if n < 3:
        return 0

    ma5_col, close_col = [], []
    for i in range(max(0, n - 5), n):
        m = _safe(df, i, "ma5")
        c = df["close"].values[i]
        if not _v(m):
            return 0
        ma5_col.append(m)
        close_col.append(c)

    last_above = close_col[-1] > ma5_col[-1]
    streak = 0
    for i in range(len(close_col) - 1, -1, -1):
        above = close_col[i] > ma5_col[i]
        if above == last_above:
            streak += 1
        else:
            break

    if streak >= 2:
        return streak if last_above else -streak
    return 0


# ---------------------------------------------------------------------------
# Dimension scorers  (each returns score:int, detail:str)
# All receive `trend` parameter for contextual interpretation
# ---------------------------------------------------------------------------

def _score_ma_trend(df, n, trend):
    """MA排列 + VWMA对抗诱多诱空 + 突破质量 + BB上下文.

    Trend context:
    - Uptrend: MA break noise capped, negative scores limited
    - Downtrend: MA bounce signals capped, positive scores limited
    """
    score, details = 0, []
    cp = df["close"].values[n - 1]
    ma5 = _safe(df, n - 1, "ma5")
    ma10 = _safe(df, n - 1, "ma10")
    ma20 = _safe(df, n - 1, "ma20")

    if not all(_v(x) for x in [ma5, ma10, ma20]):
        return 0, "均线数据不足"

    vwma5 = _safe(df, n - 1, "vwma5")
    vwma10 = _safe(df, n - 1, "vwma10")
    vwma20 = _safe(df, n - 1, "vwma20")
    bb_bw = _safe(df, n - 1, "bb_bandwidth")
    bb_upper = _safe(df, n - 1, "bb_upper")
    bb_lower = _safe(df, n - 1, "bb_lower")

    # 1. MA alignment
    if ma5 > ma10 > ma20:
        score += 4
        details.append("多头排列")
    elif ma5 < ma10 < ma20:
        score -= 4
        details.append("空头排列")
    elif ma5 > ma10 and ma10 < ma20:
        score += 1
        details.append("短期金叉待确认")
    elif ma5 < ma10 and ma10 > ma20:
        score -= 1
        details.append("短期死叉待确认")
    else:
        details.append("均线纠缠")

    # 2. VWMA vs MA 诱多诱空
    has_vwma = all(_v(x) for x in [vwma5, vwma10, vwma20])
    if has_vwma:
        if ma5 > vwma5:
            score += 2
            details.append("MA5>VWMA5(量能支撑)")
        elif ma5 < vwma5:
            score -= 2
            details.append("MA5<VWMA5(量价分歧)")
        if ma20 > vwma20:
            score += 1
        elif ma20 < vwma20:
            score -= 1
            details.append("MA20<VWMA20(量能不足)")
        if cp > ma5 and ma5 < vwma5:
            score -= 3
            details.append("疑似诱多(缩量站上MA)")
        elif cp < ma5 and ma5 > vwma5:
            score += 3
            details.append("疑似诱空(缩量跌破MA)")

    # 3. Break quality
    break_score = _eval_ma_break_quality(df, n)
    if break_score != 0:
        score += break_score
        details.append(f"突破质量{'+' if break_score > 0 else ''}{break_score}")

    # 4. BB context
    if _v(bb_bw):
        if bb_bw < 3:
            score += 1 if score > 0 else -1
            details.append("BB收敛(变盘在即)")
        elif bb_bw > 8:
            score = int(score * 0.6)
            details.append("BB宽幅震荡(MA信号打折)")
        if _v(bb_upper) and _v(bb_lower) and bb_upper > bb_lower:
            bb_pos = (cp - bb_lower) / (bb_upper - bb_lower) * 100
            if bb_pos > 85:
                score -= 2
                details.append(f"BB上轨({bb_pos:.0f}%)")
            elif bb_pos < 15:
                score += 2
                details.append(f"BB下轨({bb_pos:.0f}%)")

    # 5. Close confirmation
    confirm = _check_ma_close_confirmation(df, n)
    if confirm > 0:
        score += 2
        details.append(f"连续{confirm}日站稳MA5上方")
    elif confirm < 0:
        score -= 2
        details.append(f"连续{abs(confirm)}日收于MA5下方")

    # 6. Trend context adjustment
    if trend >= 1 and score < -3:
        score = -3
        details.append("上升趋势(MA信号下限)")
    elif trend <= -1 and score > 3:
        score = 3
        details.append("下跌趋势(MA信号上限)")

    return _clamp(score), "; ".join(details)


def _score_macd(df, n, trend):
    """MACD动能 with trend context.

    - Uptrend + MACD turning up from below zero = strong reversal
    - Downtrend + MACD turning down from above zero = danger
    """
    score, details = 0, []
    dif = _safe(df, n - 1, "macd_dif")
    dea = _safe(df, n - 1, "macd_dea")
    hist = _safe(df, n - 1, "macd_hist")
    prev_hist = _safe(df, n - 2, "macd_hist") if n >= 2 else None

    if not all(_v(x) for x in [dif, dea, hist]):
        return 0, "MACD数据不足"

    # DIF vs DEA
    if dif > dea:
        score += 3
        details.append("DIF>DEA")
    else:
        score -= 3
        details.append("DIF<DEA")

    # Zero line
    if dif > 0 and dea > 0:
        score += 2
        details.append("零轴上方")
    elif dif < 0 and dea < 0:
        score -= 2
        details.append("零轴下方")
    else:
        details.append("零轴附近")

    # Histogram change
    if _v(prev_hist):
        if hist > 0 and hist > prev_hist:
            score += 3
            details.append("红柱放大")
        elif hist > 0 and hist < prev_hist:
            if trend >= 1:
                score += 1
                details.append("红柱缩短(上升整理)")
            else:
                score -= 1
                details.append("红柱缩短")
        elif hist < 0 and hist < prev_hist:
            score -= 3
            details.append("绿柱放大")
        elif hist < 0 and hist > prev_hist:
            if trend <= -1:
                score -= 1
                details.append("绿柱缩短(下跌中继)")
            else:
                score += 1
                details.append("绿柱缩短")
    else:
        score += 1 if hist > 0 else -1

    # Trend-specific signals
    if trend >= 1 and dif < 0 and _v(prev_hist) and hist > prev_hist:
        score += 2
        details.append("上升趋势MACD回升")
    elif trend <= -1 and dif > 0 and _v(prev_hist) and hist < prev_hist:
        score -= 2
        details.append("下跌趋势MACD转弱")

    # Consecutive bars
    if n >= 5:
        hists = [_safe(df, n - 1 - i, "macd_hist") for i in range(5)]
        if all(_v(h) and h > 0 for h in hists):
            score += 1
            details.append("连续红柱")
        elif all(_v(h) and h < 0 for h in hists):
            score -= 1
            details.append("连续绿柱")

    return _clamp(score), "; ".join(details)


def _score_kdj(df, n, trend):
    """KDJ with trend-contextualized interpretation.

    KEY: KDJ超买 in uptrend = STRONG momentum (positive!)
         KDJ超卖 in downtrend = STILL weak (negative/neutral!)
    """
    score, details = 0, []
    k = _safe(df, n - 1, "kdj_k")
    d = _safe(df, n - 1, "kdj_d")
    j = _safe(df, n - 1, "kdj_j")
    pk = _safe(df, n - 2, "kdj_k") if n >= 2 else None
    pd = _safe(df, n - 2, "kdj_d") if n >= 2 else None

    if not all(_v(x) for x in [k, d, j]):
        return 0, "KDJ数据不足"

    # === Overbought / oversold with TREND CONTEXT ===
    if k > 80 and d > 70:
        if trend >= 1:
            # Uptrend: overbought = strong momentum
            score += 4
            details.append("强势区(趋势配合)")
            # Stagnation check - even in uptrend, extended OB is risky
            if n >= 5:
                ob = sum(1 for i in range(5)
                         if _v(_safe(df, n - 1 - i, "kdj_k"))
                         and _safe(df, n - 1 - i, "kdj_k") > 80
                         and _v(_safe(df, n - 1 - i, "kdj_d"))
                         and _safe(df, n - 1 - i, "kdj_d") > 70)
                if ob >= 5:
                    score -= 2
                    details.append("高位钝化(注意风险)")
        elif trend <= -1:
            # Downtrend: overbought = trap
            score -= 4
            details.append("超买(反弹陷阱)")
        else:
            score -= 2
            details.append("超买区")

    elif k < 20 and d < 30:
        if trend >= 1:
            # Uptrend: oversold = buy the dip
            score += 5
            details.append("回调节买点(趋势向上)")
        elif trend <= -1:
            # Downtrend: oversold = still weak
            score -= 2
            details.append("超卖(趋势偏弱)")
        else:
            score += 3
            details.append("超卖区")

    elif 40 <= k <= 60:
        details.append("中位震荡")
    else:
        details.append(f"K={k:.0f}")

    # === Golden / death cross with trend ===
    if _v(pk) and _v(pd):
        if pk <= pd and k > d:
            if trend >= 1:
                score += 5
                details.append("金叉(趋势确认)")
            elif trend <= -1:
                score += 1
                details.append("金叉(趋势偏弱)")
            else:
                score += 4
                details.append("金叉")
        elif pk >= pd and k < d:
            if trend >= 1:
                score -= 1
                details.append("死叉(趋势尚可)")
            elif trend <= -1:
                score -= 5
                details.append("死叉(趋势确认)")
            else:
                score -= 4
                details.append("死叉")

    # === J extremes ===
    if j > 100:
        if trend >= 1:
            details.append(f"J={j:.0f}极强")
        else:
            score -= 2
            details.append(f"J={j:.0f}极高")
    elif j < 0:
        if trend <= -1:
            details.append(f"J={j:.0f}极弱")
        else:
            score += 2
            details.append(f"J={j:.0f}极低")

    return _clamp(score), "; ".join(details)


def _score_volume(df, n, trend):
    """量价配合 with trend context.

    - Uptrend + shrinking volume + price up = controlled rise (bullish)
    - Downtrend + heavy volume + small gain = distribution (bearish)
    """
    score, details = 0, []
    vol = df["volume"].fillna(0).values.astype(np.float64)
    close = df["close"].values
    v = vol[n - 1]

    if v <= 0:
        return 0, "停牌或无成交"

    va5 = np.mean(vol[max(0, n - 5):n - 1]) if n >= 2 else v
    if va5 <= 0:
        va5 = v
    ratio5 = v / va5
    pct = _safe(df, n - 1, "pct_change")
    pv = float(pct) if _v(pct) else 0

    # Volume ratio scoring
    if ratio5 >= 3:
        if pv > 2:
            score += 5
            details.append(f"放量上涨(比={ratio5:.1f})")
            if trend <= -1:
                score -= 2
                details.append("下跌趋势放量反弹(警惕)")
        elif pv < -2:
            score -= 5
            details.append(f"放量下跌(比={ratio5:.1f})")
            if trend >= 1:
                score += 2
                details.append("上升趋势放量下跌(洗盘?)")
        else:
            score -= 3
            details.append(f"放量滞涨(比={ratio5:.1f})")
    elif ratio5 >= 1.5:
        if pv > 1:
            score += 3
            details.append(f"放量上涨(比={ratio5:.1f})")
        elif pv < -1:
            score -= 3
            details.append(f"放量下跌(比={ratio5:.1f})")
        else:
            details.append(f"温和放量(比={ratio5:.1f})")
    elif ratio5 <= 0.5:
        if pv > 0:
            if trend >= 1:
                score += 3
                details.append("缩量上涨(主力控盘)")
            else:
                score += 1
                details.append(f"缩量上涨(比={ratio5:.1f})")
        else:
            if trend <= -1:
                score -= 1
                details.append("缩量下跌(无人接盘)")
            elif trend >= 1:
                score += 2
                details.append("缩量回调(洗盘)")
            else:
                details.append(f"地量(比={ratio5:.1f})")
                if n >= 30:
                    is_v = all((vol[j] == 0 or vol[j] >= v) for j in range(max(0, n - 30), n - 1))
                    if is_v:
                        score += 3
                        details.append("30日地量")
    else:
        details.append(f"量比={ratio5:.1f}")

    # Volume-price consistency (3-day)
    if n >= 3:
        vp = 0
        for i in range(n - 3, n):
            if i < 1:
                continue
            vi, vip = vol[i], vol[i - 1]
            ci, cip = close[i], close[i - 1]
            if vip > 0 and cip > 0:
                vu = vi > vip
                pu = ci > cip
                if vu and pu:
                    vp += 2
                elif not vu and not pu:
                    vp -= 1
                elif vu and not pu:
                    vp -= 3
                else:
                    vp += 1
        if vp >= 3:
            score += 3
            details.append("量价配合良好")
        elif vp <= -3:
            score -= 3
            details.append("量价背离")

    return _clamp(score), "; ".join(details)


def _score_sr(df, n, zones, trend):
    """支撑压力 with trend context.

    - Uptrend near support = strong buy signal
    - Downtrend no support = dangerous
    - Uptrend no resistance = clear sky
    """
    score, details = 0, []
    cp = df["close"].values[n - 1]

    if not zones:
        return 0, "无有效支撑压力"

    nearest_sup, nearest_res = None, None
    sup_dist, res_dist = float("inf"), float("inf")

    for z in zones.get("support", []):
        if z.get("status") == "broken":
            continue
        mid = (z["low"] + z["high"]) / 2
        dist = cp - mid
        if dist >= 0 and dist < sup_dist:
            sup_dist, nearest_sup = dist, z

    for z in zones.get("resistance", []):
        if z.get("status") == "broken":
            continue
        mid = (z["low"] + z["high"]) / 2
        dist = mid - cp
        if dist >= 0 and dist < res_dist:
            res_dist, nearest_res = dist, z

    atr = _calc_atr(df, n, 14)
    if atr <= 0:
        atr = cp * 0.02

    # Support
    if nearest_sup:
        sp = sup_dist / cp * 100
        if sp < 1:
            score += 5 if trend >= 1 else 3
            details.append(f"紧贴支撑{'[强势确认]' if trend >= 1 else ''}")
        elif sp < 3:
            score += 2
            details.append(f"支撑附近({sp:.1f}%)")
        else:
            details.append(f"距支撑{sp:.1f}%")
        if nearest_sup.get("status") == "trap":
            score += 3
            details.append("支撑假突破(诱空)")
    else:
        score -= 3 if trend <= -1 else 1
        details.append("下方无支撑" + ("(下跌趋势)" if trend <= -1 else ""))

    # Resistance
    if nearest_res:
        rp = res_dist / cp * 100
        if rp < 1:
            score -= 2 if trend >= 1 else 5
            details.append(f"临近压力{'[上升趋势受阻]' if trend >= 1 else ''}")
        elif rp < 3:
            score -= 2
            details.append(f"压力附近({rp:.1f}%)")
        else:
            details.append(f"距压力{rp:.1f}%")
        if nearest_res.get("status") == "trap":
            score -= 3
            details.append("压力假突破(诱多)")
    else:
        score += 3 if trend >= 1 else 1
        details.append("上方无压力" + ("(上升空间)" if trend >= 1 else ""))

    return _clamp(score), "; ".join(details)


def _score_squeeze(df, n, trend):
    """均线粘合 + trend-biased direction."""
    score, details = 0, []
    ma5 = _safe(df, n - 1, "ma5")
    ma10 = _safe(df, n - 1, "ma10")
    ma20 = _safe(df, n - 1, "ma20")
    cp = df["close"].values[n - 1]

    if not all(_v(x) for x in [ma5, ma10, ma20]):
        return 0, "数据不足"

    spread = (max(ma5, ma10, ma20) - min(ma5, ma10, ma20)) / cp * 100

    if spread >= 1.5:
        details.append(f"均线分散({spread:.1f}%)")
        return _clamp(score), "; ".join(details)

    if spread < 0.5:
        details.append("高度粘合")
    else:
        details.append(f"均线收敛({spread:.1f}%)")

    bull = ma5 > ma10 > ma20
    bear = ma5 < ma10 < ma20

    if bull:
        score += 7 if trend >= 1 else 3
        details.append("多头粘合" + ("(趋势配合)" if trend >= 1 else ""))
    elif bear:
        score -= 7 if trend <= -1 else 3
        details.append("空头粘合" + ("(趋势配合)" if trend <= -1 else ""))
    else:
        if trend >= 1:
            score += 3
            details.append("上升趋势粘合(偏多)")
        elif trend <= -1:
            score -= 3
            details.append("下跌趋势粘合(偏空)")
        else:
            hist = _safe(df, n - 1, "macd_hist")
            if _v(hist):
                if hist > 0.02:
                    score += 2
                    details.append("MACD多头")
                elif hist < -0.02:
                    score -= 2
                    details.append("MACD空头")

    center = (ma5 + ma10 + ma20) / 3
    dev = (cp - center) / cp * 100
    if dev > 0.5:
        score += 2
    elif dev < -0.5:
        score -= 2

    return _clamp(score), "; ".join(details)


def _score_momentum(df, n, trend):
    """短期动量 (CORE dimension for short-term).

    Trend context:
    - Uptrend + pullback = buying opportunity
    - Downtrend + bounce = selling opportunity
    - Strong momentum aligned with trend = AMPLIFIED
    """
    score, details = 0, []
    close = df["close"].values
    pct = df["pct_change"].fillna(0).values if "pct_change" in df.columns else np.zeros(n)
    cp = close[n - 1]

    # === Today's change (most important) ===
    tp = float(pct[n - 1]) if n > 0 else 0

    if tp > 5:
        if trend <= -1:
            score += 2
            details.append(f"反弹{tp:+.1f}%(趋势偏弱)")
        else:
            score += 7
            details.append(f"大涨{tp:+.1f}%")
    elif tp > 3:
        if trend >= 1:
            score += 6
            details.append(f"强势上涨{tp:+.1f}%")
        elif trend <= -1:
            score += 1
            details.append(f"反弹{tp:+.1f}%")
        else:
            score += 4
            details.append(f"上涨{tp:+.1f}%")
    elif tp > 1:
        if trend >= 1:
            score += 4
            details.append(f"上涨{tp:+.1f}%")
        elif trend <= -1:
            score += 0
            details.append(f"微涨{tp:+.1f}%(趋势偏弱)")
        else:
            score += 2
            details.append(f"上涨{tp:+.1f}%")
    elif tp > 0:
        score += 1
        details.append(f"微涨{tp:+.1f}%")
    elif tp > -1:
        score -= 1
        details.append(f"微跌{tp:+.1f}%")
    elif tp > -3:
        if trend >= 1:
            score -= 1
            details.append(f"回调{tp:+.1f}%(买点?)")
        elif trend <= -1:
            score -= 4
            details.append(f"下跌{tp:+.1f}%")
        else:
            score -= 2
            details.append(f"下跌{tp:+.1f}%")
    elif tp > -5:
        if trend >= 1:
            score -= 2
            details.append(f"回调{tp:+.1f}%")
        else:
            score -= 6
            details.append(f"下跌{tp:+.1f}%")
    else:
        score -= 3 if trend >= 1 else 8
        details.append(f"急跌{tp:+.1f}%" if trend >= 1 else f"大跌{tp:+.1f}%")

    # Limit up/down
    if tp >= 9.5:
        score += 3
        details.append("接近涨停")
    elif tp <= -9.5:
        score -= 3
        details.append("接近跌停")

    # Consecutive up/down
    streak = 0
    for i in range(n - 1, max(n - 10, 0), -1):
        if close[i] > close[i - 1]:
            if streak >= 0:
                streak += 1
            else:
                break
        elif close[i] < close[i - 1]:
            if streak <= 0:
                streak -= 1
            else:
                break
        else:
            break
    if streak >= 5:
        score -= 1 if trend >= 1 else 0
        details.append(f"连阳{streak}日(连涨过多)")
    elif streak >= 3:
        score += 0 if trend >= 1 else 0
        details.append(f"连阳{streak}日(偏高)")
    elif streak == 2:
        score += 2 if trend >= 1 else 1
        details.append("连阳2日(趋势确立中)")
    elif streak == 1:
        score += 1
        details.append("首日上涨(启动)")
    if streak <= -5:
        score += 0 if trend <= -1 else 1
        details.append(f"连阴{abs(streak)}日(超跌)")
    elif streak <= -3:
        score -= 3 if trend <= -1 else 1
        details.append(f"连阴{abs(streak)}日" + ("(趋势配合)" if trend <= -1 else ""))

    # 3-day cumulative (trend-contextualized: penalize chasing, reward pullbacks)
    if n >= 4 and close[n - 4] > 0:
        c3 = (close[n - 1] / close[n - 4] - 1) * 100
        if trend >= 1:  # Uptrend: prefer mild/pullback entries
            if c3 > 10:
                score -= 1
                details.append(f"3日涨{c3:+.1f}%(追高风险)")
            elif c3 > 7:
                score += 0
                details.append(f"3日涨{c3:+.1f}%(偏高)")
            elif c3 > 3:
                score += 1
                details.append(f"3日涨{c3:+.1f}%(健康动量)")
            elif c3 > 0:
                score += 2
                details.append(f"3日涨{c3:+.1f}%(温和启动)")
            elif c3 > -3:
                score += 2
                details.append(f"3日微调{c3:+.1f}%(回调买入)")
            elif c3 > -5:
                score += 3
                details.append(f"3日回调{c3:+.1f}%(买点)")
            else:
                score += 1
                details.append(f"3日深调{c3:+.1f}%(趋势待确认)")
        elif trend <= -1:  # Downtrend: defensive logic
            if c3 > 10:
                score -= 3
                details.append(f"3日涨幅{c3:+.1f}%过热")
            elif c3 > 5:
                score += 1
                details.append(f"3日反弹{c3:+.1f}%")
            elif c3 > 0:
                score += 1
            elif c3 < -10:
                score -= 2
                details.append(f"3日跌{c3:+.1f}%(弱势)")
            elif c3 < -5:
                score -= 2
                details.append(f"3日跌{c3:+.1f}%")
            else:
                if c3 < 0:
                    score -= 1
        else:  # Sideways
            if c3 > 7:
                score += 0
                details.append(f"3日涨{c3:+.1f}%(突破待确认)")
            elif c3 > 3:
                score += 1
                details.append(f"3日涨{c3:+.1f}%")
            elif c3 > 0:
                score += 1
            elif c3 > -5:
                score -= 1
            else:
                score += 1
                details.append(f"3日跌{c3:+.1f}%(区间超跌)")

    # Distance from 10-day low (position safety)
    if n >= 11:
        low10 = min(close[n - 11:n])
        if low10 > 0:
            dist_from_low = (cp - low10) / low10 * 100
            if trend >= 1:  # uptrend
                if dist_from_low < 3:
                    score += 2
                    details.append("贴近10日低点(安全边际高)")
                elif dist_from_low < 7:
                    score += 1
                elif dist_from_low > 15:
                    score -= 2
                    details.append(f"远离10日低点{dist_from_low:.0f}%(追高风险)")
                elif dist_from_low > 10:
                    score -= 1
                    details.append("偏离10日低点较远")

    # Today's initiative (first-day move vs continuation)
    if n >= 3:
        yesterday_pct = float(pct[n - 2]) if n >= 2 else 0
        if tp > 2 and yesterday_pct <= 0:
            score += 2
            details.append("启动日(首日上攻)")
        elif tp > 2 and yesterday_pct > 3:
            score -= 1
            details.append("连续加速(获利盘压力)")

    # 5-day cumulative (penalize overextension even in uptrend)
    if n >= 6 and close[n - 6] > 0:
        c5 = (close[n - 1] / close[n - 6] - 1) * 100
        if c5 > 15:
            score -= 2
            details.append(f"5日涨幅{c5:+.1f}%(偏高超买)")
        elif c5 > 10 and trend >= 1:
            score -= 1
            details.append(f"5日涨{c5:+.1f}%(偏高)")
        if c5 < -15:
            if trend >= 1:
                score += 3
                details.append(f"5日深调{c5:+.1f}%(抄底)")

    # Amplitude
    hi = df["high"].values[n - 1]
    lo = df["low"].values[n - 1]
    if cp > 0:
        amp = (hi - lo) / cp * 100
        if amp > 7 and close[n - 1] < df["open"].values[n - 1]:
            score -= 2 if trend <= -1 else 1
            details.append("高振幅收阴")

    return _clamp(score), "; ".join(details)


def _score_divergence(df, n, signals, trend):
    """背离信号 with trend context.

    - Top divergence against uptrend: warning (significant)
    - Bottom divergence with uptrend: continuation signal
    """
    score, details = 0, []
    recent_range = 10
    date_strs = [str(d)[:10] for d in df["date"].values]
    last_date = date_strs[n - 1] if n > 0 else ""

    def _is_recent(sig_list):
        for s in sig_list:
            if s.get("d") and last_date:
                try:
                    from datetime import datetime
                    sd = datetime.strptime(s["d"], "%Y-%m-%d")
                    ld = datetime.strptime(last_date, "%Y-%m-%d")
                    if (ld - sd).days <= recent_range:
                        return True, s
                except Exception:
                    pass
        return False, None

    found, s = _is_recent(signals.get("macdDiv", []))
    if found:
        if s["g"] == 1:
            score -= 5 if trend >= 1 else 7
            details.append("MACD顶背离" + ("(注意风险)" if trend >= 1 else "(趋势确认)"))
        else:
            if trend <= -1:
                score += 5
                details.append("MACD底背离(关注反转)")
            elif trend >= 1:
                score += 3
                details.append("MACD底背离(趋势支撑)")
            else:
                score += 6
                details.append("MACD底背离")

    found, s = _is_recent(signals.get("volPrice", []))
    if found:
        if s["g"] == 1:
            score -= 5
            details.append("量价顶背离")
        else:
            score += 5
            details.append("量价底背离")

    if not details:
        details.append("无明显背离")

    return _clamp(score), "; ".join(details)


def _score_smart_money(df, n, trend):
    """主力行为 with trend context."""
    score, details = 0, []
    close = df["close"].values
    open_ = df["open"].values
    high = df["high"].values
    low = df["low"].values
    vol = df["volume"].fillna(0).values.astype(np.float64)

    if n < 5:
        return 0, "数据不足"

    cp, op = close[n - 1], open_[n - 1]
    hi, lo = high[n - 1], low[n - 1]
    body = abs(cp - op)
    us = hi - max(cp, op)
    ls = min(cp, op) - lo
    tr = hi - lo

    if tr <= 0:
        return 0, "无波动"

    # Long upper shadow
    if us > body * 2 and us > ls * 2:
        score -= 4 if trend >= 1 else 2
        details.append("长上影线" + ("(高位抛压)" if trend >= 1 else "(诱多嫌疑)"))

    # Long lower shadow
    if ls > body * 2 and ls > us * 2:
        if trend >= 1:
            score += 4
            details.append("长下影线(支撑确认)")
        elif trend <= -1:
            score += 1
            details.append("长下影线(抵抗)")
        else:
            score += 2
            details.append("长下影线")

    # Doji
    if body / tr < 0.1:
        if trend >= 1:
            score -= 2
            details.append("十字星(上升受阻)")
        elif trend <= -1:
            score += 2
            details.append("十字星(下跌减速)")
        else:
            score -= 1
            details.append("十字星(方向不明)")

    # Engulfing
    if n >= 2:
        pb = abs(close[n - 2] - open_[n - 2])
        if cp > op and close[n - 2] < open_[n - 2] and body > pb * 1.2:
            score += 4 if trend >= 1 else 2
            details.append("阳包阴" + ("(趋势确认)" if trend >= 1 else ""))
        elif cp < op and close[n - 2] > open_[n - 2] and body > pb * 1.2:
            score -= 4 if trend <= -1 else 2
            details.append("阴包阳" + ("(趋势确认)" if trend <= -1 else ""))

    # Volume-price anomalies
    va5 = np.mean(vol[max(0, n - 6):n - 1])
    if va5 > 0:
        vr = vol[n - 1] / va5
        pvv = float(_safe(df, n - 1, "pct_change") or 0)
        if vr > 2 and abs(pvv) < 1:
            score -= 5 if trend >= 1 else 3
            details.append("放量滞涨" + ("(高位出货)" if trend >= 1 else "(主力出货)"))
        elif vr < 0.6 and pvv > 3:
            score += 3 if trend >= 1 else 1
            details.append("缩量大涨" + ("(主力控盘)" if trend >= 1 else ""))
        elif vr < 0.6 and pvv < -3:
            score -= 3 if trend <= -1 else -1
            if trend <= -1:
                details.append("缩量下跌(恐慌不足)")
            else:
                details.append("缩量下跌(洗盘?)")

    # Tail manipulation
    pvv = float(_safe(df, n - 1, "pct_change") or 0)
    if (hi - cp) / tr < 0.15 and pvv < 2:
        score -= 3 if trend >= 1 else 1
        details.append("疑似拉尾" + ("(诱多)" if trend >= 1 else ""))
    elif (cp - lo) / tr < 0.15 and pvv > -2:
        score += 2 if trend <= -1 else 1
        details.append("疑似砸尾" + ("(加速下跌)" if trend <= -1 else "(洗盘)"))

    if not details:
        details.append("无明显异常")

    return _clamp(score), "; ".join(details)


def _score_meta(df, n, raw_scores, trend):
    """Meta: signal consistency + trend confirmation + anti-trap."""
    score, details = 0, []
    close = df["close"].values
    vol = df["volume"].fillna(0).values.astype(np.float64)

    # 1. Signal consistency
    positive = sum(1 for s in raw_scores if s > 0)
    negative = sum(1 for s in raw_scores if s < 0)
    total_abs = sum(abs(s) for s in raw_scores)

    if positive >= 6 and total_abs > 15:
        score += 3 if trend >= 1 else 1
        details.append("多维共振(看多)")
    elif negative >= 6 and total_abs > 15:
        score -= 3 if trend <= -1 else 1
        details.append("多维共振(看空)")
    elif positive <= 2 and negative <= 2:
        score -= 1
        details.append("信号混乱")
    else:
        details.append("信号分歧")

    # 2. Trend confirmation bonus
    raw_sum = sum(raw_scores)
    if trend >= 1 and raw_sum > 0:
        score += 2
        details.append("趋势确认(多头)")
    elif trend <= -1 and raw_sum < 0:
        score -= 2
        details.append("趋势确认(空头)")

    # 3. Anti-trap
    ma20 = _safe(df, n - 1, "ma20")
    cp = close[n - 1]
    if _v(ma20) and ma20 > 0:
        dev = (cp - ma20) / ma20 * 100
        if dev > 8:
            pct = _safe(df, n - 1, "pct_change")
            va5 = np.mean(vol[max(0, n - 6):n - 1])
            vr = vol[n - 1] / va5 if va5 > 0 else 1
            if _v(pct) and pct > 2 and vr > 2:
                score -= 5 if trend <= -1 else 3
                details.append("高位放量" + ("(诱多)" if trend <= -1 else "(警惕)"))
        if dev < -8:
            pct = _safe(df, n - 1, "pct_change")
            if _v(pct) and pct < -2:
                score += 4 if trend >= 1 else 2
                details.append("低位急跌" + ("(洗盘)" if trend >= 1 else "(诱空)"))

    # 4. Post-surge / post-purge
    if n >= 6:
        cum5 = (close[n - 1] / close[n - 6] - 1) * 100 if close[n - 6] > 0 else 0
        pt = _safe(df, n - 1, "pct_change")
        if cum5 > 10 and _v(pt) and pt < -1:
            if trend >= 1:
                score += 1
                details.append("大涨后回调(买点)")
            else:
                score -= 3
                details.append("大涨后回调(追高危险)")
        elif cum5 < -10 and _v(pt) and pt > 1:
            if trend >= 1:
                score += 3
                details.append("大跌后反弹(抄底)")
            else:
                details.append("大跌后反弹(趋势偏弱)")

    if not details:
        details.append("无特殊信号")

    return _clamp(score), "; ".join(details)


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _calc_atr(df, n, period=14):
    high = df["high"].values
    low = df["low"].values
    close = df["close"].values
    if n < 2:
        return 0
    trs = []
    for i in range(max(1, n - period), n):
        tr = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))
        trs.append(tr)
    return np.mean(trs) if trs else 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def calc_score(df: pd.DataFrame) -> dict:
    """Calculate next-day trading score.

    Returns dict with:
      total: int (0~100)
      action: str
      hold_advice: str
      dimensions: list of {name, score, detail, weight}
      summary: str
    """
    n = len(df)
    if n < 30:
        return {
            "total": 50, "action": "数据不足", "hold_advice": "无法判断",
            "dimensions": [], "summary": "历史数据不足30天，无法评分",
        }

    # Detect trend FIRST - all scorers use this
    trend = _detect_trend(df, n)

    zones = calc_support_resistance(df)
    signals = calc_signals(df)

    dim_scores = []
    dim_raw = []

    scorers = [
        ("短期动量", 20, _score_momentum, (df, n, trend)),
        ("量价配合", 18, _score_volume, (df, n, trend)),
        ("MA趋势", 12, _score_ma_trend, (df, n, trend)),
        ("主力行为", 12, _score_smart_money, (df, n, trend)),
        ("MACD动能", 10, _score_macd, (df, n, trend)),
        ("KDJ状态", 10, _score_kdj, (df, n, trend)),
        ("支撑压力", 8, _score_sr, (df, n, zones, trend)),
        ("背离信号", 5, _score_divergence, (df, n, signals, trend)),
        ("均线形态", 3, _score_squeeze, (df, n, trend)),
        ("趋势确认", 2, _score_meta, (df, n, dim_raw, trend)),
    ]

    for name, weight, fn, args in scorers[:9]:
        s, d = fn(*args)
        dim_raw.append(s)
        dim_scores.append({"name": name, "score": s, "detail": d, "weight": weight})

    name, weight, fn, args = scorers[9]
    s, d = fn(*args)
    dim_scores.append({"name": name, "score": s, "detail": d, "weight": weight})

    # Weighted total -> tanh non-linear normalization
    total_raw = sum(d["score"] * d["weight"] for d in dim_scores)
    total_weight = sum(d["weight"] for d in dim_scores)
    max_raw = total_weight * 10

    raw_ratio = total_raw / max_raw if max_raw > 0 else 0
    # tanh spreads scores: ratio=0.1→~62, ratio=0.2→~74, ratio=-0.1→~38
    normalized = 50 + 50 * np.tanh(2.5 * raw_ratio)
    normalized = max(0, min(100, round(normalized)))

    action, hold_advice, summary = _make_advice(normalized, dim_scores, df, n, trend)

    return {
        "total": normalized,
        "action": action,
        "hold_advice": hold_advice,
        "dimensions": dim_scores,
        "summary": summary,
    }


def _make_advice(total, dims, df, n, trend):
    trend_names = {2: "强势上升", 1: "上升", 0: "震荡", -1: "下降", -2: "强势下降"}

    if total >= 78:
        action = "强烈买入"
        hold_advice = "短线强势，果断跟进"
    elif total >= 63:
        action = "建议买入"
        hold_advice = "可逢低介入，设好止盈止损"
    elif total >= 50:
        action = "偏多观望"
        hold_advice = "轻仓试探，等待确认"
    elif total >= 40:
        action = "观望"
        hold_advice = "方向不明，场外等待"
    elif total >= 25:
        action = "建议卖出"
        hold_advice = "逢高减仓，规避风险"
    else:
        action = "强烈卖出"
        hold_advice = "尽快离场，切勿抄底"

    positives = [d for d in dims if d["score"] > 0]
    negatives = [d for d in dims if d["score"] < 0]

    parts = [f"短期趋势: {trend_names.get(trend, '震荡')}"]
    if positives:
        top_p = max(positives, key=lambda x: x["score"])
        parts.append(f"主要支撑: {top_p['name']}({top_p['detail']})")
    if negatives:
        top_n = min(negatives, key=lambda x: x["score"])
        parts.append(f"主要风险: {top_n['name']}({top_n['detail']})")

    summary = f"综合评分{total}分。" + "；".join(parts)
    return action, hold_advice, summary
