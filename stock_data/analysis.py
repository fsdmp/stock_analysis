"""Advanced technical analysis: Support/Resistance zones and Signal detection.

Ported from the original JS implementation in detail.html.
All calculations are done server-side; front-end only handles rendering.
"""

import numpy as np
import pandas as pd

from stock_data.config import MA_PERIODS


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _valid(v) -> bool:
    """Check if a value is non-None and finite."""
    return v is not None and np.isfinite(v)


# ---------------------------------------------------------------------------
# Support / Resistance Zone Calculation
# Multi-Factor + Anti-Trap Rules (identical logic to the original JS calcSR)
# ---------------------------------------------------------------------------

def calc_support_resistance(df: pd.DataFrame, lookback: int = 120) -> dict:
    """Calculate support and resistance zones.

    Returns::

        {
            "resistance": [{"low": float, "high": float, "score": int, "status": str, "tag": str}, ...],
            "support":    [{"low": float, "high": float, "score": int, "status": str, "tag": str}, ...],
        }

    Each list is sorted by score descending, max 3 entries.
    """
    n = len(df)
    if n < 20:
        return {"resistance": [], "support": []}

    lb = min(lookback, n)
    si = n - lb  # start index for lookback window

    close = df["close"].values
    high = df["high"].values
    low = df["low"].values
    vol = df["volume"].fillna(0).values.astype(np.float64)

    cp = close[n - 1]
    if not cp or cp <= 0:
        return {"resistance": [], "support": []}

    # --- Pre-compute ATR, volume averages, noisy bar flags ---
    atr_s = 0.0
    atr_n = 0
    va = np.zeros(n)       # 5-day average volume
    noisy = np.zeros(n, dtype=bool)

    for i in range(si, n):
        if i > si:
            tr = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))
            atr_s += tr
            atr_n += 1
        vs, vc = 0.0, 0
        for j in range(max(si, i - 5), i):
            vj = vol[j]
            if vj > 0:
                vs += vj
                vc += 1
        va[i] = vs / vc if vc > 0 else 0

    atr = atr_s / atr_n if atr_n > 0 else cp * 0.02

    # Rule 5: Mark quant noise bars (huge range + tiny volume)
    for i in range(si, n):
        noisy[i] = (high[i] - low[i]) > atr * 3 and va[i] > 0 and vol[i] < va[i] * 0.7

    pts: list[dict] = []

    def _add_bar_points(idx: int):
        """Add local high/low as candidate points."""
        d_close = close[idx]
        d_high = high[idx]
        d_low = low[idx]
        is_recent = idx >= n - 10
        N = 2 if is_recent else 5
        bwd = min(N, idx - si)
        fwd = min(N, n - 1 - idx)
        is_hi = all(high[j] < d_high for j in range(idx - bwd, idx + fwd + 1) if j != idx)
        is_lo = all(low[j] > d_low for j in range(idx - bwd, idx + fwd + 1) if j != idx)

        base = 15 if (not is_recent and bwd >= 5 and fwd >= 5) else 10
        br = d_high - d_low

        if is_hi:
            sc = base if (not is_recent and bwd >= 5 and fwd >= 5) else (
                base if (br > 0 and (d_high - d_close) / br < 0.3) else round(base * 0.5))
            pts.append({"p": d_high, "s": sc})

        if is_lo:
            sc = base if (not is_recent and bwd >= 5 and fwd >= 5) else (
                base if (br > 0 and (d_close - d_low) / br < 0.3) else round(base * 0.5))
            pts.append({"p": d_low, "s": sc})

    # --- Factor 1: Local High/Low (Rule 1 close-based + Rule 5 noise filter) ---
    for i in range(si + 2, n):
        if noisy[i]:
            continue
        _add_bar_points(i)

    # --- Factor 2: Moving Averages ---
    last = df.iloc[n - 1]
    for col, score in [("ma5", 12), ("ma10", 18), ("ma20", 30)]:
        v = last.get(col)
        if _valid(v):
            pts.append({"p": float(v), "s": score})

    # --- Factor 3: Volume Spikes (Rule 5 filter) ---
    for i in range(si, n):
        if noisy[i]:
            continue
        v = vol[i]
        if not v or not va[i]:
            continue
        if v >= va[i] * 2:
            r = v / va[i]
            sc = 25 if r >= 3 else round(10 + (r - 2) * 15)
            pts.append({"p": high[i], "s": sc})
            pts.append({"p": low[i], "s": sc})

    # --- Factor 4: Gaps (Rule 5 filter) ---
    for i in range(si + 1, n):
        if noisy[i] or noisy[i - 1]:
            continue
        pv_high, pv_low = high[i - 1], low[i - 1]
        cu_low, cu_high = low[i], high[i]
        if cu_low > pv_high:
            filled = any(low[j] <= pv_high for j in range(i + 1, n))
            pts.append({"p": (cu_low + pv_high) / 2, "s": 8 if filled else 20})
        if cu_high < pv_low:
            filled = any(high[j] >= pv_low for j in range(i + 1, n))
            pts.append({"p": (cu_high + pv_low) / 2, "s": 8 if filled else 20})

    # --- Factor 5: Consolidation Platforms ---
    for i in range(si + 10, n + 1):
        h_m = max(high[j] for j in range(i - 10, i))
        l_m = min(low[j] for j in range(i - 10, i))
        if h_m - l_m < atr * 1.5:
            pts.append({"p": h_m, "s": 10})
            pts.append({"p": l_m, "s": 10})

    # --- Factor 6: Recent bar highs/lows ---
    for i in range(max(si, n - 5), n):
        pts.append({"p": high[i], "s": 8})
        pts.append({"p": low[i], "s": 8})

    # --- Merge nearby candidates ---
    pts.sort(key=lambda x: x["p"])
    th = max(cp * 0.01, atr * 0.5)
    merged: list[dict] = []
    ci = 0
    while ci < len(pts):
        base_p = pts[ci]["p"]
        cl = {"min": base_p, "max": base_p, "score": pts[ci]["s"]}
        cj = ci + 1
        while cj < len(pts) and pts[cj]["p"] - base_p <= th:
            cl["score"] += pts[cj]["s"]
            cl["max"] = pts[cj]["p"]
            cj += 1
        merged.append(cl)
        ci = cj

    # --- Post-process: Rules 2, 3, 4, 6 per zone ---
    max_dist = max(cp * 0.20, atr * 5)
    min_w = cp * 0.008
    res, sup = [], []

    for z in merged:
        if z["score"] < 15:
            continue
        lo, hi = z["min"], z["max"]
        if hi - lo < min_w:
            c = (hi + lo) / 2
            lo = c - min_w / 2
            hi = c + min_w / 2
        mid = (lo + hi) / 2
        if mid > cp + max_dist or mid < cp - max_dist:
            continue

        score = min(100, z["score"])
        status, tag = "normal", ""
        is_res = mid > cp

        # Find zone "birth" index
        birth = n
        for i in range(si, n):
            if (is_res and close[i] < lo) or (not is_res and close[i] > hi):
                birth = i
                break

        # Rule 6: Count close-based touches AFTER birth
        in_z, touches = False, 0
        for i in range(birth, n):
            cl = close[i]
            inside = lo <= cl <= hi
            if inside and not in_z:
                touches += 1
                in_z = True
            elif not inside:
                in_z = False
        if touches >= 3:
            score += 20
        elif touches >= 2:
            score += 10

        # Rule 2: False breakout detection (only after birth)
        fb = 0
        for i in range(max(birth, si + 1), n - 3):
            if is_res:
                if high[i] > hi and close[i] < lo:
                    ret = all(close[j] < lo for j in range(i + 1, min(i + 4, n)))
                    if ret:
                        fb += 1
            else:
                if low[i] < lo and close[i] > hi:
                    ret = all(close[j] > hi for j in range(i + 1, min(i + 4, n)))
                    if ret:
                        fb += 1
        if fb > 0:
            score += 20 + (15 if fb > 1 else 0)
            status = "trap"
            tag = f"假突破{fb}次"

        # Rule 3: Valid breakout (only after birth)
        vb = False
        for i in range(max(birth, si + 1), n - 2):
            if is_res:
                if close[i] > hi and va[i] > 0 and vol[i] > va[i] * 1.2:
                    sus = all(close[j] > hi for j in range(i + 1, min(i + 3, n)))
                    if sus:
                        vb = True
                        break
            else:
                if close[i] < lo and va[i] > 0 and vol[i] > va[i] * 1.2:
                    sus = all(close[j] < lo for j in range(i + 1, min(i + 3, n)))
                    if sus:
                        vb = True
                        break
        if vb:
            score -= 25
            if status != "trap":
                status = "broken"
                tag = "失效"

        # Rule 4: Volume-price divergence (only after birth)
        div = False
        for i in range(max(birth, si + 2), n):
            d0_h, d1_h, d2_h = high[i - 2], high[i - 1], high[i]
            d0_l, d1_l, d2_l = low[i - 2], low[i - 1], low[i]
            d0_v, d1_v, d2_v = vol[i - 2], vol[i - 1], vol[i]
            if is_res:
                if (d2_h > d1_h > d0_h and d2_v < d1_v < d0_v
                        and d2_h >= lo - atr and d2_h <= hi + atr):
                    div = True
                    break
            else:
                if (d2_l < d1_l < d0_l and d2_v < d1_v < d0_v
                        and d2_l >= lo - atr and d2_l <= hi + atr):
                    div = True
                    break
        if div:
            score += -15 if is_res else 10
            tag = (tag + " 背离") if tag else "背离"

        score = max(0, min(100, score))
        if score < 15:
            continue

        (res if is_res else sup).append({
            "low": round(lo, 4),
            "high": round(hi, 4),
            "score": score,
            "status": status,
            "tag": tag,
        })

    res.sort(key=lambda x: x["score"], reverse=True)
    sup.sort(key=lambda x: x["score"], reverse=True)
    return {"resistance": res[:3], "support": sup[:3]}


# ---------------------------------------------------------------------------
# Signal Detection (identical logic to the original JS calcSignals)
# ---------------------------------------------------------------------------

def calc_signals(df: pd.DataFrame) -> dict:
    """Detect trading signals from a stock DataFrame with indicator columns.

    Requires columns: date, open, close, high, low, volume, pct_change,
                      ma5, ma7, ma10, ma20, macd_dif, macd_dea, macd_hist, kdj_k, kdj_d, kdj_j

    Returns a dict of signal arrays, each entry: {d: date_str, v: value, g: direction, ...}
    Keys: ma, macd, macdDiv, kdj, kdjExt, vol, squeeze, volPrice
    """
    n = len(df)
    sig: dict[str, list] = {
        "ma": [], "macd": [], "macdDiv": [], "kdj": [], "kdjExt": [],
        "vol": [], "squeeze": [], "volPrice": [],
    }
    if n < 30:
        return sig

    s0 = max(1, n - 300)

    date = df["date"].values
    close = df["close"].values
    high = df["high"].values
    low = df["low"].values
    vol_arr = df["volume"].fillna(0).values.astype(np.float64)
    pct = df["pct_change"].fillna(0).values if "pct_change" in df.columns else np.zeros(n)

    # Pre-convert dates to YYYY-MM-DD strings (avoid closure issues)
    date_strs = [str(d)[:10] for d in date]

    def _v(idx, col):
        val = df.iloc[idx].get(col)
        return _valid(val)

    def _get(idx, col):
        return df.iloc[idx][col]

    def _date_str(idx):
        return date_strs[idx]

    # MA Cross (MA5×MA10, MA10×MA20)
    for i in range(s0, n):
        if _v(i - 1, "ma5") and _v(i, "ma5") and _v(i - 1, "ma10") and _v(i, "ma10"):
            p5, c5 = _get(i - 1, "ma5"), _get(i, "ma5")
            p10, c10 = _get(i - 1, "ma10"), _get(i, "ma10")
            if p5 <= p10 and c5 > c10:
                sig["ma"].append({"d": _date_str(i), "v": round(float(c5), 4), "g": 1, "nm": "5/10"})
            if p5 >= p10 and c5 < c10:
                sig["ma"].append({"d": _date_str(i), "v": round(float(c5), 4), "g": 0, "nm": "5/10"})
        if _v(i - 1, "ma10") and _v(i, "ma10") and _v(i - 1, "ma20") and _v(i, "ma20"):
            p10, c10 = _get(i - 1, "ma10"), _get(i, "ma10")
            p20, c20 = _get(i - 1, "ma20"), _get(i, "ma20")
            if p10 <= p20 and c10 > c20:
                sig["ma"].append({"d": _date_str(i), "v": round(float(c10), 4), "g": 1, "nm": "10/20"})
            if p10 >= p20 and c10 < c20:
                sig["ma"].append({"d": _date_str(i), "v": round(float(c10), 4), "g": 0, "nm": "10/20"})

    # MACD Cross
    for i in range(s0, n):
        if not _v(i - 1, "macd_dif") or not _v(i, "macd_dif"):
            continue
        p_dif = _get(i - 1, "macd_dif")
        c_dif = _get(i, "macd_dif")
        p_dea = _get(i - 1, "macd_dea")
        c_dea = _get(i, "macd_dea")
        if p_dif <= p_dea and c_dif > c_dea:
            sig["macd"].append({"d": _date_str(i), "v": round(float(c_dea), 4), "g": 1})
        if p_dif >= p_dea and c_dif < c_dea:
            sig["macd"].append({"d": _date_str(i), "v": round(float(c_dea), 4), "g": 0})

    # KDJ Cross
    for i in range(s0, n):
        if not _v(i - 1, "kdj_k") or not _v(i, "kdj_k"):
            continue
        pk, ck = _get(i - 1, "kdj_k"), _get(i, "kdj_k")
        pd, cd = _get(i - 1, "kdj_d"), _get(i, "kdj_d")
        if pk <= pd and ck > cd:
            sig["kdj"].append({"d": _date_str(i), "v": round(float(cd), 4), "g": 1})
        if pk >= pd and ck < cd:
            sig["kdj"].append({"d": _date_str(i), "v": round(float(cd), 4), "g": 0})

    # KDJ Overbought/Oversold + 钝化
    ob_days, os_days = 0, 0
    for i in range(s0, n):
        if not _v(i, "kdj_k") or not _v(i, "kdj_d"):
            ob_days, os_days = 0, 0
            continue
        k_val = _get(i, "kdj_k")
        d_val = _get(i, "kdj_d")
        is_ob = k_val > 80 and d_val > 70
        is_os = k_val < 20 and d_val < 30
        if is_ob:
            ob_days += 1
            if ob_days == 1:
                sig["kdjExt"].append({"d": _date_str(i), "v": round(float(k_val), 4), "g": 1, "dh": 0})
            elif ob_days == 5:
                sig["kdjExt"].append({"d": _date_str(i), "v": round(float(k_val), 4), "g": 1, "dh": 1})
        else:
            ob_days = 0
        if is_os:
            os_days += 1
            if os_days == 1:
                sig["kdjExt"].append({"d": _date_str(i), "v": round(float(k_val), 4), "g": 0, "dh": 0})
            elif os_days == 5:
                sig["kdjExt"].append({"d": _date_str(i), "v": round(float(k_val), 4), "g": 0, "dh": 1})
        else:
            os_days = 0

    # Volume: 天量 / 巨量 / 地量
    for i in range(max(s0, 30), n):
        v = vol_arr[i]
        if not v or v <= 0:
            continue
        s5, c5 = 0.0, 0
        for j in range(i - 5, i):
            vj = vol_arr[j]
            if vj > 0:
                s5 += vj
                c5 += 1
        s20, c20 = 0.0, 0
        for j in range(i - 20, i):
            vj = vol_arr[j]
            if vj > 0:
                s20 += vj
                c20 += 1
        if c5 < 3:
            continue
        ma5_v = s5 / c5
        ma20_v = s20 / c20 if c20 >= 10 else ma5_v
        r5 = v / ma5_v
        r20 = v / ma20_v
        # 天量: 120日新高
        is_new_h = all(vol_arr[j] < v for j in range(max(0, i - 120), i))
        # 巨量: 近10日最高量 + 量比≥2.5
        is_peak10 = all(vol_arr[j] < v for j in range(max(0, i - 10), i))
        # 地量: 近30日最低量 + 不足均量50%
        is_valley30 = all((vol_arr[j] == 0 or vol_arr[j] >= v) for j in range(max(0, i - 30), i))

        if is_new_h:
            sig["vol"].append({"d": _date_str(i), "v": float(v), "g": 3})
        elif is_peak10 and (r5 >= 2.5 or r20 >= 2.5):
            sig["vol"].append({"d": _date_str(i), "v": float(v), "g": 1})
        if is_valley30 and r20 < 0.5:
            sig["vol"].append({"d": _date_str(i), "v": float(v), "g": 0})

    # MA Squeeze: MA5/10/20收敛 + 方向评分
    for i in range(max(s0, 20), n):
        if not (_v(i, "ma5") and _v(i, "ma10") and _v(i, "ma20")):
            continue
        ma5 = _get(i, "ma5")
        ma10 = _get(i, "ma10")
        ma20 = _get(i, "ma20")
        spread = (max(ma5, ma10, ma20) - min(ma5, ma10, ma20)) / close[i] * 100
        if spread >= 1.2:
            continue
        # Check if this is local minimum of spread
        is_min = True
        for j in range(max(s0, i - 10), i):
            if not (_v(j, "ma5") and _v(j, "ma10") and _v(j, "ma20")):
                continue
            ma5j = _get(j, "ma5")
            ma10j = _get(j, "ma10")
            ma20j = _get(j, "ma20")
            s2 = (max(ma5j, ma10j, ma20j) - min(ma5j, ma10j, ma20j)) / close[j] * 100
            if s2 <= spread:
                is_min = False
                break
        if not is_min:
            continue
        # 收敛确认: earlier spread should be wider
        max_prev = 0
        for j in range(max(s0, i - 10), max(s0, i - 3)):
            if not (_v(j, "ma5") and _v(j, "ma10") and _v(j, "ma20")):
                continue
            ma5j = _get(j, "ma5")
            ma10j = _get(j, "ma10")
            ma20j = _get(j, "ma20")
            s2 = (max(ma5j, ma10j, ma20j) - min(ma5j, ma10j, ma20j)) / close[j] * 100
            if s2 > max_prev:
                max_prev = s2
        if max_prev <= spread * 1.3:
            continue

        # === Direction scoring ===
        bs, bes = 0, 0
        bull_align = ma5 > ma10 > ma20
        bear_align = ma5 < ma10 < ma20
        if bull_align:
            bs += 3
        if bear_align:
            bes += 3
        ma_center = (ma5 + ma10 + ma20) / 3
        dev = (close[i] - ma_center) / close[i] * 100
        if dev > 0.5:
            bs += 2
        elif dev > 0.2:
            bs += 1
        if dev < -0.5:
            bes += 2
        elif dev < -0.2:
            bes += 1

        if i >= 5 and _v(i - 5, "ma5"):
            if ma5 > _get(i - 5, "ma5"):
                bs += 1
            if ma5 < _get(i - 5, "ma5"):
                bes += 1

        vr = sum(vol_arr[j] for j in range(i - 2, i + 1)) / 3
        vp_range = list(range(max(s0, i - 7), max(s0, i - 3)))
        vp = sum(vol_arr[j] for j in vp_range) / max(1, len(vp_range))
        vol_up = vp > 0 and vr > vp * 1.15
        if vol_up and dev > 0:
            bs += 1
        if vol_up and dev < 0:
            bes += 1
        if _v(i, "macd_hist") and _get(i, "macd_hist") > 0.02:
            bs += 1
        if _v(i, "macd_hist") and _get(i, "macd_hist") < -0.02:
            bes += 1

        # 20-day price direction
        ref_idx = max(s0, i - 20)
        trend_up = close[ref_idx] > 0 and close[i] > close[ref_idx]
        trend_dn = close[ref_idx] > 0 and close[i] < close[ref_idx]
        if abs(dev) < 3:
            if trend_up and bes > bs:
                bes -= 2
            if trend_dn and bs > bes:
                bs -= 2
        if dev > 3:
            bs -= 2
        if dev > 3 and trend_dn:
            bes += 2
        if dev < -3 and trend_up:
            bs += 2

        # Final direction
        if bull_align and bs >= 3 and (not trend_dn or bs >= 5):
            sig["squeeze"].append({"d": _date_str(i), "v": round(float(close[i]), 4), "g": 1})
        elif bear_align and bes >= 3 and (not trend_up or bes >= 5):
            sig["squeeze"].append({"d": _date_str(i), "v": round(float(close[i]), 4), "g": -1})
        elif bs >= 4 and bs >= bes + 2:
            sig["squeeze"].append({"d": _date_str(i), "v": round(float(close[i]), 4), "g": 1})
        elif bes >= 4 and bes >= bs + 2:
            sig["squeeze"].append({"d": _date_str(i), "v": round(float(close[i]), 4), "g": -1})
        elif spread < 0.5:
            sig["squeeze"].append({"d": _date_str(i), "v": round(float(close[i]), 4), "g": 0})

    # MACD Divergence: find peaks and troughs in price
    pk, tr = [], []
    for i in range(s0 + 3, n - 2):
        if not _v(i, "close") or not _v(i, "macd_dif"):
            continue
        is_p = all(close[j] < close[i] for j in range(i - 2, i + 3) if j != i)
        is_t = all(close[j] > close[i] for j in range(i - 2, i + 3) if j != i)
        if is_p:
            pk.append(i)
        if is_t:
            tr.append(i)

    # Top divergence
    for idx in range(1, len(pk)):
        a, b = pk[idx - 1], pk[idx]
        if not _v(a, "macd_dif") or not _v(b, "macd_dif"):
            continue
        if close[b] > close[a] * 1.015 and _get(b, "macd_dif") < _get(a, "macd_dif") - 0.01:
            sig["macdDiv"].append({"d": _date_str(b), "v": round(float(_get(b, "macd_dif")), 4), "g": 1})

    # Bottom divergence
    for idx in range(1, len(tr)):
        a, b = tr[idx - 1], tr[idx]
        if not _v(a, "macd_dif") or not _v(b, "macd_dif"):
            continue
        if close[b] < close[a] * 0.985 and _get(b, "macd_dif") > _get(a, "macd_dif") + 0.01:
            sig["macdDiv"].append({"d": _date_str(b), "v": round(float(_get(b, "macd_dif")), 4), "g": 0})

    # 量价背离
    for idx in range(1, len(pk)):
        a, b = pk[idx - 1], pk[idx]
        if close[b] > close[a] * 1.02 and vol_arr[b] < vol_arr[a] * 0.65:
            sig["volPrice"].append({"d": _date_str(b), "v": float(vol_arr[b]), "g": 1})
    for idx in range(1, len(tr)):
        a, b = tr[idx - 1], tr[idx]
        if close[b] < close[a] * 0.98 and vol_arr[b] < vol_arr[a] * 0.6:
            sig["volPrice"].append({"d": _date_str(b), "v": float(vol_arr[b]), "g": 0})

    # === Noise Reduction: Anti-Trap Filters ===
    date_to_idx: dict[str, int] = {}
    for i in range(n):
        date_to_idx[_date_str(i)] = i

    def _va(idx, p):
        s, c = 0.0, 0
        for j in range(max(0, idx - p), idx):
            v = vol_arr[j]
            if v > 0:
                s += v
                c += 1
        return s / c if c >= 3 else 0

    def _dedup(arr, gap):
        if len(arr) <= 1:
            return arr
        r = []
        for i, s in enumerate(arr):
            idx = date_to_idx.get(s["d"])
            if idx is None:
                continue
            if i + 1 < len(arr):
                ni = date_to_idx.get(arr[i + 1]["d"])
                if ni is not None and ni - idx < gap:
                    continue
            r.append(s)
        return r

    # 1. MA Cross: persistence + volume + anti-whipsaw
    filtered_ma = []
    for s in sig["ma"]:
        idx = date_to_idx.get(s["d"])
        if idx is None:
            continue
        # Persistence: next bar must still hold cross direction
        if idx + 1 < n:
            nx_ma5 = _get(idx + 1, "ma5") if _v(idx + 1, "ma5") else None
            nx_ma10 = _get(idx + 1, "ma10") if _v(idx + 1, "ma10") else None
            nx_ma20 = _get(idx + 1, "ma20") if _v(idx + 1, "ma20") else None
            if s["g"] == 1:
                if s["nm"] == "5/10" and _valid(nx_ma5) and _valid(nx_ma10) and nx_ma5 <= nx_ma10:
                    continue
                if s["nm"] == "10/20" and _valid(nx_ma10) and _valid(nx_ma20) and nx_ma10 <= nx_ma20:
                    continue
            else:
                if s["nm"] == "5/10" and _valid(nx_ma5) and _valid(nx_ma10) and nx_ma5 >= nx_ma10:
                    continue
                if s["nm"] == "10/20" and _valid(nx_ma10) and _valid(nx_ma20) and nx_ma10 >= nx_ma20:
                    continue
        # Golden cross volume confirmation
        if s["g"] == 1:
            va5 = _va(idx, 5)
            if va5 > 0 and vol_arr[idx] < va5 * 0.7:
                continue
        filtered_ma.append(s)
    sig["ma"] = _dedup(filtered_ma, 8)

    # 2. MACD Cross: HIST expansion + volume
    filtered_macd = []
    for s in sig["macd"]:
        idx = date_to_idx.get(s["d"])
        if idx is None:
            continue
        if idx > 0 and _v(idx, "macd_hist") and _v(idx - 1, "macd_hist"):
            d_hist = _get(idx, "macd_hist")
            p_hist = _get(idx - 1, "macd_hist")
            if abs(d_hist) <= abs(p_hist) * 0.8:
                continue
        if s["g"] == 1:
            va5 = _va(idx, 5)
            if va5 > 0 and vol_arr[idx] < va5 * 0.7:
                continue
        filtered_macd.append(s)
    sig["macd"] = _dedup(filtered_macd, 5)

    # 3. KDJ Cross: zone filter (mid-zone 35~65 = noise)
    filtered_kdj = []
    for s in sig["kdj"]:
        idx = date_to_idx.get(s["d"])
        if idx is None:
            continue
        if _v(idx, "kdj_k"):
            k_val = _get(idx, "kdj_k")
            if 35 < k_val < 65:
                continue
        if s["g"] == 1:
            va5 = _va(idx, 5)
            if va5 > 0 and vol_arr[idx] < va5 * 0.7:
                continue
        filtered_kdj.append(s)
    sig["kdj"] = _dedup(filtered_kdj, 5)

    # 4. MACD Divergence: DIF must have meaningful absolute value
    sig["macdDiv"] = _dedup([
        s for s in sig["macdDiv"]
        if date_to_idx.get(s["d"]) is not None
        and _v(date_to_idx[s["d"]], "macd_dif")
        and abs(_get(date_to_idx[s["d"]], "macd_dif")) >= 0.02
    ], 8)

    # 5. Volume: 巨量 requires meaningful price move + drop invalid index
    sig["vol"] = _dedup([
        s for s in sig["vol"]
        if date_to_idx.get(s["d"]) is not None
        and not (s["g"] == 1 and abs(pct[date_to_idx[s["d"]]]) < 1.0)
    ], 5)

    # 6. MA Squeeze: MACD direction confirmation
    sig["squeeze"] = _dedup([
        s for s in sig["squeeze"]
        if date_to_idx.get(s["d"]) is not None
        and not (
            s["g"] == 1 and _v(date_to_idx[s["d"]], "macd_hist")
            and _get(date_to_idx[s["d"]], "macd_hist") < -0.05
        )
        and not (
            s["g"] == -1 and _v(date_to_idx[s["d"]], "macd_hist")
            and _get(date_to_idx[s["d"]], "macd_hist") > 0.05
        )
    ], 10)

    # 7. Volume-Price Divergence: dedup
    sig["volPrice"] = _dedup(sig["volPrice"], 8)

    return sig


# ---------------------------------------------------------------------------
# Convenience: Full analysis for a stock
# ---------------------------------------------------------------------------

def analyze_stock(df: pd.DataFrame) -> dict:
    """Run full analysis on a DataFrame that already has indicator columns.

    Returns {"zones": {...}, "signals": {...}}.
    """
    return {
        "zones": calc_support_resistance(df),
        "signals": calc_signals(df),
    }
