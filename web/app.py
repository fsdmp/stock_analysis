"""Flask backend for A-share stock data visualization."""

import sys
import json
import atexit
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from flask import Flask, jsonify, request, send_from_directory, session, redirect, url_for
import pandas as pd
import numpy as np
import pyarrow.parquet as pq

from stock_data.bs_manager import bs_shutdown

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "stocks"
CACHE_DIR = Path(__file__).resolve().parent.parent / "data"
STRATEGIES_DIR = Path(__file__).resolve().parent.parent / "strategies"
STRATEGY_RESULTS_DIR = Path(__file__).resolve().parent.parent / "data" / "strategy_results"
SCORE_RESULTS_DIR = Path(__file__).resolve().parent.parent / "data" / "score_results"
STOCK_NAMES_CACHE = CACHE_DIR / "stock_names.json"
WATCHLIST_FILE = CACHE_DIR / "watchlist.json"
app = Flask(__name__, static_folder="static", static_url_path="")
app.secret_key = "fsdm-stock-analysis-2026"

# Fixed credentials
_CREDENTIALS = {"fsdm": "fsdm00001"}


def login_required(f):
    """Redirect to login page if not authenticated."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user"):
            # API requests get 401, page requests redirect
            if request.path.startswith("/api/"):
                return jsonify({"error": "未登录"}), 401
            return redirect(url_for("page_login"))
        return f(*args, **kwargs)
    return decorated


atexit.register(bs_shutdown)

# Pre-build stock index on startup
_stock_index: dict = {}
_stock_names: dict = {}  # code -> name mapping
_update_status = {"running": False, "message": "", "current": 0, "total": 0, "code": ""}
_full_scan_status = {
    "running": False, "done": False, "message": "",
    "current": 0, "total": 0, "code": "",
    "results": [], "min_score": 95,
}
_score_scan_status = {
    "running": False, "done": False, "message": "",
    "current": 0, "total": 0, "code": "",
    "results": [], "_saved": False,
}
_strategy_scan_status = {
    "running": False, "done": False, "message": "",
    "current": 0, "total": 0, "code": "",
    "results": [], "strategy_name": "",
}
_backtest_status = {
    "running": False, "done": False, "message": "",
    "current": 0, "total": 0,
    "trades": [], "report": None,
}


def build_stock_names(use_cache=True):
    """Load stock code-to-name mapping, preferring local
       cache."""
    global _stock_names
     
    # Try loading from cache first
    if use_cache and STOCK_NAMES_CACHE.exists():
        try:
            with open(STOCK_NAMES_CACHE, "r", encoding="utf-8") as f:
                 _stock_names = json.load(f)
            print(f"Loaded {len(_stock_names)} stock names from cache")
            return
        except Exception as e:
            print(f"Warning: cache read failed: {e}")
    
    # Fallback: fetch from baostock
    try:
        from stock_data.fetcher import get_mainboard_stocks
        df = get_mainboard_stocks()
        _stock_names = dict(zip(df["code"], df["name"]))
        # Save to cache for next startup
        with open(STOCK_NAMES_CACHE, "w", encoding="utf-8") as f:
            json.dump(_stock_names, f, ensure_ascii=False)
        print(f"Loaded {len(_stock_names)} stock names and saved cache")
    except Exception as e:
        print(f"Warning: failed to load stock names: {e}")
        _stock_names = {}


def build_stock_index():
    """Build lightweight index by reading only last row of each parquet file."""
    print(f"Building stock index from {DATA_DIR}...")
    for f in DATA_DIR.glob("*.parquet"):
        try:
            pf = pq.ParquetFile(f)
            last_batch = pf.read_row_group(pf.num_row_groups - 1, columns=["date", "close", "pct_change"])
            last_idx = last_batch.num_rows - 1
            _stock_index[f.stem] = {
                "code": f.stem,
                "name": _stock_names.get(f.stem, ""),
                "close": round(float(last_batch.column("close")[last_idx].as_py()), 2),
                "pct_change": round(float(last_batch.column("pct_change")[last_idx].as_py() or 0), 2),
                "date": str(pd.Timestamp(last_batch.column("date")[last_idx].as_py()).date()),
            }
        except Exception:
            _stock_index[f.stem] = {"code": f.stem, "name": _stock_names.get(f.stem, ""), "close": 0, "pct_change": 0, "date": ""}
    print(f"Indexed {len(_stock_index)} stocks")


# === Watchlist ===

def _load_watchlist():
    """Load watchlist from JSON file, ensuring default group exists."""
    if WATCHLIST_FILE.exists():
        try:
            with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            groups = data.get("groups", [])
            if not any(g["id"] == "default" for g in groups):
                groups.insert(0, {"id": "default", "name": "默认分组", "stocks": []})
            return groups
        except Exception:
            pass
    return [{"id": "default", "name": "默认分组", "stocks": []}]


def _save_watchlist(groups):
    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
        json.dump({"groups": groups}, f, ensure_ascii=False, indent=2)


@app.route("/watchlist")
@login_required
def page_watchlist():
    return send_from_directory("static", "watchlist.html")


@app.route("/api/watchlist")
@login_required
def api_watchlist():
    """Get all groups with enriched stock info."""
    groups = _load_watchlist()
    result = []
    for g in groups:
        stocks = []
        for code in g["stocks"]:
            info = _stock_index.get(code, {"code": code, "name": _stock_names.get(code, "")})
            stocks.append({
                "code": code,
                "name": info.get("name", _stock_names.get(code, "")),
                "close": info.get("close", 0),
                "pct_change": info.get("pct_change", 0),
                "date": info.get("date", ""),
            })
        result.append({"id": g["id"], "name": g["name"], "stocks": stocks})
    return jsonify(result)


@app.route("/api/watchlist/group", methods=["POST"])
@login_required
def api_watchlist_add_group():
    """Create a new group."""
    data = request.get_json(silent=True) or {}
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "分组名不能为空"}), 400
    import time
    groups = _load_watchlist()
    gid = f"g_{int(time.time() * 1000)}"
    groups.append({"id": gid, "name": name, "stocks": []})
    _save_watchlist(groups)
    return jsonify({"status": "ok", "id": gid, "name": name})


@app.route("/api/watchlist/group/<gid>", methods=["DELETE"])
@login_required
def api_watchlist_del_group(gid):
    """Delete a group; its stocks move to default."""
    if gid == "default":
        return jsonify({"error": "不能删除默认分组"}), 400
    groups = _load_watchlist()
    moved = []
    new_groups = []
    for g in groups:
        if g["id"] == gid:
            moved = g["stocks"]
        else:
            new_groups.append(g)
    # Merge stocks into default
    for g in new_groups:
        if g["id"] == "default":
            existing = set(g["stocks"])
            for s in moved:
                if s not in existing:
                    g["stocks"].append(s)
            break
    _save_watchlist(new_groups)
    return jsonify({"status": "ok"})


@app.route("/api/watchlist/group/<gid>", methods=["PUT"])
@login_required
def api_watchlist_rename_group(gid):
    """Rename a group."""
    data = request.get_json(silent=True) or {}
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "分组名不能为空"}), 400
    groups = _load_watchlist()
    for g in groups:
        if g["id"] == gid:
            g["name"] = name
            break
    _save_watchlist(groups)
    return jsonify({"status": "ok"})


@app.route("/api/watchlist/stock", methods=["POST"])
@login_required
def api_watchlist_add_stock():
    """Add a stock to a group."""
    data = request.get_json(silent=True) or {}
    code = data.get("code", "").strip().zfill(6)
    gid = data.get("group_id", "default")
    if not code:
        return jsonify({"error": "股票代码不能为空"}), 400
    groups = _load_watchlist()
    # Remove from all groups first (avoid duplicates)
    for g in groups:
        if code in g["stocks"]:
            g["stocks"].remove(code)
    # Add to target group
    for g in groups:
        if g["id"] == gid:
            g["stocks"].append(code)
            break
    _save_watchlist(groups)
    return jsonify({"status": "ok"})


@app.route("/api/watchlist/stock", methods=["DELETE"])
@login_required
def api_watchlist_remove_stock():
    """Remove a stock from a group."""
    data = request.get_json(silent=True) or {}
    code = data.get("code", "").strip().zfill(6)
    gid = data.get("group_id", "")
    if not code:
        return jsonify({"error": "股票代码不能为空"}), 400
    groups = _load_watchlist()
    for g in groups:
        if gid and g["id"] != gid:
            continue
        if code in g["stocks"]:
            g["stocks"].remove(code)
    _save_watchlist(groups)
    return jsonify({"status": "ok"})


@app.route("/api/watchlist/move", methods=["POST"])
@login_required
def api_watchlist_move_stock():
    """Move a stock between groups."""
    data = request.get_json(silent=True) or {}
    code = data.get("code", "").strip().zfill(6)
    from_gid = data.get("from", "")
    to_gid = data.get("to", "")
    if not code or not from_gid or not to_gid:
        return jsonify({"error": "参数不完整"}), 400
    groups = _load_watchlist()
    src = next((g for g in groups if g["id"] == from_gid), None)
    dst = next((g for g in groups if g["id"] == to_gid), None)
    if not src or not dst:
        return jsonify({"error": "分组不存在"}), 404
    if code in src["stocks"]:
        src["stocks"].remove(code)
    if code not in dst["stocks"]:
        dst["stocks"].append(code)
    _save_watchlist(groups)
    return jsonify({"status": "ok"})


@app.route("/api/watchlist/batch-add", methods=["POST"])
@login_required
def api_watchlist_batch_add():
    """Batch-add stocks to a (possibly new) group. { group_name, codes }"""
    data = request.get_json(silent=True) or {}
    group_name = data.get("group_name", "").strip()
    codes = data.get("codes", [])
    if not group_name:
        return jsonify({"error": "分组名不能为空"}), 400
    if not codes or not isinstance(codes, list):
        return jsonify({"error": "股票列表不能为空"}), 400

    import time
    groups = _load_watchlist()

    # Find or create group
    target = next((g for g in groups if g["name"] == group_name), None)
    if not target:
        gid = f"g_{int(time.time() * 1000)}"
        target = {"id": gid, "name": group_name, "stocks": []}
        groups.append(target)

    existing = set(target["stocks"])
    added = 0
    for raw_code in codes:
        code = str(raw_code).strip().zfill(6)
        if not code:
            continue
        # Add to target (allow stock in multiple groups, don't remove from others)
        if code not in existing:
            target["stocks"].append(code)
            existing.add(code)
            added += 1

    _save_watchlist(groups)
    return jsonify({"status": "ok", "group_id": target["id"], "group_name": group_name, "added": added, "total": len(target["stocks"])})


@app.route("/api/watchlist/check/<code>")
@login_required
def api_watchlist_check(code):
    """Check if a stock is in watchlist and return its groups."""
    code = code.zfill(6)
    groups = _load_watchlist()
    result = []
    for g in groups:
        if code in g["stocks"]:
            result.append({"id": g["id"], "name": g["name"]})
    return jsonify({"code": code, "groups": result})


# === Pages ===

@app.route("/login")
def page_login():
    if session.get("user"):
        return redirect(url_for("page_home"))
    return send_from_directory("static", "login.html")


@app.route("/")
def page_home():
    return send_from_directory("static", "home.html")


@app.route("/stock/<code>")
def page_detail(code):
    return send_from_directory("static", "detail.html")


@app.route("/recommend")
@login_required
def page_recommend():
    return send_from_directory("static", "recommend.html")


# === APIs ===

@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(silent=True) or {}
    username = data.get("username", "").strip()
    password = data.get("password", "")
    if username in _CREDENTIALS and _CREDENTIALS[username] == password:
        session["user"] = username
        return jsonify({"status": "ok", "user": username})
    return jsonify({"error": "账号或密码错误"}), 401


@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.pop("user", None)
    return jsonify({"status": "ok"})


@app.route("/api/check")
def api_check():
    if session.get("user"):
        return jsonify({"logged_in": True, "user": session["user"]})
    return jsonify({"logged_in": False}), 401


@app.route("/api/stocks")
def api_stocks():
    """Stock list with pagination and search. ?q=&page=&size="""
    q = request.args.get("q", "").strip()
    page = int(request.args.get("page", 1))
    size = int(request.args.get("size", 50))

    # Rebuild index if new files appeared (fetcher running in background)
    file_count = len(list(DATA_DIR.glob("*.parquet")))
    if file_count != len(_stock_index):
        build_stock_index()

    all_list = sorted(_stock_index.values(), key=lambda x: x["code"])

    if q:
        all_list = [s for s in all_list if q in s["code"] or q in s.get("name", "")]

    total = len(all_list)
    start = (page - 1) * size
    items = all_list[start:start + size]
    return jsonify({"total": total, "page": page, "size": size, "data": items})


@app.route("/api/stock/<code>")
def api_stock(code):
    """Single stock chart data."""
    code = code.zfill(6)
    path = DATA_DIR / f"{code}.parquet"
    if not path.exists():
        return jsonify({"error": f"Stock {code} not found"}), 404

    cols = [
        "date", "open", "close", "high", "low", "volume", "pct_change", "turnover",
        "ma5", "ma7", "ma10", "ma20",
        "vwma5", "vwma10", "vwma20",
        "bb_upper", "bb_middle", "bb_lower", "bb_bandwidth",
        "macd_dif", "macd_dea", "macd_hist",
        "kdj_k", "kdj_d", "kdj_j",
    ]
    # Only read columns that exist in the parquet file
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(path)
    available = set(pf.schema.names)
    cols = [c for c in cols if c in available]

    df = pd.read_parquet(path, columns=cols)

    start = request.args.get("start")
    end = request.args.get("end")
    if start:
        df = df[df["date"] >= pd.Timestamp(start)]
    if end:
        df = df[df["date"] <= pd.Timestamp(end)]

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    # Replace NaN/NA with None for JSON serialization
    df = df.astype(object).where(df.notna(), None)
    return jsonify({"columns": cols, "data": df.values.tolist(), "name": _stock_names.get(code, "")})


@app.route("/api/analysis/<code>")
def api_analysis(code):
    """Return support/resistance zones and trading signals for a stock."""
    code = code.zfill(6)
    path = DATA_DIR / f"{code}.parquet"
    if not path.exists():
        return jsonify({"error": f"Stock {code} not found"}), 404

    from stock_data.analysis import analyze_stock
    cols = [
        "date", "open", "close", "high", "low", "volume", "pct_change",
        "ma5", "ma7", "ma10", "ma20",
        "vwma5", "vwma10", "vwma20",
        "bb_upper", "bb_middle", "bb_lower", "bb_bandwidth",
        "macd_dif", "macd_dea", "macd_hist",
        "kdj_k", "kdj_d", "kdj_j",
    ]
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(path)
    available = set(pf.schema.names)
    cols = [c for c in cols if c in available]

    df = pd.read_parquet(path, columns=cols)

    start = request.args.get("start")
    end = request.args.get("end")
    if start:
        df = df[df["date"] >= pd.Timestamp(start)]
    if end:
        df = df[df["date"] <= pd.Timestamp(end)]

    result = analyze_stock(df)
    return jsonify(result)


@app.route("/api/intraday/<code>/<date>")
def api_intraday(code, date):
    """Return minute-level intraday data for a stock on a specific date.

    Query params: freq=5 (default) or freq=1
    """
    code = code.zfill(6)
    freq = request.args.get("freq", "5")
    if freq not in ("5", "15"):
        return jsonify({"error": f"Invalid freq '{freq}', must be 5 or 15"}), 400

    # Validate date format
    try:
        pd.Timestamp(date)
    except Exception:
        return jsonify({"error": f"Invalid date format: {date}"}), 400

    from stock_data.intraday_fetcher import get_intraday_data
    df = get_intraday_data(code, date.replace("-", ""), freq)
    if df is None or df.empty:
        return jsonify({"error": f"暂无 {code} 在 {date} 的分时数据"}), 404

    cols = ["time", "open", "close", "high", "low", "volume", "amount",
            "ma5", "ma20", "macd_dif", "macd_dea", "macd_hist"]
    cols = [c for c in cols if c in df.columns]
    df = df[cols]
    df = df.astype(object).where(df.notna(), None)
    return jsonify({"columns": cols, "data": df.values.tolist(),
                     "date": date, "code": code, "freq": freq})


@app.route("/api/score/<code>")
def api_score(code):
    """Return next-day trading score for a stock, using cached data if available."""
    code = code.zfill(6)
    path = DATA_DIR / f"{code}.parquet"
    if not path.exists():
        return jsonify({"error": f"Stock {code} not found"}), 404

    # Try to load from cache first
    result_file = SCORE_RESULTS_DIR / "latest.parquet"
    if result_file.exists():
        try:
            cached_df = pd.read_parquet(result_file)
            cached_row = cached_df[cached_df["code"] == code]
            if not cached_row.empty:
                # Check if data file has changed
                current_mtime = path.stat().st_mtime
                cached_mtime = cached_row.iloc[0].get("data_mtime", 0)

                if current_mtime <= cached_mtime:
                    # Return cached result
                    result = cached_row.iloc[0].to_dict()
                    # Convert numpy types to Python types for JSON serialization
                    for k, v in result.items():
                        if k == "dimensions" and isinstance(v, str):
                            try:
                                result[k] = json.loads(v)
                            except Exception:
                                pass
                        elif isinstance(v, np.ndarray):
                            result[k] = v.tolist()
                        elif pd.isna(v):
                            result[k] = None
                        elif isinstance(v, (np.integer, np.int64)):
                            result[k] = int(v)
                        elif isinstance(v, (np.floating, np.float64)):
                            result[k] = float(v)
                    return jsonify(result)
        except Exception:
            pass

    # Fall back to calculation
    from stock_data.scoring import calc_score
    cols = [
        "date", "open", "close", "high", "low", "volume", "pct_change", "turnover",
        "ma5", "ma7", "ma10", "ma20",
        "vwma5", "vwma10", "vwma20",
        "bb_upper", "bb_middle", "bb_lower", "bb_bandwidth",
        "macd_dif", "macd_dea", "macd_hist",
        "kdj_k", "kdj_d", "kdj_j",
    ]
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(path)
    available = set(pf.schema.names)
    cols = [c for c in cols if c in available]

    df = pd.read_parquet(path, columns=cols)
    result = calc_score(df)
    result["code"] = code
    result["name"] = _stock_names.get(code, "")
    result["close"] = round(float(df["close"].iloc[-1]), 2)
    return jsonify(result)


@app.route("/api/batch-score", methods=["POST"])
@login_required
def api_batch_score():
    """Score multiple stocks and return ranked results, using cached data if available."""
    codes = request.get_json(silent=True) or []
    if not codes or not isinstance(codes, list):
        return jsonify({"error": "请提供股票代码列表"}), 400

    codes = [c.zfill(6) for c in codes if isinstance(c, str) and c.strip()]
    if not codes:
        return jsonify({"error": "无有效股票代码"}), 400

    # Load cached results if available
    result_file = SCORE_RESULTS_DIR / "latest.parquet"
    cached_map = {}
    cached_mtime_map = {}
    if result_file.exists():
        try:
            cached_df = pd.read_parquet(result_file)
            for _, row in cached_df.iterrows():
                cached_map[row["code"]] = row.to_dict()
                cached_mtime_map[row["code"]] = row.get("data_mtime", 0)
        except Exception:
            pass

    results = []
    codes_to_calc = []

    # First pass: try to use cached data
    for code in codes:
        path = DATA_DIR / f"{code}.parquet"
        if not path.exists():
            results.append({
                "code": code, "name": _stock_names.get(code, ""),
                "error": "未找到数据",
            })
            continue

        if code in cached_map:
            # Check if data file has changed
            current_mtime = path.stat().st_mtime
            if current_mtime <= cached_mtime_map[code]:
                # Use cached result
                result = cached_map[code].copy()
                # Convert numpy types to Python types
                for k, v in result.items():
                    if k == "dimensions" and isinstance(v, str):
                        try:
                            result[k] = json.loads(v)
                        except Exception:
                            pass
                    elif isinstance(v, np.ndarray):
                        result[k] = v.tolist()
                    elif pd.isna(v):
                        result[k] = None
                    elif isinstance(v, (np.integer, np.int64)):
                        result[k] = int(v)
                    elif isinstance(v, (np.floating, np.float64)):
                        result[k] = float(v)
                results.append(result)
                continue

        # Need to calculate
        codes_to_calc.append(code)

    # Second pass: calculate for uncached or changed stocks
    from stock_data.scoring import calc_score
    for code in codes_to_calc:
        path = DATA_DIR / f"{code}.parquet"
        try:
            cols = [
                "date", "open", "close", "high", "low", "volume", "pct_change", "turnover",
                "ma5", "ma7", "ma10", "ma20",
                "vwma5", "vwma10", "vwma20",
                "bb_upper", "bb_middle", "bb_lower", "bb_bandwidth",
                "macd_dif", "macd_dea", "macd_hist",
                "kdj_k", "kdj_d", "kdj_j",
            ]
            pf = pq.ParquetFile(path)
            available = set(pf.schema.names)
            cols = [c for c in cols if c in available]

            df = pd.read_parquet(path, columns=cols)
            result = calc_score(df)
            result["code"] = code
            result["name"] = _stock_names.get(code, "")
            result["close"] = round(float(df["close"].iloc[-1]), 2)
            result["pct_change"] = round(float(df["pct_change"].iloc[-1] or 0), 2)
            results.append(result)
        except Exception as e:
            results.append({
                "code": code, "name": _stock_names.get(code, ""),
                "error": str(e),
            })

    results.sort(key=lambda x: x.get("total", 0) if "total" in x else -1, reverse=True)
    return jsonify(results)


@app.route("/api/full-scan", methods=["POST"])
@login_required
def api_full_scan():
    """Start full market score scan and save results to disk."""
    if _score_scan_status["running"]:
        return jsonify({"status": "already_running", "message": "评分扫描正在进行中"})

    data = request.get_json(silent=True) or {}
    min_score = int(data.get("min_score", 0))
    min_score = max(0, min(100, min_score))
    max_price = float(data.get("max_price", 10000))

    def run_scan():
        import os
        from datetime import datetime

        _score_scan_status.update({
            "running": True, "done": False, "results": [],
            "current": 0, "total": 0, "code": "",
            "min_score": min_score, "_saved": False,
            "message": "正在扫描...",
        })

        files = sorted(DATA_DIR.glob("*.parquet"))
        # Pre-filter: skip stocks with close > max_price
        files = [f for f in files if _stock_index.get(f.stem, {}).get("close", 0) <= max_price]
        _score_scan_status["total"] = len(files)

        from stock_data.scoring import calc_score

        _scan_cols = [
            "date", "open", "close", "high", "low", "volume",
            "pct_change", "turnover",
            "ma5", "ma7", "ma10", "ma20",
            "vwma5", "vwma10", "vwma20",
            "bb_upper", "bb_middle", "bb_lower", "bb_bandwidth",
            "macd_dif", "macd_dea", "macd_hist",
            "kdj_k", "kdj_d", "kdj_j",
        ]

        def _score_one(f):
            code = f.stem
            try:
                pf = pq.ParquetFile(f)
                total_rows = pf.metadata.num_rows
                if total_rows < 30:
                    return None
                available = set(pf.schema.names)
                cols = [c for c in _scan_cols if c in available]
                tail = min(total_rows, 150)
                df = pd.read_parquet(f, columns=cols).iloc[-tail:].reset_index(drop=True)
                if len(df) < 30:
                    return None
                result = calc_score(df)
                result["code"] = code
                result["name"] = _stock_names.get(code, "")
                result["close"] = round(float(df["close"].iloc[-1]), 2)
                result["pct_change"] = round(float(df["pct_change"].iloc[-1] or 0), 2)
                result["data_mtime"] = f.stat().st_mtime
                return result
            except Exception:
                pass
            return None

        def _score_one_timeout(f, timeout=30):
            """Run _score_one with a per-stock timeout."""
            import queue
            q = queue.Queue()

            def _worker():
                try:
                    q.put(_score_one(f))
                except Exception:
                    q.put(None)

            t = threading.Thread(target=_worker, daemon=True)
            t.start()
            t.join(timeout=timeout)
            if t.is_alive():
                print(f"[WARN] {f.stem} timed out after {timeout}s, skipping")
                return None
            return q.get_nowait()

        all_results = []
        for i, f in enumerate(files):
            _score_scan_status["current"] = i + 1
            _score_scan_status["code"] = f.stem
            try:
                r = _score_one_timeout(f)
                if r:
                    all_results.append(r)
            except Exception:
                pass

        # Sort by total score and save to disk
        all_results.sort(key=lambda x: x.get("total", 0), reverse=True)
        _score_scan_status["results"] = all_results

        # Save to parquet file — serialize 'dimensions' as JSON string
        SCORE_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        flat_results = []
        for r in all_results:
            fr = {}
            for k, v in r.items():
                if k == "dimensions":
                    fr[k] = json.dumps(v, ensure_ascii=False)
                else:
                    fr[k] = v
            flat_results.append(fr)
        result_df = pd.DataFrame(flat_results)
        result_file = SCORE_RESULTS_DIR / "latest.parquet"
        result_df.to_parquet(result_file, index=False)

        # Also save a summary JSON
        summary_file = SCORE_RESULTS_DIR / "summary.json"
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump({
                "scan_time": datetime.now().isoformat(timespec="seconds"),
                "total_stocks": len(all_results),
                "high_score_count": len([r for r in all_results if r["total"] >= min_score]),
                "min_score_filter": min_score,
            }, f, ensure_ascii=False)

        _score_scan_status["running"] = False
        _score_scan_status["done"] = True
        _score_scan_status["_saved"] = True
        _score_scan_status["message"] = f"扫描完成，共 {len(all_results)} 只股票，其中 {len([r for r in all_results if r['total'] >= min_score])} 只评分≥{min_score}"

    t = threading.Thread(target=run_scan, daemon=True)
    t.start()
    return jsonify({"status": "started", "message": "评分扫描已启动"})


@app.route("/api/full-scan/status")
@login_required
def api_full_scan_status():
    """Check score scan progress (legacy endpoint for backward compatibility)."""
    return api_score_scan_status()


@app.route("/api/score/scan/status")
@login_required
def api_score_scan_status():
    """Check score scan progress."""
    status = {
        "running": _score_scan_status["running"],
        "done": _score_scan_status["done"],
        "message": _score_scan_status["message"],
        "current": _score_scan_status["current"],
        "total": _score_scan_status["total"],
        "code": _score_scan_status["code"],
        "min_score": _score_scan_status.get("min_score", 0),
        "found": len(_score_scan_status["results"]),
    }
    return jsonify(status)


# === Score APIs ===

@app.route("/api/score/results")
@login_required
def api_score_results():
    """Get persisted score results with filtering."""
    min_score = int(request.args.get("min_score", 0))
    max_score = int(request.args.get("max_score", 100))
    max_price = float(request.args.get("max_price", 10000))
    page = int(request.args.get("page", 1))
    size = int(request.args.get("size", 50))

    result_file = SCORE_RESULTS_DIR / "latest.parquet"
    summary_file = SCORE_RESULTS_DIR / "summary.json"

    if not result_file.exists():
        return jsonify({
            "results": [],
            "total": 0,
            "page": page,
            "size": size,
            "scan_time": None,
            "message": "暂无评分数据，请先运行评分扫描"
        })

    try:
        df = pd.read_parquet(result_file)

        # Apply filters
        if min_score > 0:
            df = df[df["total"] >= min_score]
        if max_score < 100:
            df = df[df["total"] <= max_score]
        if max_price < 10000:
            df = df[df["close"] <= max_price]

        total = len(df)

        # Pagination
        start = (page - 1) * size
        df_page = df.iloc[start:start + size]

        # Convert to list of dicts, ensuring numpy types are serializable
        results = []
        for record in df_page.to_dict("records"):
            for k, v in record.items():
                if k == "dimensions" and isinstance(v, str):
                    try:
                        record[k] = json.loads(v)
                    except Exception:
                        pass
                elif isinstance(v, np.ndarray):
                    record[k] = v.tolist()
                elif pd.isna(v):
                    record[k] = None
                elif isinstance(v, (np.integer, np.int64)):
                    record[k] = int(v)
                elif isinstance(v, (np.floating, np.float64)):
                    record[k] = float(v)
            results.append(record)

        # Read scan time from summary
        scan_time = None
        if summary_file.exists():
            with open(summary_file, "r", encoding="utf-8") as f:
                summary = json.load(f)
                scan_time = summary.get("scan_time")

        return jsonify({
            "results": results,
            "total": total,
            "page": page,
            "size": size,
            "scan_time": scan_time,
        })
    except Exception as e:
        return jsonify({
            "results": [],
            "total": 0,
            "page": page,
            "size": size,
            "scan_time": None,
            "error": str(e)
        }), 500


@app.route("/api/score/refresh", methods=["POST"])
@login_required
def api_score_refresh():
    """Incremental refresh score results - only recalculate stocks with updated data."""
    if _score_scan_status["running"]:
        return jsonify({"status": "already_running", "message": "评分扫描正在进行中"})

    result_file = SCORE_RESULTS_DIR / "latest.parquet"
    if not result_file.exists():
        return jsonify({"status": "not_found", "message": "暂无评分数据，请先运行全量扫描"})

    data = request.get_json(silent=True) or {}
    min_score = int(data.get("min_score", 0))
    min_score = max(0, min(100, min_score))
    max_price = float(data.get("max_price", 10000))

    def run_refresh():
        import os
        from datetime import datetime

        # Load existing results
        old_df = pd.read_parquet(result_file)
        old_mtime_map = dict(zip(old_df["code"], old_df["data_mtime"]))

        _score_scan_status.update({
            "running": True, "done": False, "results": [],
            "current": 0, "total": 0, "code": "",
            "min_score": min_score, "_saved": False,
            "message": "正在检查更新...",
        })

        files = sorted(DATA_DIR.glob("*.parquet"))
        # Pre-filter: skip stocks with close > max_price
        files = [f for f in files if _stock_index.get(f.stem, {}).get("close", 0) <= max_price]
        _score_scan_status["total"] = len(files)

        from stock_data.scoring import calc_score

        _scan_cols = [
            "date", "open", "close", "high", "low", "volume",
            "pct_change", "turnover",
            "ma5", "ma7", "ma10", "ma20",
            "vwma5", "vwma10", "vwma20",
            "bb_upper", "bb_middle", "bb_lower", "bb_bandwidth",
            "macd_dif", "macd_dea", "macd_hist",
            "kdj_k", "kdj_d", "kdj_j",
        ]

        def _score_one(f):
            code = f.stem
            try:
                current_mtime = f.stat().st_mtime
                old_mtime = old_mtime_map.get(code, 0)

                # Skip if data hasn't changed
                if current_mtime <= old_mtime:
                    return None

                pf = pq.ParquetFile(f)
                total_rows = pf.metadata.num_rows
                if total_rows < 30:
                    return None
                available = set(pf.schema.names)
                cols = [c for c in _scan_cols if c in available]
                tail = min(total_rows, 150)
                df = pd.read_parquet(f, columns=cols).iloc[-tail:].reset_index(drop=True)
                if len(df) < 30:
                    return None
                result = calc_score(df)
                result["code"] = code
                result["name"] = _stock_names.get(code, "")
                result["close"] = round(float(df["close"].iloc[-1]), 2)
                result["pct_change"] = round(float(df["pct_change"].iloc[-1] or 0), 2)
                result["data_mtime"] = current_mtime
                return result
            except Exception:
                pass
            return None

        updated_results = []
        skipped_count = 0

        for i, f in enumerate(files):
            _score_scan_status["current"] = i + 1
            _score_scan_status["code"] = f.stem
            try:
                r = _score_one(f)
                if r:
                    updated_results.append(r)
                else:
                    skipped_count += 1
            except Exception:
                skipped_count += 1

        # Merge updated results with old data
        updated_codes = set(r["code"] for r in updated_results)
        merged_results = [
            r for r in old_df.to_dict("records")
            if r["code"] not in updated_codes
        ] + updated_results

        # Sort by total score
        merged_results.sort(key=lambda x: x.get("total", 0), reverse=True)
        _score_scan_status["results"] = merged_results

        # Save to disk — serialize 'dimensions' as JSON string
        SCORE_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        flat_results = []
        for r in merged_results:
            fr = {}
            for k, v in r.items():
                if k == "dimensions":
                    fr[k] = json.dumps(v, ensure_ascii=False)
                else:
                    fr[k] = v
            flat_results.append(fr)
        result_df = pd.DataFrame(flat_results)
        result_df.to_parquet(result_file, index=False)

        # Update summary
        summary_file = SCORE_RESULTS_DIR / "summary.json"
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump({
                "scan_time": datetime.now().isoformat(timespec="seconds"),
                "total_stocks": len(merged_results),
                "high_score_count": len([r for r in merged_results if r["total"] >= min_score]),
                "min_score_filter": min_score,
                "last_refresh": datetime.now().isoformat(timespec="seconds"),
                "updated_count": len(updated_results),
                "skipped_count": skipped_count,
            }, f, ensure_ascii=False)

        _score_scan_status["running"] = False
        _score_scan_status["done"] = True
        _score_scan_status["_saved"] = True
        _score_scan_status["message"] = f"刷新完成，更新 {len(updated_results)} 只股票，跳过 {skipped_count} 只未变化股票"

    t = threading.Thread(target=run_refresh, daemon=True)
    t.start()
    return jsonify({"status": "started", "message": "评分刷新已启动"})


# === Strategy APIs ===

@app.route("/api/strategies")
@login_required
def api_strategies():
    """List all available strategy JSON files."""
    strategies = []
    for f in sorted(STRATEGIES_DIR.glob("*.json"),reverse=True):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            strategies.append({
                "filename": f.stem,
                "name": data.get("name", ""),
                "description": data.get("description", ""),
                "tags": data.get("tags", []),
                "version": data.get("version", ""),
                "rules": data.get("filter", {}).get("rules", []),
            })
        except Exception:
            pass
    return jsonify(strategies)


@app.route("/api/strategy/scan", methods=["POST"])
@login_required
def api_strategy_scan():
    """Start strategy-based scan in background."""
    if _strategy_scan_status["running"]:
        return jsonify({"status": "already_running", "message": "策略扫描正在进行中"})

    data = request.get_json(silent=True) or {}
    strategy_file = data.get("strategy", "").strip()
    if not strategy_file:
        return jsonify({"error": "请提供策略文件名 (strategy)"}), 400

    strategy_path = STRATEGIES_DIR / f"{strategy_file}.json"
    if not strategy_path.exists():
        return jsonify({"error": f"策略文件 {strategy_file} 不存在"}), 404

    with open(strategy_path, "r", encoding="utf-8") as f:
        strategy_def = json.load(f)

    def run_strategy_scan():
        import os
        from stock_data.strategy_engine import StrategyEngine

        engine = StrategyEngine(strategy_def)
        _strategy_scan_status.update({
            "running": True, "done": False, "results": [],
            "current": 0, "total": 0, "code": "",
            "strategy_name": strategy_def.get("name", ""),
            "strategy_file": strategy_file,
            "_saved": False,
            "message": "正在扫描...",
        })

        # Determine required columns for pruning
        req_cols = engine.get_required_columns()

        files = sorted(DATA_DIR.glob("*.parquet"))
        _strategy_scan_status["total"] = len(files)

        def _scan_one(f):
            code = f.stem
            try:
                pf = pq.ParquetFile(f)
                total_rows = pf.metadata.num_rows
                if total_rows < 30:
                    return None
                available = set(pf.schema.names)
                cols = [c for c in req_cols if c in available]
                tail = min(total_rows, 200)
                df = pd.read_parquet(f, columns=cols).iloc[-tail:].reset_index(drop=True)
                if len(df) < 30:
                    return None
                stock_name = _stock_names.get(code, "")
                if engine.evaluate(df, code, stock_name):
                    return {
                        "code": code,
                        "name": stock_name,
                        "close": round(float(df["close"].iloc[-1]), 2),
                        "pct_change": round(float(df["pct_change"].iloc[-1] or 0), 2),
                    }
            except Exception:
                pass
            return None

        for i, f in enumerate(files):
            _strategy_scan_status["current"] = i + 1
            _strategy_scan_status["code"] = f.stem
            try:
                r = _scan_one(f)
                if r:
                    _strategy_scan_status["results"].append(r)
            except Exception:
                pass

        _strategy_scan_status["running"] = False
        _strategy_scan_status["done"] = True
        _strategy_scan_status["message"] = (
            f"扫描完成，{len(_strategy_scan_status['results'])} 只股票匹配策略"
        )

    t = threading.Thread(target=run_strategy_scan, daemon=True)
    t.start()
    return jsonify({"status": "started", "message": f"策略扫描已启动: {strategy_def.get('name', '')}"})


@app.route("/api/strategy/scan/status")
@login_required
def api_strategy_scan_status():
    """Check strategy scan progress."""
    status = {
        "running": _strategy_scan_status["running"],
        "done": _strategy_scan_status["done"],
        "message": _strategy_scan_status["message"],
        "current": _strategy_scan_status["current"],
        "total": _strategy_scan_status["total"],
        "strategy_name": _strategy_scan_status["strategy_name"],
        "found": len(_strategy_scan_status["results"]),
        "results": list(_strategy_scan_status["results"]),
        "strategy_file": _strategy_scan_status.get("strategy_file", ""),
    }
    # Auto-save results when done
    if status["done"] and status["results"] and not _strategy_scan_status.get("_saved"):
        sf = _strategy_scan_status.get("strategy_file", "")
        if sf:
            from datetime import datetime
            STRATEGY_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
            result_path = STRATEGY_RESULTS_DIR / f"{sf}.json"
            with open(result_path, "w", encoding="utf-8") as f:
                json.dump({
                    "strategy_name": status["strategy_name"],
                    "scan_time": datetime.now().isoformat(timespec="seconds"),
                    "count": len(status["results"]),
                    "results": status["results"],
                }, f, ensure_ascii=False)
            _strategy_scan_status["_saved"] = True
    return jsonify(status)


@app.route("/strategy")
@login_required
def page_strategy():
    return send_from_directory("static", "strategy.html")


# ---------------------------------------------------------------------------
# Backtest
# ---------------------------------------------------------------------------

_BACKTEST_ACTION_SCORE_MAP = {
    "强烈买入": 78, "建议买入": 63, "偏多观望": 50,
    "观望": 40, "建议卖出": 25, "强烈卖出": 0,
}


def _check_condition(cond_type, cond_op, cond_value, score, action):
    """Check if a buy/sell condition is met."""
    if cond_type == "score":
        if cond_op == ">=":
            return score >= cond_value
        elif cond_op == ">":
            return score > cond_value
        elif cond_op == "<=":
            return score <= cond_value
        elif cond_op == "<":
            return score < cond_value
        elif cond_op == "=":
            return score == cond_value
    elif cond_type == "action":
        return action == cond_value
    return False


def _backtest_single_stock(code, date_start, date_end,
                           buy_cond, sell_cond,
                           buy_price_type, sell_price_type,
                           price_min, price_max,
                           max_hold_days):
    """Run backtest on a single stock. Returns list of trade dicts or None."""
    from stock_data.scoring import calc_score

    path = DATA_DIR / f"{code}.parquet"
    if not path.exists():
        return None

    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])

    mask = (df["date"] >= date_start) & (df["date"] <= date_end)
    df_range = df[mask].reset_index(drop=True)
    if len(df_range) < 3:
        return None

    # Need enough history before date_start to compute scores
    start_idx_in_full = df.index[df["date"] >= date_start][0]
    if start_idx_in_full < 30:
        return None  # not enough historical data for indicator warm-up

    trade_dates = df_range["date"].tolist()
    n_trade = len(trade_dates)

    trades = []
    holding = False
    buy_price = 0
    buy_date = None

    WINDOW_SIZE = 150
    START_OFFSET = 30

    for t in range(n_trade):
        full_idx = start_idx_in_full + t
        if full_idx < START_OFFSET:
            continue

        win_start = max(0, full_idx - WINDOW_SIZE + 1)
        df_win = df.iloc[win_start:full_idx + 1].copy()
        if len(df_win) < START_OFFSET:
            continue

        try:
            result = calc_score(df_win)
            score = result["total"]
            action = result.get("action", "")
        except Exception:
            continue

        current_date = trade_dates[t]
        current_close = float(df_range.iloc[t]["close"])

        # Price range filter for buy
        if not holding and price_min is not None and current_close < price_min:
            continue
        if not holding and price_max is not None and current_close > price_max:
            continue

        # T+1 execution
        if t + 1 >= n_trade:
            if holding:
                sell_price = current_close
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

        next_row = df_range.iloc[t + 1]

        if not holding:
            # Check buy condition
            if buy_cond:
                bc_type = buy_cond.get("type", "score")
                bc_op = buy_cond.get("op", ">=")
                bc_val = buy_cond.get("value", 90)
                if _check_condition(bc_type, bc_op, bc_val, score, action):
                    buy_price = float(next_row["close" if buy_price_type == "close" else "open"])
                    buy_date = trade_dates[t + 1]
                    holding = True
        else:
            # Max hold days: force sell if exceeded
            if max_hold_days and max_hold_days > 0:
                days_held = (current_date - buy_date).days
                if days_held >= max_hold_days:
                    sell_price = float(next_row["close" if sell_price_type == "close" else "open"])
                    sell_date = trade_dates[t + 1]
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
                        "exit_reason": "max_hold",
                    })
                    holding = False
                    continue

            # Check sell condition
            if sell_cond:
                sc_type = sell_cond.get("type", "score")
                sc_op = sell_cond.get("op", "<")
                sc_val = sell_cond.get("value", 30)
                if _check_condition(sc_type, sc_op, sc_val, score, action):
                    sell_price = float(next_row["close" if sell_price_type == "close" else "open"])
                    sell_date = trade_dates[t + 1]
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
                    holding = False

    # Close remaining position
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

    return trades if trades else None


def _build_backtest_report(all_trades, n_stocks, elapsed):
    """Build backtest statistics report."""
    if not all_trades:
        return {"summary": "无交易记录", "total_trades": 0}

    df = pd.DataFrame(all_trades)
    total_trades = len(df)
    win_trades = len(df[df["return_pct"] > 0])
    loss_trades = len(df[df["return_pct"] <= 0])
    win_rate = win_trades / total_trades * 100 if total_trades > 0 else 0
    avg_return = df["return_pct"].mean()
    median_return = df["return_pct"].median()
    avg_hold = df["hold_days"].mean()
    median_hold = df["hold_days"].median()
    compound = (1 + df["return_pct"] / 100).prod() - 1

    avg_win = df[df["return_pct"] > 0]["return_pct"].mean() if win_trades > 0 else 0
    avg_loss = abs(df[df["return_pct"] <= 0]["return_pct"].mean()) if loss_trades > 0 else 0
    profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else float('inf')

    # Distribution
    bins = [(-999, -10), (-10, -5), (-5, -3), (-3, 0), (0, 3), (3, 5), (5, 10), (10, 999)]
    labels = ["<-10%", "-10~-5%", "-5~-3%", "-3~0%", "0~3%", "3~5%", "5~10%", ">10%"]
    distribution = []
    for (lo, hi), label in zip(bins, labels):
        count = len(df[(df["return_pct"] >= lo) & (df["return_pct"] < hi)])
        distribution.append({"label": label, "count": count, "pct": round(count / total_trades * 100, 1)})

    # Hold days distribution
    hold_bins = [(0, 3), (3, 7), (7, 14), (14, 30), (30, 60), (60, 999)]
    hold_labels = ["1-3天", "4-7天", "8-14天", "15-30天", "31-60天", ">60天"]
    hold_distribution = []
    for (lo, hi), label in zip(hold_bins, hold_labels):
        subset = df[(df["hold_days"] > lo) & (df["hold_days"] <= hi)]
        count = len(subset)
        avg_r = round(subset["return_pct"].mean(), 2) if count > 0 else 0
        wr = round(len(subset[subset["return_pct"] > 0]) / count * 100, 1) if count > 0 else 0
        hold_distribution.append({"label": label, "count": count, "avg_return": avg_r, "win_rate": wr})

    # Monthly stats
    df["sell_month"] = pd.to_datetime(df["sell_date"]).dt.to_period("M").astype(str)
    monthly = df.groupby("sell_month").agg(
        trades=("return_pct", "count"),
        avg_return=("return_pct", "mean"),
        total_return=("return_pct", "sum"),
    ).reset_index()
    monthly_list = []
    for _, row in monthly.iterrows():
        monthly_list.append({
            "month": row["sell_month"],
            "trades": int(row["trades"]),
            "avg_return": round(row["avg_return"], 2),
            "total_return": round(row["total_return"], 1),
        })

    # Top/bottom trades
    top5 = df.nlargest(5, "return_pct")[["code", "buy_date", "sell_date", "hold_days", "return_pct"]].to_dict("records")
    bot5 = df.nsmallest(5, "return_pct")[["code", "buy_date", "sell_date", "hold_days", "return_pct"]].to_dict("records")

    return {
        "total_trades": total_trades,
        "win_trades": win_trades,
        "loss_trades": loss_trades,
        "win_rate": round(win_rate, 1),
        "avg_return": round(avg_return, 2),
        "median_return": round(median_return, 2),
        "avg_hold_days": round(avg_hold, 1),
        "median_hold_days": round(median_hold, 1),
        "compound_return": round(compound * 100, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "profit_loss_ratio": round(profit_loss_ratio, 2),
        "distribution": distribution,
        "hold_distribution": hold_distribution,
        "monthly": monthly_list,
        "top5": top5,
        "bottom5": bot5,
        "n_stocks": n_stocks,
        "elapsed": round(elapsed, 1),
    }


@app.route("/api/backtest", methods=["POST"])
@login_required
def api_backtest():
    """Start backtest in background thread."""
    if _backtest_status["running"]:
        return jsonify({"status": "already_running", "message": "回测正在进行中"})

    data = request.get_json(silent=True) or {}
    date_start = data.get("date_start", "2025-01-01")
    date_end = data.get("date_end", "2026-04-27")
    price_min = data.get("price_min")
    price_max = data.get("price_max")
    buy_cond = data.get("buy_condition")  # e.g. {"type":"score","op":">=","value":90} or {"type":"action","value":"强烈买入"} or null
    sell_cond = data.get("sell_condition")  # e.g. {"type":"score","op":"<","value":30} or null
    buy_price_type = data.get("buy_price", "open")
    sell_price_type = data.get("sell_price", "open")
    sample_size = data.get("sample_size", 200)
    max_hold_days = data.get("max_hold_days")  # e.g. 10, null=unlimited

    if price_min is not None:
        price_min = float(price_min)
    if price_max is not None:
        price_max = float(price_max)
    if max_hold_days is not None:
        max_hold_days = int(max_hold_days)

    print(f"[backtest] params: date={date_start}~{date_end}, buy={buy_cond}, sell={sell_cond}, "
          f"buy_price={buy_price_type}, sell_price={sell_price_type}, "
          f"price={price_min}~{price_max}, sample={sample_size}, max_hold={max_hold_days}")

    def run_backtest():
        import time
        import traceback

        _backtest_status.update({
            "running": True, "done": False, "message": "正在回测...",
            "current": 0, "total": 0,
            "trades": [], "report": None,
        })

        files = sorted(DATA_DIR.glob("*.parquet"))
        codes = [f.stem for f in files]

        if sample_size and len(codes) > sample_size:
            np.random.seed(42)
            codes = list(np.random.choice(codes, min(sample_size, len(codes)), replace=False))

        _backtest_status["total"] = len(codes)
        t0 = time.time()
        all_trades = []
        error_count = 0

        for i, code in enumerate(codes):
            _backtest_status["current"] = i + 1
            _backtest_status["message"] = f"回测中 {i+1}/{len(codes)} - {code}"
            try:
                result = _backtest_single_stock(
                    code, date_start, date_end,
                    buy_cond, sell_cond,
                    buy_price_type, sell_price_type,
                    price_min, price_max,
                    max_hold_days,
                )
                if result:
                    all_trades.extend(result)
            except Exception as e:
                error_count += 1
                if error_count <= 3:
                    traceback.print_exc()

        elapsed = time.time() - t0
        report = _build_backtest_report(all_trades, len(codes), elapsed)

        _backtest_status.update({
            "running": False,
            "done": True,
            "message": f"回测完成，共 {len(all_trades)} 笔交易" + (f"（{error_count}只出错）" if error_count else ""),
            "trades": all_trades,
            "report": report,
        })
        print(f"[backtest] finished: {len(all_trades)} trades, {len(codes)} stocks, {elapsed:.1f}s, report={report is not None}")

    t = threading.Thread(target=run_backtest, daemon=True)
    t.start()
    return jsonify({"status": "started", "message": "回测已启动"})


@app.route("/api/backtest/status")
@login_required
def api_backtest_status():
    """Check backtest progress and get results."""
    return jsonify({
        "running": _backtest_status["running"],
        "done": _backtest_status["done"],
        "message": _backtest_status["message"],
        "current": _backtest_status["current"],
        "total": _backtest_status["total"],
        "trades": _backtest_status["trades"],
        "report": _backtest_status["report"],
    })


@app.route("/api/strategy/save", methods=["POST"])
@login_required
def api_strategy_save():
    """Create or update a strategy."""
    data = request.get_json(silent=True) or {}
    filename = data.get("filename", "").strip()
    strategy = data.get("strategy")
    if not filename or not strategy:
        return jsonify({"error": "缺少文件名或策略定义"}), 400
    # Sanitize filename
    filename = "".join(c for c in filename if c.isalnum() or c in "_-")
    if not filename:
        return jsonify({"error": "文件名无效"}), 400
    path = STRATEGIES_DIR / f"{filename}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(strategy, f, ensure_ascii=False, indent=2)
    return jsonify({"status": "ok", "filename": filename})


@app.route("/api/strategy/<name>/definition")
@login_required
def api_strategy_definition(name):
    """Get strategy definition JSON."""
    name = "".join(c for c in name if c.isalnum() or c in "_-")
    path = STRATEGIES_DIR / f"{name}.json"
    if not path.exists():
        return jsonify({"error": "策略不存在"}), 404
    with open(path, "r", encoding="utf-8") as f:
        return jsonify(json.load(f))


@app.route("/api/strategy/<name>", methods=["DELETE"])
@login_required
def api_strategy_delete(name):
    """Delete a strategy and its cached results."""
    name = "".join(c for c in name if c.isalnum() or c in "_-")
    path = STRATEGIES_DIR / f"{name}.json"
    if not path.exists():
        return jsonify({"error": "策略不存在"}), 404
    path.unlink()
    result_path = STRATEGY_RESULTS_DIR / f"{name}.json"
    if result_path.exists():
        result_path.unlink()
    return jsonify({"status": "ok"})


@app.route("/api/strategy/<name>/results")
@login_required
def api_strategy_results(name):
    """Get persisted scan results for a strategy."""
    name = "".join(c for c in name if c.isalnum() or c in "_-")
    result_path = STRATEGY_RESULTS_DIR / f"{name}.json"
    if not result_path.exists():
        return jsonify({"results": [], "scan_time": None})
    with open(result_path, "r", encoding="utf-8") as f:
        return jsonify(json.load(f))


@app.route("/api/update", methods=["POST"])
@login_required
def api_update():
    """Trigger incremental data update in background."""
    if _update_status["running"]:
        return jsonify({"status": "already_running", "message": "更新正在进行中"})

    def run_update():
        _update_status["running"] = True
        _update_status["message"] = "正在更新..."
        _update_status["current"] = 0
        _update_status["total"] = 0
        _update_status["code"] = ""
        try:
            from stock_data.update import update_all, fetch_new_listings

            def on_progress(current, total, code):
                _update_status["current"] = current
                _update_status["total"] = total
                _update_status["code"] = code
                _update_status["message"] = f"更新中 {current}/{total} - {code}"

            update_all(progress_cb=on_progress)
            fetch_new_listings()
            build_stock_names(use_cache=False)
            build_stock_index()
            _update_status["message"] = f"更新完成，共 {len(_stock_index)} 只股票"
        except Exception as e:
            _update_status["message"] = f"更新失败: {e}"
        finally:
            _update_status["running"] = False

    t = threading.Thread(target=run_update, daemon=True)
    t.start()
    return jsonify({"status": "started", "message": "更新已启动"})


@app.route("/api/update/status")
@login_required
def api_update_status():
    """Check update progress."""
    _update_status["count"] = len(_stock_index)
    return jsonify(_update_status)


if __name__ == "__main__":
    build_stock_names()
    build_stock_index()
    print("Starting A-Share Stock Viewer...")
    print("Open http://localhost:8888 in your browser")
    app.run(host="0.0.0.0", port=8888, debug=True)
