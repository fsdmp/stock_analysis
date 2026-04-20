"""Flask backend for A-share stock data visualization."""

import sys
import json
import atexit
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from flask import Flask, jsonify, request, send_from_directory
import pandas as pd
import pyarrow.parquet as pq

from stock_data.bs_manager import bs_shutdown

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "stocks"
CACHE_DIR = Path(__file__).resolve().parent.parent / "data"
STOCK_NAMES_CACHE = CACHE_DIR / "stock_names.json"
app = Flask(__name__, static_folder="static", static_url_path="")
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


# === Pages ===

@app.route("/")
def page_home():
    return send_from_directory("static", "home.html")


@app.route("/stock/<code>")
def page_detail(code):
    return send_from_directory("static", "detail.html")


@app.route("/recommend")
def page_recommend():
    return send_from_directory("static", "recommend.html")


# === APIs ===

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
    """Return next-day trading score for a stock."""
    code = code.zfill(6)
    path = DATA_DIR / f"{code}.parquet"
    if not path.exists():
        return jsonify({"error": f"Stock {code} not found"}), 404

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
def api_batch_score():
    """Score multiple stocks and return ranked results."""
    codes = request.get_json(silent=True) or []
    if not codes or not isinstance(codes, list):
        return jsonify({"error": "请提供股票代码列表"}), 400

    codes = [c.zfill(6) for c in codes if isinstance(c, str) and c.strip()]
    if not codes:
        return jsonify({"error": "无有效股票代码"}), 400

    results = []
    for code in codes:
        path = DATA_DIR / f"{code}.parquet"
        if not path.exists():
            results.append({
                "code": code, "name": _stock_names.get(code, ""),
                "error": "未找到数据",
            })
            continue

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
            from stock_data.scoring import calc_score
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
def api_full_scan():
    """Start full market scan for high-score stocks in background."""
    if _full_scan_status["running"]:
        return jsonify({"status": "already_running", "message": "扫描正在进行中"})

    data = request.get_json(silent=True) or {}
    min_score = int(data.get("min_score", 95))
    min_score = max(0, min(100, min_score))
    max_price = float(data.get("max_price", 100))

    def run_scan():
        import os
        _full_scan_status.update({
            "running": True, "done": False, "results": [],
            "min_score": min_score, "current": 0, "total": 0,
            "code": "", "message": "正在扫描...",
        })

        files = sorted(DATA_DIR.glob("*.parquet"))
        # Pre-filter: skip stocks with close > max_price
        files = [f for f in files if _stock_index.get(f.stem, {}).get("close", 0) <= max_price]
        _full_scan_status["total"] = len(files)

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
                # Only read the last 150 rows — scoring only needs recent data
                tail = min(total_rows, 150)
                df = pd.read_parquet(f, columns=cols).iloc[-tail:].reset_index(drop=True)
                if len(df) < 30:
                    return None
                result = calc_score(df)
                if result["total"] >= min_score:
                    result["code"] = code
                    result["name"] = _stock_names.get(code, "")
                    result["close"] = round(float(df["close"].iloc[-1]), 2)
                    result["pct_change"] = round(
                        float(df["pct_change"].iloc[-1] or 0), 2
                    )
                    return result
            except Exception:
                pass
            return None

        workers = min(os.cpu_count() or 4, 8)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_score_one, f): f for f in files}
            for future in as_completed(futures):
                code = futures[future].stem
                _full_scan_status["current"] += 1
                _full_scan_status["code"] = code
                try:
                    r = future.result()
                    if r:
                        _full_scan_status["results"].append(r)
                except Exception:
                    pass

        _full_scan_status["results"].sort(
            key=lambda x: x.get("total", 0), reverse=True
        )
        _full_scan_status["running"] = False
        _full_scan_status["done"] = True
        _full_scan_status["message"] = (
            f"扫描完成，{len(_full_scan_status['results'])} 只股票评分≥{min_score}"
        )

    t = threading.Thread(target=run_scan, daemon=True)
    t.start()
    return jsonify({"status": "started", "message": "全量扫描已启动"})


@app.route("/api/full-scan/status")
def api_full_scan_status():
    """Check full scan progress."""
    status = {
        "running": _full_scan_status["running"],
        "done": _full_scan_status["done"],
        "message": _full_scan_status["message"],
        "current": _full_scan_status["current"],
        "total": _full_scan_status["total"],
        "code": _full_scan_status["code"],
        "min_score": _full_scan_status["min_score"],
        "found": len(_full_scan_status["results"]),
        "results": list(_full_scan_status["results"]),
    }
    return jsonify(status)


@app.route("/api/update", methods=["POST"])
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
