"""Flask backend for A-share stock data visualization."""

import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from flask import Flask, jsonify, request, send_from_directory
import pandas as pd
import pyarrow.parquet as pq

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "stocks"

app = Flask(__name__, static_folder="static", static_url_path="")

# Pre-build stock index on startup
_stock_index: dict = {}
_update_status = {"running": False, "message": ""}


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
                "close": round(float(last_batch.column("close")[last_idx].as_py()), 2),
                "pct_change": round(float(last_batch.column("pct_change")[last_idx].as_py() or 0), 2),
                "date": str(pd.Timestamp(last_batch.column("date")[last_idx].as_py()).date()),
            }
        except Exception:
            _stock_index[f.stem] = {"code": f.stem, "close": 0, "pct_change": 0, "date": ""}
    print(f"Indexed {len(_stock_index)} stocks")


# === Pages ===

@app.route("/")
def page_home():
    return send_from_directory("static", "home.html")


@app.route("/stock/<code>")
def page_detail(code):
    return send_from_directory("static", "detail.html")


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
        all_list = [s for s in all_list if q in s["code"]]

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
        "date", "open", "close", "high", "low", "volume", "pct_change",
        "ma5", "ma7", "ma10", "ma20",
        "macd_dif", "macd_dea", "macd_hist",
        "kdj_k", "kdj_d", "kdj_j",
    ]
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
    return jsonify({"columns": cols, "data": df.values.tolist()})


@app.route("/api/update", methods=["POST"])
def api_update():
    """Trigger incremental data update in background."""
    if _update_status["running"]:
        return jsonify({"status": "already_running", "message": "更新正在进行中"})

    def run_update():
        _update_status["running"] = True
        _update_status["message"] = "正在更新..."
        try:
            from stock_data.update import update_all, fetch_new_listings
            update_all()
            fetch_new_listings()
            build_stock_index()
            _update_status["message"] = f"更新完成，共 {_update_status.get('count', 0)} 只股票"
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
    build_stock_index()
    print("Starting A-Share Stock Viewer...")
    print("Open http://localhost:8080 in your browser")
    app.run(host="0.0.0.0", port=8080, debug=True)
