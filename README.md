# A股主板数据分析平台

拉取A股全部主板个股近5年历史数据，计算技术指标，支持可视化分析。

## 快速开始

### 1. 安装依赖

```bash
pip3 install -r requirements.txt
```

### 2. 拉取全量数据（首次）

```bash
python3 -m stock_data.fetcher
```

拉取约3500只主板个股，每只5年日线数据。支持断点续传，中断后重新运行会跳过已下载的股票。

### 3. 启动可视化页面

```bash
python3 web/app.py
```

浏览器打开 http://localhost:8080

---

## 项目结构

```
p/
├── stock_data/              # 数据拉取与处理
│   ├── config.py            # 配置（日期范围、指标参数）
│   ├── indicators.py        # 技术指标计算（MA/MACD/KDJ/OBV）
│   ├── fetcher.py           # 全量数据拉取（数据源：baostock）
│   ├── update.py            # 增量更新 + 新股检测
│   └── reader.py            # 数据读取工具
├── web/                     # Web可视化
│   ├── app.py               # Flask 后端（API + 页面路由）
│   └── static/
│       ├── home.html        # 主页（股票列表）
│       └── detail.html      # K线详情页
├── data/                    # 数据存储
│   └── stocks/              # 每只股票一个 Parquet 文件（如 600519.parquet）
└── requirements.txt
```

---

## 脚本说明

### `python3 -m stock_data.fetcher` — 全量拉取

拉取所有主板个股（60xxxx沪市 + 00xxxx深市）近5年前复权日线数据，自动计算技术指标并保存为 Parquet 文件。

- 支持断点续传，已下载的股票自动跳过
- 可指定日期范围：`python3 -m stock_data.fetcher 20230101 20260418`

### `python3 -m stock_data.update` — 增量更新

检测已有数据文件的最后日期，仅拉取新增部分，合并后重新计算指标。同时自动发现新上市股票并补拉全量数据。

- 指定股票更新：`python3 -m stock_data.update 600519 000001`
- 无参数则更新全部已有股票

### `python3 web/app.py` — 启动Web服务

启动后访问：

| 页面 | 地址 | 功能 |
|------|------|------|
| 主页 | http://localhost:8080 | 股票列表、搜索、分页、数据更新 |
| 详情 | http://localhost:8080/stock/600519 | K线图 + 技术指标 |

### 在代码中读取数据

```python
from stock_data.reader import load_stock, load_all_as_panel, filter_by_date

df = load_stock("600519")                          # 单只股票
panel = load_all_as_panel()                         # 全部股票合并
recent = filter_by_date(df, "2025-01-01", "2026-04-17")  # 日期筛选
```

---

## 数据字段

每只股票保存为 `data/stocks/{code}.parquet`，包含以下字段：

| 类别 | 字段 |
|------|------|
| K线 | date, code, open, close, high, low, volume, amount, pct_change, turnover |
| 均线 | ma5, ma7, ma10, ma20, v_ma5, v_ma7, v_ma10, v_ma20 |
| MACD | macd_dif, macd_dea, macd_hist |
| KDJ | kdj_k, kdj_d, kdj_j |
| 量能 | obv, vol_ratio |

**指标参数：**
- MA 周期：5, 7, 10, 20
- MACD：快线12，慢线26，信号线9
- KDJ：N=9, M1=3, M2=3
- 复权方式：前复权
- 成交量单位：股（非手）
