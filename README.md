# A股主板数据分析平台

一个面向A股主板（沪市60xxxx / 深市00xxxx）的本地化股票数据分析与可视化平台。基于 Flask + ECharts 构建，集成多因子支撑/阻力识别、交易信号检测与降噪过滤算法，提供日K线与分时级别的交互式图表。

---

## 目录

- [功能概览](#功能概览)
- [项目结构](#项目结构)
- [快速开始](#快速开始)
- [使用方式](#使用方式)
  - [数据获取（全量拉取）](#数据获取全量拉取)
  - [增量更新](#增量更新)
  - [启动 Web 服务](#启动-web-服务)
  - [独立运行技术分析](#独立运行技术分析)
  - [独立计算技术指标](#独立计算技术指标)
  - [读取与查询数据](#读取与查询数据)
  - [获取分时数据](#获取分时数据)
  - [完整分析流程示例](#完整分析流程示例)
- [技术指标计算规则](#技术指标计算规则)
  - [移动平均线 MA](#移动平均线-ma)
  - [MACD 指数平滑异同移动平均线](#macd-指数平滑异同移动平均线)
  - [KDJ 随机指标](#kdj-随机指标)
  - [OBV 能量潮指标](#obv-能量潮指标)
  - [量比](#量比)
- [支撑/阻力区算法](#支撑阻力区算法)
  - [多因子候选点生成](#多因子候选点生成)
  - [候选点合并](#候选点合并)
  - [Anti-Trap 后处理规则](#anti-trap-后处理规则)
- [交易信号检测](#交易信号检测)
  - [信号类型总览](#信号类型总览)
  - [MA 均线交叉](#ma-均线交叉)
  - [MACD 交叉](#macd-交叉)
  - [KDJ 交叉](#kdj-交叉)
  - [KDJ 超买超卖与钝化](#kdj-超买超卖与钝化)
  - [量能异动（天量/巨量/地量）](#量能异动天量巨量地量)
  - [均线收敛（MA Squeeze）](#均线收敛ma-squeeze)
  - [MACD 背离](#macd-背离)
  - [量价背离](#量价背离)
- [降噪算法](#降噪算法)
  - [噪声K线过滤](#噪声k线过滤)
  - [信号持续性校验](#信号持续性校验)
  - [成交量确认过滤](#成交量确认过滤)
  - [KDJ 中位区间过滤](#kdj-中位区间过滤)
  - [MACD 柱状图扩张校验](#macd-柱状图扩张校验)
  - [MACD方向一致性校验](#macd方向一致性校验)
  - [量价背离过滤](#量价背离过滤)
  - [信号去重（Dedup）](#信号去重dedup)
- [API 接口](#api-接口)
  - [GET /api/stocks — 股票列表](#get-apistocks--股票列表)
  - [GET /api/stock/code — K线数据](#get-apistockcode--单只股票k线数据)
  - [GET /api/analysis/code — 分析结果](#get-apianalysiscode--分析结果)
  - [GET /api/intraday/code/date — 分时数据](#get-apiintradaycodedate--分时数据)
  - [POST /api/update — 触发更新](#post-apiupdate--触发增量更新)
  - [GET /api/update/status — 更新进度](#get-apiupdatestatus--查询更新进度)
- [数据存储](#数据存储)
  - [日K线数据（27列）](#日k线数据)
  - [分时数据（12列）](#分时数据)
  - [股票名称缓存](#股票名称缓存)
  - [拉取摘要](#拉取摘要)
- [依赖说明](#依赖说明)
- [配置参数](#配置参数)

---

## 功能概览

| 功能 | 说明 |
|------|------|
| 数据获取 | 从 baostock 批量拉取全部 A 股主板约 3000 只股票近 5 年日K数据 |
| 增量更新 | 仅拉取最新交易日数据，自动检测新股并补全历史 |
| 技术指标 | MA（5/7/10/20）、MACD（12/26/9）、KDJ（9/3/3）、OBV、量比 |
| 支撑/阻力 | 6 因子评分体系 + 假突破/有效突破/背离识别，输出 Top3 关键价位 |
| 交易信号 | 7 类信号检测：均线交叉、MACD交叉、KDJ交叉/极端、量能异动、均线收敛、MACD背离、量价背离 |
| 降噪过滤 | 噪声K线剔除 + 持续性校验 + 量能确认 + 区间过滤 + 信号去重 |
| 交互式图表 | ECharts 四面板（K线+支撑阻力、成交量、MACD、KDJ），支持缩放/拖拽 |
| 分时数据 | 支持 5 分钟 / 15 分钟级别分时K线，双击日K查看对应日期分时 |

---

## 项目结构

```
stock_analysis/
├── stock_data/                    # 数据获取与分析模块
│   ├── config.py                  # 全局配置（日期范围、指标参数、路径）
│   ├── indicators.py              # 技术指标计算（MA/MACD/KDJ/OBV/量比）
│   ├── analysis.py                # 支撑阻力识别 + 交易信号检测 + 降噪
│   ├── fetcher.py                 # 批量数据拉取（baostock 多进程）
│   ├── update.py                  # 增量更新 + 新股检测
│   ├── intraday_fetcher.py        # 分时（分钟级）数据拉取与缓存
│   ├── bs_manager.py              # baostock 会话管理（线程安全 + 自动重连）
│   └── __init__.py                # 模块入口
├── web/                           # Web 可视化
│   ├── app.py                     # Flask 后端（页面路由 + REST API）
│   └── static/
│       ├── home.html              # 首页（股票列表 + 搜索 + 更新）
│       └── detail.html            # K线详情页（四面板图表 + 分时）
├── data/                          # 数据存储目录（自动创建）
│   ├── stocks/                    # 日K数据（Parquet 格式，每只一个文件）
│   ├── intraday/                  # 分时数据缓存
│   └── stock_names.json           # 股票代码-名称映射缓存
└── requirements.txt               # Python 依赖
```

---

## 快速开始

### 环境要求

- Python 3.10+
- pip

### 安装依赖

```bash
cd stock_analysis
pip install -r requirements.txt
pip install baostock flask
```

### 首次运行

```bash
# 1. 批量拉取全部 A 股主板历史数据（约 3000 只，首次需较长时间）
python3 -m stock_data.fetcher

# 2. 启动 Web 服务
python3 web/app.py

# 3. 浏览器访问
# http://localhost:8080
```

---

## 使用方式

### 1. 数据获取（全量拉取）

**脚本**: `stock_data/fetcher.py`

```bash
# 拉取全部主板股票近 5 年日K数据（默认跳过已存在文件）
python3 -m stock_data.fetcher

# 指定日期范围拉取
python3 -m stock_data.fetcher 20230101 20240101

# 指定起始和结束日期（YYYYMMDD格式）
python3 -m stock_data.fetcher 20200101 20260418
```

**行为说明：**
- 自动获取沪市（60xxxx）+ 深市（00xxxx）全部主板股票列表
- 使用 4 个工作进程并发拉取，请求间隔 0.3s 避免被限流
- 已存在的 Parquet 文件自动跳过（断点续传）
- 拉取完成后自动计算全部技术指标（MA/MACD/KDJ/OBV）并保存
- 数据源：baostock（免费、稳定、无需 API Key）
- 存储格式：Parquet（列式存储，读取高效）

**输出示例：**
```
2024-04-18 10:00:00 [INFO] Found 3098 main board stocks
Fetching stocks: 100%|██████████| 3098/3098 [12:30<00:00,  4.13it/s]
2024-04-18 10:12:30 [INFO] Done. OK: 3050, Skipped: 0, No data: 48
```

---

### 2. 增量更新

**脚本**: `stock_data/update.py`

```bash
# 更新全部已存在股票（仅拉取最后日期之后的新数据）+ 检测新股
python3 -m stock_data.update

# 仅更新指定的几只股票
python3 -m stock_data.update 600519 000858 000001

# 无参数 = 更新全部 + 拉取新股
python3 -m stock_data.update
```

**行为说明：**
- 读取每只股票 Parquet 文件的最后日期，仅拉取之后的新数据
- 合并新旧数据后全量重算所有技术指标
- 自动检测最新已完成交易日（A 股 15:00 收盘，15:00 前取前一交易日，跳过周末）
- 无参数运行时还会检测新上市股票并补拉全量历史
- 适合配置为定时任务（如每日收盘后运行）

**定时更新示例（crontab）：**
```bash
# 每个交易日 15:30 自动更新（假设部署在 Linux 服务器）
30 15 * * 1-5 cd /path/to/stock_analysis && python3 -m stock_data.update >> /var/log/stock_update.log 2>&1
```

也可通过 Web 界面首页的"更新数据"按钮触发后台增量更新。

---

### 3. 启动 Web 服务

**脚本**: `web/app.py`

```bash
python3 web/app.py
# 访问 http://localhost:8080
```

**启动流程：**
1. 加载股票名称缓存（`data/stock_names.json`），缓存不存在则从 baostock 拉取
2. 扫描 `data/stocks/` 目录，构建股票索引（仅读取每个文件的最后一行）
3. 启动 Flask 开发服务器，监听 `0.0.0.0:8080`

**页面操作说明：**

| 操作 | 说明 |
|------|------|
| 搜索框 | 输入股票代码或名称，实时筛选 |
| 更新数据 | 后台触发增量更新，显示进度 |
| 点击股票行 | 跳转至 K 线详情页 |
| K 线图缩放 | 鼠标滚轮或拖拽底部导航条 |
| 双击 K 线柱 | 打开当日分时图 |
| 分时切换 | 在分时弹窗中切换 分时/5分钟/15分钟 |
| 键盘操作 | ESC 关闭分时弹窗，左右箭头切换前后交易日 |

---

### 4. 独立运行技术分析

**模块**: `stock_data/analysis.py`

可以直接在 Python 中调用分析模块，对任意股票进行支撑阻力区计算和信号检测：

```python
from stock_data.reader import load_stock
from stock_data.analysis import analyze_stock, calc_support_resistance, calc_signals

# 加载股票数据（需包含指标列）
df = load_stock("600519")

# ===== 完整分析（支撑阻力 + 全部信号） =====
result = analyze_stock(df)
print("阻力区:", result["zones"]["resistance"])
print("支撑区:", result["zones"]["support"])
print("MA交叉信号:", result["signals"]["ma"])
print("MACD交叉:", result["signals"]["macd"])
print("MACD背离:", result["signals"]["macdDiv"])
print("KDJ信号:", result["signals"]["kdj"])
print("量能异动:", result["signals"]["vol"])
print("均线收敛:", result["signals"]["squeeze"])

# ===== 仅计算支撑阻力区 =====
zones = calc_support_resistance(df, lookback=120)
for z in zones["resistance"]:
    print(f"阻力 {z['low']}-{z['high']} 评分:{z['score']} 状态:{z['status']} {z['tag']}")

# ===== 仅检测交易信号 =====
signals = calc_signals(df)
for s in signals["vol"]:
    label = {3: "天量", 1: "巨量", 0: "地量"}.get(s["g"], "")
    print(f"{s['d']} {label} 成交量={s['v']}")

# ===== 自定义回望窗口 =====
zones = calc_support_resistance(df, lookback=60)  # 只看最近60个交易日
```

**输出示例：**
```python
>>> zones["resistance"][0]
{'low': 1850.32, 'high': 1872.56, 'score': 78, 'status': 'trap', 'tag': '假突破2次'}

>>> signals["ma"][0]
{'d': '2024-03-15', 'v': 1825.4, 'g': 1, 'nm': '5/10'}  # MA5上穿MA10（金叉）
```

---

### 5. 独立计算技术指标

**模块**: `stock_data/indicators.py`

可以单独对 DataFrame 计算技术指标，适用于自定义数据分析场景：

```python
import pandas as pd
from stock_data.indicators import calc_ma, calc_macd, calc_kdj, calc_volume_indicators, add_all_indicators

# 方式一：一次性计算全部指标
df = pd.read_parquet("data/stocks/600519.parquet")
df = add_all_indicators(df)   # 按顺序计算 MA -> MACD -> KDJ -> OBV/量比

# 方式二：仅计算需要的指标
df = calc_ma(df, periods=[5, 10, 20])       # 只算均线
df = calc_macd(df)                           # 只算 MACD
df = calc_kdj(df)                            # 只算 KDJ
df = calc_volume_indicators(df)              # 只算 OBV 和量比

# 方式三：对自定义数据计算指标
# （比如从其他数据源获取的 DataFrame）
custom_df = pd.DataFrame({
    "date": [...], "open": [...], "close": [...],
    "high": [...], "low": [...], "volume": [...]
})
custom_df = add_all_indicators(custom_df)

# 方式四：对分时数据计算指标（MA + MACD，不含 KDJ/OBV）
from stock_data.indicators import add_intraday_indicators
intraday_df = add_intraday_indicators(custom_df, ma_periods=[5, 20])
```

**注意**: `calc_kdj` 和 `calc_macd` 依赖 `calc_ma` 中产生的均线数据吗？不依赖，它们各自独立。但 `add_all_indicators` 会按顺序执行所有计算。

---

### 6. 读取与查询数据

**模块**: `stock_data/reader.py`

```python
from stock_data.reader import load_stock, load_multiple, load_all_as_panel, filter_by_date, list_available_stocks, get_stock_info

# 列出所有已下载的股票代码
codes = list_available_stocks()
print(f"共 {len(codes)} 只股票")

# 加载单只股票全部数据
df = load_stock("600519")
print(df.columns.tolist())  # 查看所有列
print(df.tail())             # 最近几日数据

# 加载多只股票（返回字典）
data = load_multiple(["600519", "000858", "000001"])
for code, df in data.items():
    print(f"{code}: {len(df)} rows")

# 加载全部股票到一个大 DataFrame
panel = load_all_as_panel()
print(f"共 {len(panel)} 行数据")

# 按日期范围筛选
recent = filter_by_date(df, start="2025-01-01", end="2026-04-17")
recent = filter_by_date(df, start="2025-01-01")  # 只设起始
recent = filter_by_date(df, end="2026-04-17")     # 只设结束

# 获取股票概要信息
info = get_stock_info("600519")
# {'code': '600519', 'date_range': ('2021-04-19', '2026-04-18'),
#  'total_rows': 1218, 'latest_close': 1680.0, 'latest_volume': 23456789}
```

---

### 7. 获取分时数据

**模块**: `stock_data/intraday_fetcher.py`

```python
from stock_data.intraday_fetcher import get_intraday_data

# 获取 5 分钟 K 线（默认）
df_5min = get_intraday_data("600519", "20260418", freq="5")

# 获取 15 分钟 K 线
df_15min = get_intraday_data("600519", "20260418", freq="15")

# 日期格式也支持 YYYY-MM-DD
df = get_intraday_data("600519", "2026-04-18", freq="5")

print(df.columns.tolist())
# ['time', 'open', 'close', 'high', 'low', 'volume', 'amount',
#  'ma5', 'ma20', 'macd_dif', 'macd_dea', 'macd_hist']

print(df.head())
#     time   open  close   high    low    volume     amount
# 0  09:30  1680  1682  1683  1679  1234567  2073000000
# 1  09:35  1682  1681  1684  1680   987654  1660000000
```

**缓存策略：**
- 历史日期：首次从 baostock 拉取后缓存为 Parquet，后续直接读缓存
- 当日数据：仅 15:00（收盘）后才会缓存，盘中每次重新拉取
- 缓存路径：`data/intraday/{CODE}/{DATE}_{freq}min.parquet`

---

### 8. 完整分析流程示例

以下是一个从数据获取到分析输出的完整 Python 脚本示例：

```python
"""
独立分析脚本示例：对指定股票进行完整的技术分析
"""

from stock_data.reader import load_stock, filter_by_date
from stock_data.analysis import analyze_stock

# ===== 配置 =====
CODE = "600519"          # 股票代码
START = "2025-01-01"     # 分析起始日期
END = None               # None = 到最新

# ===== 1. 加载数据 =====
df = load_stock(CODE)
if START or END:
    df = filter_by_date(df, start=START, end=END)

print(f"=== {CODE} 分析报告 ===")
print(f"数据范围: {df['date'].min().date()} ~ {df['date'].max().date()}")
print(f"最新收盘: {df['close'].iloc[-1]:.2f}")
print(f"最新涨跌: {df['pct_change'].iloc[-1]:.2f}%")
print()

# ===== 2. 运行完整分析 =====
result = analyze_stock(df)
zones = result["zones"]
signals = result["signals"]

# ===== 3. 输出支撑阻力区 =====
print("--- 阻力区 ---")
for z in zones["resistance"]:
    print(f"  [{z['low']:.2f} ~ {z['high']:.2f}] 评分:{z['score']} 状态:{z['status']} {z['tag']}")

print("--- 支撑区 ---")
for z in zones["support"]:
    print(f"  [{z['low']:.2f} ~ {z['high']:.2f}] 评分:{z['score']} 状态:{z['status']} {z['tag']}")

# ===== 4. 输出近期信号 =====
print("\n--- 近期交易信号 ---")
for s in signals["ma"][-5:]:
    direction = "金叉" if s["g"] == 1 else "死叉"
    print(f"  {s['d']} MA{s['nm']} {direction} @ {s['v']:.2f}")

for s in signals["macd"][-3:]:
    direction = "金叉" if s["g"] == 1 else "死叉"
    print(f"  {s['d']} MACD {direction} @ {s['v']:.4f}")

for s in signals["vol"][-3:]:
    label = {3: "天量", 1: "巨量", 0: "地量"}.get(s["g"], "")
    print(f"  {s['d']} {label} 成交量:{s['v']:,.0f}")

for s in signals["squeeze"][-3:]:
    direction = {1: "看多", -1: "看空", 0: "中性"}.get(s["g"], "")
    print(f"  {s['d']} 均线收敛 {direction} @ {s['v']:.2f}")

for s in signals["macdDiv"][-3:]:
    label = "顶背离" if s["g"] == 1 else "底背离"
    print(f"  {s['d']} {label} DIF:{s['v']:.4f}")
```

---

## 技术指标计算规则

所有技术指标在 `stock_data/indicators.py` 中计算，参数在 `stock_data/config.py` 中配置。

### 移动平均线 MA

**参数**: 周期 = [5, 7, 10, 20]

**计算方式**: 简单移动平均（SMA）

```
MA_N = (C_1 + C_2 + ... + C_N) / N
```

其中 `C_i` 为第 i 日的收盘价。同时计算成交量均线 `V_MA_N`。

- `min_periods=1`：数据不足 N 天时按实际天数计算
- 结果保留 3 位小数（价格）或整数（成交量）

### MACD 指数平滑异同移动平均线

**参数**: 快线周期 = 12，慢线周期 = 26，信号线周期 = 9

**计算步骤**:

1. **EMA（指数移动平均）**:

```
EMA_N = Close_t * alpha + EMA_N{t-1} * (1 - alpha)
其中 alpha = 2 / (N + 1)
```

2. **DIF（差离值）**:

```
DIF = EMA_12 - EMA_26
```

3. **DEA（信号线）**:

```
DEA = EMA_9(DIF)
```

4. **MACD 柱状图**:

```
HIST = 2 * (DIF - DEA)
```

> 注意：柱状图乘以 2 是为了在图表上更直观地显示红绿柱变化。

### KDJ 随机指标

**参数**: N = 9, M1 = 3, M2 = 3

**计算步骤**:

1. **RSV（未成熟随机值）**:

```
RSV = (C - L_N) / (H_N - L_N) * 100
```

其中 `L_N` 为 N 日内最低价，`H_N` 为 N 日内最高价。空值填充为 50。

2. **K 值**（RSV 的 M1 日指数平滑）:

```
K_t = (M1-1)/M1 * K_{t-1} + 1/M1 * RSV_t
    = 2/3 * K_{t-1} + 1/3 * RSV_t
```

初始值 K_0 = 50

3. **D 值**（K 的 M2 日指数平滑）:

```
D_t = (M2-1)/M2 * D_{t-1} + 1/M2 * K_t
    = 2/3 * D_{t-1} + 1/3 * K_t
```

初始值 D_0 = 50

4. **J 值**:

```
J = 3K - 2D
```

> J 值可超出 [0, 100] 范围，超过 100 为超买信号，低于 0 为超卖信号。

### OBV 能量潮指标

**计算规则**:

```
OBV_t = OBV_{t-1} + sign(C_t - C_{t-1}) * Volume_t
```

| 条件 | sign |
|------|------|
| 收盘价上涨 (C_t > C_{t-1}) | +1 |
| 收盘价下跌 (C_t < C_{t-1}) | -1 |
| 收盘价持平 | 0 |

停牌日（成交量为 NaN）按 0 处理。

### 量比

```
量比 = 今日成交量 / MA5_成交量
```

即当日成交量与近 5 日平均成交量的比值，反映当日放量/缩量程度。

---

## 支撑/阻力区算法

支撑/阻力区识别采用 **多因子评分 + Anti-Trap 后处理** 的架构，在 `stock_data/analysis.py` 的 `calc_support_resistance()` 函数中实现。

### 预计算

在因子分析之前，先计算以下基础数据：

- **ATR（平均真实波幅）**: `TR = max(H-L, |H-C_prev|, |L-C_prev|)` 的 lookback 窗口均值
- **5 日均量**: 过去 5 个有效交易日的平均成交量
- **噪声K线标记**: 波幅 > 3*ATR 且成交量 < 0.7*均量 的K线标记为噪声

### 多因子候选点生成

系统从 6 个维度生成候选支撑/阻力点，每个点携带一个评分（score）：

#### 因子 1: 局部高低点（基础分 10~15）

对 lookback 窗口（默认 120 日）内每一根非噪声K线，判断是否为局部高点或低点：

- **近期K线**（最近 10 根）: 前后各看 2 根K线，判断是否为最高/最低
- **远期K线**: 前后各看 5 根K线，判断是否为最高/最低
- 基础评分：远期且前后均有 5 根以上参考的记 15 分，其他记 10 分
- **影线折扣**: 如果上下影线占比超过 30%（实体太小），评分减半
- **噪声过滤**: 被标记为噪声K线的直接跳过

#### 因子 2: 均线价位（12/18/30 分）

取最后一日的均线值作为候选支撑/阻力位：

| 均线 | 评分 | 逻辑 |
|------|------|------|
| MA5 | 12 | 短期趋势参考 |
| MA10 | 18 | 中期趋势参考 |
| MA20 | 30 | 中长期关键均线，评分最高 |

#### 因子 3: 放量K线（10~25 分）

识别成交量显著放大的K线（量 >= 2*均量），将其最高价和最低价加入候选：

```
放大量比 R = 当日成交量 / 5日均量
- R >= 3: 评分 25 分（爆量）
- 2 <= R < 3: 评分 = 10 + (R - 2) * 15（线性插值）
```

同样过滤噪声K线。

#### 因子 4: 跳空缺口（8/20 分）

检测相邻两日K线之间的跳空缺口：

- **向上跳空**（当日最低 > 前日最高）: 缺口中点价位作为候选
- **向下跳空**（当日最高 < 前日最低）: 缺口中点价位作为候选
- **已回补缺口**: 后续价格曾填补该缺口，评分降至 8 分
- **未回补缺口**: 评分 20 分（更重要的支撑/阻力）

#### 因子 5: 盘整平台（10 分）

检测 10 日内价格横盘整理区间：

```
条件: max(High_10) - min(Low_10) < 1.5 * ATR
```

满足条件时，将 10 日最高价和最低价各记 10 分。盘整平台上下沿是天然支撑/阻力。

#### 因子 6: 近期K线高低点（8 分）

最近 5 根K线的最高价和最低价，各记 8 分。近期价格行为对未来短期走势有直接影响。

### 候选点合并

将所有候选点按价格排序，相邻点（价差 <= max(1%*当前价, 0.5*ATR)）合并为一个区域：

```
阈值 T = max(当前价 * 1%, ATR * 0.5)
```

合并后区域的评分 = 所有包含候选点的评分之和。

### 区域分类与初筛

对每个合并后的区域：

1. **宽度保障**: 区域宽度不足 `0.8%*当前价` 时，以中点为中心扩展
2. **距离过滤**: 超出 `max(20%*当前价, 5*ATR)` 的区域丢弃
3. **最低评分**: 合并后评分 < 15 的区域丢弃

根据区域中点与当前收盘价的关系分为 **阻力区**（中点 > 当前价）和 **支撑区**（中点 <= 当前价）。

### Anti-Trap 后处理规则

对每个区域，从其"出生点"（价格首次离开该区域的位置）开始应用以下规则：

#### 规则 6: 触碰次数加分

统计收盘价进入该区域的次数（连续进入算 1 次）：

| 触碰次数 | 加分 |
|----------|------|
| >= 3 次 | +20 |
| >= 2 次 | +10 |

触碰越多，该价位的有效性越强。

#### 规则 2: 假突破检测（+20~+35 分）

**阻力区假突破**: 某日最高价突破阻力区上沿，但收盘价跌回阻力区下方，且后续 3 日收盘价均维持在阻力区下方。

**支撑区假突破**: 某日最低价跌破支撑区下沿，但收盘价涨回支撑区上方，且后续 3 日收盘价均维持在支撑区上方。

- 检测到假突破: 评分 +20
- 假突破次数 > 1: 额外 +15
- 标记为 `trap`（陷阱）状态

#### 规则 3: 有效突破判定（-25 分）

**阻力区有效突破**: 收盘价突破阻力区上沿，且成交量 > 1.2*均量，后续 2 日收盘价均维持在阻力区上方。

**支撑区有效突破**: 收盘价跌破支撑区下沿，且成交量 > 1.2*均量，后续 2 日收盘价均维持在支撑区下方。

- 检测到有效突破: 评分 -25
- 标记为 `broken`（失效）状态

#### 规则 4: 量价背离（阻力-15 / 支撑+10）

在区域附近检测连续 3 日量价背离：

**阻力区背离**: 高点逐日创新高，但成交量逐日递减（量能不济，上涨动力衰竭）
- 评分 -15（减弱阻力有效性）

**支撑区背离**: 低点逐日创新低，但成交量逐日递减（抛压减轻，下跌动力衰竭）
- 评分 +10（增强支撑有效性）

### 输出

最终输出最多 3 个阻力区和 3 个支撑区，按评分降序排列：

```json
{
  "resistance": [
    {"low": 25.60, "high": 26.10, "score": 85, "status": "trap", "tag": "假突破2次"}
  ],
  "support": [
    {"low": 23.80, "high": 24.20, "score": 72, "status": "normal", "tag": ""}
  ]
}
```

| 字段 | 说明 |
|------|------|
| low / high | 区间上下沿价格 |
| score | 综合评分（0~100），越高越关键 |
| status | `normal`（正常）、`trap`（假突破陷阱）、`broken`（已失效） |
| tag | 附加标签：`假突破N次`、`背离`、`失效` |

---

## 交易信号检测

交易信号检测在 `calc_signals()` 函数中实现，扫描最近 300 个交易日的数据，识别 7 类交易信号。

### 信号类型总览

| 信号键 | 类型 | 说明 |
|--------|------|------|
| `ma` | MA 均线交叉 | MA5/MA10、MA10/MA20 金叉死叉 |
| `macd` | MACD 交叉 | DIF 上穿/下穿 DEA |
| `macdDiv` | MACD 背离 | 顶背离 / 底背离 |
| `kdj` | KDJ 交叉 | K 线上穿/下穿 D 线 |
| `kdjExt` | KDJ 极端 | 超买 (>80) / 超卖 (<20) / 钝化 |
| `vol` | 量能异动 | 天量 / 巨量 / 地量 |
| `squeeze` | 均线收敛 | MA5/10/20 收窄，变盘信号 |
| `volPrice` | 量价背离 | 价格创新高但缩量 / 价格创新低但缩量 |

每个信号条目格式：

```json
{
  "d": "2024-03-15",     // 日期
  "v": 25.60,            // 信号值（价格/指标值/成交量）
  "g": 1,                // 方向: 1=看多, 0=看空, -1=看空(备用), 3=天量
  "nm": "5/10"           // (仅MA信号) 交叉的均线对
}
```

### MA 均线交叉

检测两对均线的金叉（看多）和死叉（看空）：

**MA5 x MA10**:
- 金叉（g=1）: 前一日 MA5 <= MA10，当日 MA5 > MA10
- 死叉（g=0）: 前一日 MA5 >= MA10，当日 MA5 < MA10

**MA10 x MA20**:
- 金叉（g=1）: 前一日 MA10 <= MA20，当日 MA10 > MA20
- 死叉（g=0）: 前一日 MA10 >= MA20，当日 MA10 < MA20

### MACD 交叉

- 金叉（g=1）: 前一日 DIF <= DEA，当日 DIF > DEA
- 死叉（g=0）: 前一日 DIF >= DEA，当日 DIF < DEA

### KDJ 交叉

- 金叉（g=1）: 前一日 K <= D，当日 K > D
- 死叉（g=0）: 前一日 K >= D，当日 K < D

### KDJ 超买超卖与钝化

**超买区**: K > 80 且 D > 70

**超卖区**: K < 20 且 D < 30

**钝化信号**: 在超买/超卖区连续停留 >= 5 天时触发钝化标志（`dh=1`），提示趋势可能过度延伸。

| 信号 | 首次进入 | 钝化（连续5日+） |
|------|----------|------------------|
| 超买 | g=1, dh=0 | g=1, dh=1 |
| 超卖 | g=0, dh=0 | g=0, dh=1 |

### 量能异动（天量/巨量/地量）

基于成交量与 5 日/20 日均量的比较：

| 类型 | 条件 | g 值 |
|------|------|------|
| 天量 | 成交量创 120 日新高 | 3 |
| 巨量 | 创 10 日新高 且 量比>=2.5（相对MA5或MA20） | 1 |
| 地量 | 创 30 日新低 且 量比<0.5（不足均量一半） | 0 |

### 均线收敛（MA Squeeze）

检测 MA5、MA10、MA20 三条均线收敛至极窄区间的变盘信号。

**收敛判定**:

```
Spread = (max(MA5, MA10, MA20) - min(MA5, MA10, MA20)) / Close * 100%
```

1. Spread < 1.2% 且为近 10 日 Spread 的局部最小值
2. 前 7~3 日 Spread 的最大值 > 当前 Spread * 1.3（确认收敛趋势）

**方向评分**:

对收敛信号进行多方向综合评分：

| 评分因素 | 多头得分条件 | 空头得分条件 |
|----------|-------------|-------------|
| 均线排列 | MA5 > MA10 > MA20（多头排列）+3 | MA5 < MA10 < MA20（空头排列）+3 |
| 价格偏离 | 收盘价高于均线中心 >0.5% +2 | 收盘价低于均线中心 >0.5% +2 |
| MA5 趋势 | MA5 高于 5 日前 +1 | MA5 低于 5 日前 +1 |
| 量能配合 | 近 3 日均量 > 前 7~3 日均量*1.15 且价格偏向多 +1 | 同理 |
| MACD 确认 | MACD_HIST > 0.02 +1 | MACD_HIST < -0.02 +1 |

**20日趋势修正**: 如果评分方向与近 20 日价格趋势矛盾，扣除 2 分。

**极端偏离修正**: 价格偏离均线中心 >3% 时进行反向修正。

**输出规则**:

| 条件 | 方向（g） |
|------|-----------|
| 多头排列 + 多头分>=3（趋势不矛盾或分>=5） | 1（看多） |
| 空头排列 + 空头分>=3（趋势不矛盾或分>=5） | -1（看空） |
| 多头分>=4 且领先空头>=2 | 1 |
| 空头分>=4 且领先多头>=2 | -1 |
| Spread < 0.5% 且无明显方向 | 0（中性） |

### MACD 背离

**顶背离**（g=1，看空信号）:
- 价格创新高：Peak_b > Peak_a * 1.015
- DIF 未创新高：DIF_b < DIF_a - 0.01
- 含义：价格上涨但动能衰减，见顶风险

**底背离**（g=0，看多信号）:
- 价格创新低：Trough_b < Trough_a * 0.985
- DIF 未创新低：DIF_b > DIF_a + 0.01
- 含义：价格下跌但抛压减轻，见底可能

峰谷识别：某日收盘价同时高于/低于前后各 2 日收盘价。

### 量价背离

**顶部量价背离**（g=1）:
- 价格创新高：Peak_b > Peak_a * 1.02
- 成交量大幅萎缩：Vol_b < Vol_a * 0.65
- 含义：上涨无量配合，持续性存疑

**底部量价背离**（g=0）:
- 价格创新低：Trough_b < Trough_a * 0.98
- 成交量大幅萎缩：Vol_b < Vol_a * 0.6
- 含义：下跌无量配合，可能见底

---

## 降噪算法

信号降噪分为 **支撑阻力区噪声过滤** 和 **交易信号降噪** 两个层面，目标是剔除虚假信号、保留高置信度信号。

### 噪声K线过滤

在支撑阻力区计算中，预先标记并排除噪声K线：

```
条件: (High - Low) > 3 * ATR  且  成交量 < 0.7 * 5日均量
```

**逻辑**: 波幅极大但成交量极低的K线通常是量化程序或异常交易造成的，其高低点不构成有效支撑/阻力。这些K线在因子 1（局部高低点）、因子 3（放量K线）、因子 4（跳空缺口）的计算中被排除。

### 信号持续性校验（MA Cross）

对 MA 均线交叉信号，验证交叉在下一根K线是否仍然成立：

- 金叉信号要求：下一日 MA5 > MA10（或 MA10 > MA20）仍成立
- 死叉信号要求：下一日 MA5 < MA10（或 MA10 < MA20）仍成立
- 不满足则视为瞬时穿越（假交叉），直接过滤

### 成交量确认过滤

对 MA 交叉、MACD 交叉、KDJ 交叉的金叉信号，要求量能配合：

```
条件: 当日成交量 < 0.7 * 5日均量 -> 过滤
```

**逻辑**: 无量金叉的可靠性较低，可能只是技术性修复而非趋势反转。

### KDJ 中位区间过滤

KDJ 交叉信号在 35 < K < 65 的中位区间直接过滤：

```
条件: 35 < K值 < 65 -> 视为噪声，过滤
```

**逻辑**: KDJ 在中位区的交叉噪音极大，只有在超买/超卖区的交叉才有参考价值。

### MACD 柱状图扩张校验

MACD 交叉信号要求 HIST 柱状图正在扩张：

```
条件: |HIST_t| <= |HIST_{t-1}| * 0.8 -> 过滤
```

**逻辑**: 如果交叉发生时柱状图反而在收缩，说明动能不足，交叉可能不持续。

### MACD方向一致性校验

对均线收敛（Squeeze）信号，要求 MACD 方向一致：

- 看多 Squeeze（g=1）要求 MACD_HIST > -0.05（不能处于明显空头）
- 看空 Squeeze（g=-1）要求 MACD_HIST < 0.05（不能处于明显多头）

### 巨量价格变动过滤

巨量信号要求价格有实质变动：

```
条件: 巨量信号（g=1）但 |涨跌幅| < 1% -> 过滤
```

**逻辑**: 放巨量但价格几乎不动，可能是大单对倒而非真实的方向选择。

### MACD背离 DIF 绝对值过滤

```
条件: |DIF| < 0.02 -> 过滤
```

**逻辑**: DIF 值过小时背离信号意义不大，容易受噪音干扰。

### 信号去重（Dedup）

对每类信号，在时间维度上进行去重，间隔小于阈值的连续信号只保留一个：

| 信号类型 | 去重间隔 |
|----------|----------|
| MA 交叉 | 8 个交易日 |
| MACD 交叉 | 5 个交易日 |
| KDJ 交叉 | 5 个交易日 |
| MACD 背离 | 8 个交易日 |
| 量能异动 | 5 个交易日 |
| 均线收敛 | 10 个交易日 |
| 量价背离 | 8 个交易日 |

**逻辑**: 同一区域密集出现的信号通常反映同一个市场事件，保留第一个即可，避免信号冗余。

---

## API 接口

### 接口总览

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 首页（股票列表） |
| GET | `/stock/<code>` | K线详情页 |
| GET | `/api/stocks?q=&page=&size=` | 股票列表（搜索+分页） |
| GET | `/api/stock/<code>?start=&end=` | 单只股票K线数据 |
| GET | `/api/analysis/<code>?start=&end=` | 支撑阻力区 + 交易信号 |
| GET | `/api/intraday/<code>/<date>?freq=5` | 分时数据（5分钟/15分钟） |
| POST | `/api/update` | 触发后台增量更新 |
| GET | `/api/update/status` | 查询更新进度 |

---

### GET `/api/stocks` — 股票列表

**参数：**

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| q | string | "" | 搜索关键词（匹配代码或名称） |
| page | int | 1 | 页码 |
| size | int | 50 | 每页条数 |

**响应结构：**

```json
{
  "total": 3098,
  "page": 1,
  "size": 50,
  "data": [
    {
      "code": "000001",
      "name": "平安银行",
      "close": 12.35,
      "pct_change": 1.23,
      "date": "2026-04-18"
    }
  ]
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| total | int | 匹配的股票总数 |
| page | int | 当前页码 |
| size | int | 每页条数 |
| data[].code | string | 6 位股票代码 |
| data[].name | string | 股票名称 |
| data[].close | float | 最新收盘价 |
| data[].pct_change | float | 最新涨跌幅（%） |
| data[].date | string | 最新交易日期（YYYY-MM-DD） |

---

### GET `/api/stock/<code>` — 单只股票K线数据

**参数：**

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| start | string | 无 | 起始日期（YYYY-MM-DD） |
| end | string | 无 | 截止日期（YYYY-MM-DD） |

**响应结构：**

```json
{
  "columns": ["date", "open", "close", "high", "low", "volume", "pct_change",
              "ma5", "ma7", "ma10", "ma20",
              "macd_dif", "macd_dea", "macd_hist",
              "kdj_k", "kdj_d", "kdj_j"],
  "data": [
    ["2026-04-18", 1675.0, 1680.0, 1688.5, 1672.0, 23456789, 1.23,
     1670.5, 1668.2, 1665.0, 1658.3,
     5.2341, 3.8912, 2.6858,
     72.351, 65.128, 86.797],
    ...
  ],
  "name": "贵州茅台"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| columns | string[] | 列名数组，与 data 中每行数组一一对应 |
| data | any[][] | 数据行数组，每行为一个交易日的全部字段 |
| name | string | 股票名称 |

**columns 各列含义：**

| 索引 | 列名 | 类型 | 说明 |
|------|------|------|------|
| 0 | date | string | 交易日期 YYYY-MM-DD |
| 1 | open | float/null | 开盘价 |
| 2 | close | float/null | 收盘价 |
| 3 | high | float/null | 最高价 |
| 4 | low | float/null | 最低价 |
| 5 | volume | int/null | 成交量（股） |
| 6 | pct_change | float/null | 涨跌幅（%） |
| 7 | ma5 | float/null | 5 日均线 |
| 8 | ma7 | float/null | 7 日均线 |
| 9 | ma10 | float/null | 10 日均线 |
| 10 | ma20 | float/null | 20 日均线 |
| 11 | macd_dif | float/null | MACD DIF（差离值） |
| 12 | macd_dea | float/null | MACD DEA（信号线） |
| 13 | macd_hist | float/null | MACD 柱状图 |
| 14 | kdj_k | float/null | KDJ K 值 |
| 15 | kdj_d | float/null | KDJ D 值 |
| 16 | kdj_j | float/null | KDJ J 值 |

> NaN 值在 JSON 中序列化为 null。

---

### GET `/api/analysis/<code>` — 分析结果

**参数：**

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| start | string | 无 | 起始日期（YYYY-MM-DD） |
| end | string | 无 | 截止日期（YYYY-MM-DD） |

**响应结构：**

```json
{
  "zones": {
    "resistance": [
      {"low": 1850.32, "high": 1872.56, "score": 78, "status": "trap", "tag": "假突破2次"}
    ],
    "support": [
      {"low": 1620.00, "high": 1635.50, "score": 65, "status": "normal", "tag": ""}
    ]
  },
  "signals": {
    "ma": [
      {"d": "2026-04-10", "v": 1670.5, "g": 1, "nm": "5/10"}
    ],
    "macd": [
      {"d": "2026-04-08", "v": 3.89, "g": 1}
    ],
    "macdDiv": [],
    "kdj": [
      {"d": "2026-04-12", "v": 25.3, "g": 1}
    ],
    "kdjExt": [
      {"d": "2026-04-14", "v": 82.5, "g": 1, "dh": 0}
    ],
    "vol": [
      {"d": "2026-04-15", "v": 56789012, "g": 1}
    ],
    "squeeze": [
      {"d": "2026-04-16", "v": 1680.0, "g": 1}
    ],
    "volPrice": []
  }
}
```

**zones（支撑阻力区）字段说明：**

| 字段 | 类型 | 说明 |
|------|------|------|
| low | float | 区间下沿价格 |
| high | float | 区间上沿价格 |
| score | int | 综合评分（0~100），越高表示该价位越关键 |
| status | string | 状态：`normal`（正常）、`trap`（假突破陷阱）、`broken`（已失效） |
| tag | string | 附加标签：`"假突破N次"`、`"背离"`、`"失效"`、`""`（无） |

**signals（交易信号）字段说明：**

| 字段 | 类型 | 说明 |
|------|------|------|
| d | string | 信号触发日期 YYYY-MM-DD |
| v | float | 信号关联值（价格/指标值/成交量） |
| g | int | 方向：`1`=看多，`0`=看空，`-1`=看空（备用），`3`=天量 |
| nm | string | 仅 MA 信号，交叉的均线对（`"5/10"` 或 `"10/20"`） |
| dh | int | 仅 KDJ 极端信号，`0`=首次进入，`1`=钝化（连续 5 日+） |

**signals 各类别含义：**

| 键 | 含义 | g 值对照 |
|------|------|---------|
| ma | MA 均线交叉 | 1=金叉（看多），0=死叉（看空） |
| macd | MACD 交叉 | 1=DIF 上穿 DEA，0=DIF 下穿 DEA |
| macdDiv | MACD 背离 | 1=顶背离（看空信号），0=底背离（看多信号） |
| kdj | KDJ 交叉 | 1=K 上穿 D，0=K 下穿 D |
| kdjExt | KDJ 超买超卖/钝化 | 1=超买区，0=超卖区；dh=0 首次，dh=1 钝化 |
| vol | 量能异动 | 3=天量，1=巨量，0=地量 |
| squeeze | 均线收敛 | 1=看多变盘，-1=看空变盘，0=中性 |
| volPrice | 量价背离 | 1=顶部背离（看空），0=底部背离（看多） |

> resistance 和 support 数组最多各 3 个元素，按 score 降序排列。

---

### GET `/api/intraday/<code>/<date>` — 分时数据

**参数：**

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| code | path | — | 6 位股票代码 |
| date | path | — | 日期（YYYY-MM-DD） |
| freq | query | "5" | K线周期：`"5"`=5分钟，`"15"`=15分钟 |

**响应结构：**

```json
{
  "columns": ["time", "open", "close", "high", "low", "volume", "amount",
              "ma5", "ma20", "macd_dif", "macd_dea", "macd_hist"],
  "data": [
    ["09:30", 1680.0, 1682.0, 1683.5, 1679.0, 1234567, 2073000000.0,
     null, null, null, null, null],
    ["09:35", 1682.0, 1681.0, 1684.0, 1680.5, 987654, 1660000000.0,
     1681.5, null, null, null, null],
    ["09:40", 1681.0, 1683.0, 1684.0, 1680.0, 876543, 1473000000.0,
     1682.0, null, null, null, null]
  ],
  "date": "2026-04-18",
  "code": "600519",
  "freq": "5"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| columns | string[] | 列名数组 |
| data | any[][] | 分时数据行 |
| date | string | 请求的日期 |
| code | string | 股票代码 |
| freq | string | K线周期 |

**columns 各列含义：**

| 索引 | 列名 | 类型 | 说明 |
|------|------|------|------|
| 0 | time | string | 时间 HH:MM（如 "09:35"） |
| 1 | open | float | 开盘价 |
| 2 | close | float | 收盘价 |
| 3 | high | float | 最高价 |
| 4 | low | float | 最低价 |
| 5 | volume | int | 成交量（股） |
| 6 | amount | float | 成交额（元） |
| 7 | ma5 | float/null | 5 周期均线（前 4 根为 null） |
| 8 | ma20 | float/null | 20 周期均线（前 19 根为 null） |
| 9 | macd_dif | float/null | MACD DIF |
| 10 | macd_dea | float/null | MACD DEA |
| 11 | macd_hist | float/null | MACD 柱状图 |

> 分时数据不计算 KDJ 和 OBV（分钟级别意义不大）。5 分钟K线 A 股一日约 48 根（9:30~11:30, 13:00~15:00）。

---

### POST `/api/update` — 触发增量更新

**请求：** 无请求体

**响应：**

```json
// 成功启动
{"status": "started", "message": "更新已启动"}

// 已有更新在运行
{"status": "already_running", "message": "更新正在进行中"}
```

---

### GET `/api/update/status` — 查询更新进度

**响应结构：**

```json
{
  "running": true,
  "message": "更新中 1500/3098 - 600519",
  "current": 1500,
  "total": 3098,
  "code": "600519",
  "count": 3098
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| running | bool | 是否正在更新 |
| message | string | 进度描述文本 |
| current | int | 已处理的股票数 |
| total | int | 需更新的总股票数 |
| code | string | 当前正在处理的股票代码 |
| count | int | 当前索引中的股票总数 |

---

## 数据存储

### 目录总览

```
data/
├── stocks/                          # 日K线数据
│   ├── 000001.parquet               # 平安银行
│   ├── 000858.parquet               # 五粮液
│   ├── 600519.parquet               # 贵州茅台
│   └── ...                          # 约 3000 个文件
├── intraday/                        # 分时数据缓存
│   ├── 000001/
│   │   ├── 20260418_5min.parquet
│   │   └── 20260418_15min.parquet
│   └── 600519/
│       └── 20260418_5min.parquet
├── stock_names.json                 # 股票代码-名称映射
└── fetch_summary.csv                # 最近一次全量拉取的摘要
```

---

### 日K线数据

**文件路径**: `data/stocks/{CODE}.parquet`

**文件命名**: 6 位股票代码，如 `600519.parquet`、`000001.parquet`

**复权方式**: 前复权（adjustflag=2）

**完整列定义（27 列）：**

| # | 列名 | 类型 | 说明 | 取值范围 / 示例 |
|---|------|------|------|----------------|
| 1 | date | datetime64 | 交易日期 | 2021-04-19 ~ 2026-04-18 |
| 2 | code | string | 6 位股票代码 | "600519" |
| 3 | open | float64 | 开盘价（元） | 1680.0 |
| 4 | close | float64 | 收盘价（元） | 1685.5 |
| 5 | high | float64 | 最高价（元） | 1690.0 |
| 6 | low | float64 | 最低价（元） | 1675.0 |
| 7 | volume | Int64 | 成交量（股，非手） | 23456789 |
| 8 | amount | float64 | 成交额（元） | 39520000000.0 |
| 9 | pct_change | float64 | 涨跌幅（%） | 1.23 |
| 10 | turnover | float64 | 换手率（%） | 0.87 |
| 11 | ma5 | float64 | 5 日收盘价均线 | 1680.333 |
| 12 | v_ma5 | float64 | 5 日成交量均值 | 23456789.0 |
| 13 | ma7 | float64 | 7 日收盘价均线 | 1678.571 |
| 14 | v_ma7 | float64 | 7 日成交量均值 | 22345678.0 |
| 15 | ma10 | float64 | 10 日收盘价均线 | 1675.200 |
| 16 | v_ma10 | float64 | 10 日成交量均值 | 21345678.0 |
| 17 | ma20 | float64 | 20 日收盘价均线 | 1670.150 |
| 18 | v_ma20 | float64 | 20 日成交量均值 | 20345678.0 |
| 19 | macd_dif | float64 | MACD DIF（快慢线差值） | 5.2341 |
| 20 | macd_dea | float64 | MACD DEA（DIF 的信号线） | 3.8912 |
| 21 | macd_hist | float64 | MACD 柱状图 = 2*(DIF-DEA) | 2.6858 |
| 22 | kdj_k | float64 | KDJ K 值 | 72.351 |
| 23 | kdj_d | float64 | KDJ D 值 | 65.128 |
| 24 | kdj_j | float64 | KDJ J 值（= 3K - 2D） | 86.797 |
| 25 | obv | int64 | OBV 能量潮（累积值） | 1234567890 |
| 26 | vol_ratio | float64 | 量比（当日量/MA5量） | 1.352 |

**列排列顺序**：date → code → K线基础（open/close/high/low/volume/amount/pct_change/turnover）→ 均线（ma5/v_ma5/ma7/v_ma7/ma10/v_ma10/ma20/v_ma20）→ MACD（macd_dif/macd_dea/macd_hist）→ KDJ（kdj_k/kdj_d/kdj_j）→ 量能（obv/vol_ratio）

**特殊值处理：**
- 停牌日：volume 为 0 或 NaN，amount/turnover 可能为 NaN
- 首日指标：均线首日（不足 min_periods）使用已有数据计算，MACD/KDJ 起始值固定
- NaN 在 Parquet 中存储为 null，读取后为 `pd.NA` 或 `np.nan`

**数据行示例（用 Python 查看）：**
```python
from stock_data.reader import load_stock
df = load_stock("600519")
print(df.iloc[-1])  # 最新一日
# date         2026-04-18 00:00:00
# code                       600519
# open                        1675.0
# close                       1680.0
# high                        1688.5
# low                         1672.0
# volume                    23456789
# amount                  3.952e+10
# pct_change                    1.23
# turnover                     0.87
# ma5                       1670.333
# v_ma5                   2.346e+07
# ...
```

---

### 分时数据

**文件路径**: `data/intraday/{CODE}/{DATE}_{freq}min.parquet`

**命名规则**:
- `{CODE}`: 6 位股票代码目录，如 `600519/`
- `{DATE}`: YYYYMMDD 格式日期，如 `20260418`
- `{freq}`: `5`（5 分钟）或 `15`（15 分钟）
- 示例: `data/intraday/600519/20260418_5min.parquet`

**完整列定义（12 列）：**

| # | 列名 | 类型 | 说明 | 示例 |
|---|------|------|------|------|
| 1 | time | string | 时间 HH:MM | "09:35" |
| 2 | open | float64 | 开盘价 | 1680.0 |
| 3 | close | float64 | 收盘价 | 1682.0 |
| 4 | high | float64 | 最高价 | 1683.5 |
| 5 | low | float64 | 最低价 | 1679.0 |
| 6 | volume | Int64 | 成交量（股） | 1234567 |
| 7 | amount | float64 | 成交额（元） | 2073000000.0 |
| 8 | ma5 | float64 | 5 周期均线 | 1681.5 |
| 9 | ma20 | float64 | 20 周期均线 | 1678.3 |
| 10 | macd_dif | float64 | MACD DIF | 0.1523 |
| 11 | macd_dea | float64 | MACD DEA | 0.0891 |
| 12 | macd_hist | float64 | MACD 柱状图 | 0.1264 |

> 注意：分时数据 **不计算** KDJ 和 OBV（分钟级别无意义）。

**数据行数**:
- 5 分钟K线: 每个交易日约 48 行（9:30~11:30 共 24 根，13:00~15:00 共 24 根）
- 15 分钟K线: 每个交易日约 16 行

**缓存策略:**
- 历史日期: 首次拉取后缓存，后续直接读文件
- 当日数据: 仅 15:00（A 股收盘）后才写入缓存，盘中每次重新拉取
- 并发安全: 使用 `FileLock` 防止多请求同时写入

---

### 股票名称缓存

**文件路径**: `data/stock_names.json`

**格式**: JSON 对象，key 为 6 位股票代码，value 为股票名称

```json
{
  "000001": "平安银行",
  "000002": "万科A",
  "000858": "五粮液",
  "600519": "贵州茅台",
  ...
}
```

**更新时机**: 启动时加载，全量更新后重建。首次启动从 baostock 拉取，后续优先读缓存。

---

### 拉取摘要

**文件路径**: `data/fetch_summary.csv`

每次运行 `python3 -m stock_data.fetcher` 全量拉取后自动生成，记录每只股票的拉取状态：

| 列名 | 类型 | 说明 |
|------|------|------|
| code | string | 股票代码 |
| name | string | 股票名称 |
| status | string | 状态：`ok`（成功）、`skipped`（跳过）、`no_data`（无数据）、`error`（失败） |
| rows | int | 获取到的数据行数 |

```csv
code,name,status,rows
600519,贵州茅台,ok,1218
000001,平安银行,ok,1218
688001,...,no_data,0
```

---

## 依赖说明

### 核心依赖（requirements.txt）

| 包 | 版本 | 用途 |
|---|------|------|
| akshare | >=1.14.0 | A 股数据接口（备用数据源） |
| pandas | >=2.0.0 | 数据处理框架（DataFrame 操作） |
| numpy | >=1.24.0 | 数值计算（MA/MACD/KDJ 算法底层） |
| pyarrow | >=14.0.0 | Parquet 文件读写引擎 |
| tqdm | >=4.65.0 | 命令行进度条（批量拉取/更新时显示） |
| filelock | >=3.12.0 | 文件锁（分时数据并发写入保护） |

### 额外依赖（需手动安装）

| 包 | 用途 | 安装 |
|---|------|------|
| baostock | 主数据源（A股历史K线、分时数据） | `pip install baostock` |
| flask | Web 框架（后端 API + 页面服务） | `pip install flask` |

### 一键安装

```bash
pip install -r requirements.txt baostock flask
```

---

## 配置参数

所有配置集中在 `stock_data/config.py`：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `MA_PERIODS` | [5, 7, 10, 20] | MA 均线周期列表 |
| `MACD_FAST` | 12 | MACD 快线周期 |
| `MACD_SLOW` | 26 | MACD 慢线周期 |
| `MACD_SIGNAL` | 9 | MACD 信号线周期 |
| `KDJ_N` | 9 | KDJ RSV 回望周期 |
| `KDJ_M1` | 3 | KDJ K 值平滑系数 |
| `KDJ_M2` | 3 | KDJ D 值平滑系数 |
| `BATCH_WORKERS` | 4 | 并发工作进程数 |
| `REQUEST_INTERVAL` | 0.3s | API 请求间隔（防限流） |
| `START_DATE` | 5 年前 | 默认数据起始日期 |
| `END_DATE` | 今天 | 默认数据截止日期 |

支撑阻力区参数（`analysis.py` 内）：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `lookback` | 120 | 回望窗口（交易日） |
| 触碰加分阈值 | >=2/+10, >=3/+20 | 区域触碰次数评分 |
| 假突破确认 | 后续 3 日 | 假突破后维持天数 |
| 有效突破确认 | 后续 2 日 + 量>1.2*均量 | 有效突破条件 |
| 最大输出 | 3 | 每侧最多输出区域数 |
