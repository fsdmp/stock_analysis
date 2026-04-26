# 策略描述语言规范 v2.0 (Strategy DSL)

## 一、策略结构

```json
{
  "name": "策略名称",
  "description": "策略描述",
  "version": "2.0",
  "tags": ["标签"],
  "filter": {
    "logic": "AND | OR",
    "rules": [
      { "field": "...", "op": "...", "value": "...", "ref": "...", "offset": 0, "lookback": 0, "aggregate": "" },
      { "logic": "OR", "rules": [...] }
    ]
  }
}
```

- `filter.rules` 中每项要么是**条件**（含 field），要么是**子逻辑组**（含 logic + rules）
- 同一逻辑组内所有条件按 logic（AND/OR）组合
- 支持任意层级嵌套

---

## 二、字段清单

### 1. 基础筛选

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| `code_prefix` | string | 股票代码前缀 | `"00"`, `"60"`, `"30"`, `"68"` |
| `is_st` | bool | 是否ST股 | `true` / `false` |
| `trading_status` | string | 交易状态 | `"交易"`, `"停牌"` |
| `is_limit_up` | bool | 是否涨停 (pct_change >= 9.5%) | `true` / `false` |
| `is_limit_down` | bool | 是否跌停 (pct_change <= -9.5%) | `true` / `false` |

### 2. 行情数据

| 字段 | 类型 | 说明 |
|------|------|------|
| `close` | float | 收盘价 |
| `open` | float | 开盘价 |
| `high` | float | 最高价 |
| `low` | float | 最低价 |
| `volume` | float | 成交量 |
| `amount` | float | 成交额 |
| `pct_change` | float | 涨跌幅(%) |
| `change` | float | 涨跌额 |
| `amplitude` | float | 振幅(%) |
| `turnover` | float | 换手率(%) |

### 3. 均线系统

| 字段 | 类型 | 说明 |
|------|------|------|
| `ma5` / `ma7` / `ma10` / `ma20` | float | 收盘价均线 |
| `v_ma5` / `v_ma7` / `v_ma10` / `v_ma20` | float | 成交量均线 |
| `vwma5` / `vwma10` / `vwma20` | float | 成交量加权均线 |

### 4. MACD 指标

| 字段 | 类型 | 说明 |
|------|------|------|
| `macd_dif` | float | 快线(DIF) |
| `macd_dea` | float | 慢线(DEA/Signal) |
| `macd_hist` | float | 柱状图(MACD柱) |

### 5. KDJ 指标

| 字段 | 类型 | 说明 |
|------|------|------|
| `kdj_k` | float | K值 |
| `kdj_d` | float | D值 |
| `kdj_j` | float | J值 |

### 6. 布林带

| 字段 | 类型 | 说明 |
|------|------|------|
| `bb_upper` | float | 上轨 |
| `bb_middle` | float | 中轨 |
| `bb_lower` | float | 下轨 |
| `bb_bandwidth` | float | 带宽(%) |

### 7. 量能指标

| 字段 | 类型 | 说明 |
|------|------|------|
| `vol_ratio` | float | 量比(当日量/MA5量) |
| `obv` | float | 能量潮 |

### 8. 统计类字段（需配合 lookback）

| 字段 | 类型 | 说明 | 必需参数 |
|------|------|------|----------|
| `limit_up_count` | int | 涨停次数 | `lookback` |
| `limit_down_count` | int | 跌停次数 | `lookback` |
| `continuous_limit_up` | int | 连续涨停天数 | — |
| `continuous_limit_down` | int | 连续跌停天数 | — |
| `continuous_up` | int | 连涨天数(收阳) | — |
| `continuous_down` | int | 连跌天数(收阴) | — |
| `pct_change_sum` | float | N日累计涨跌幅(%) | `lookback` |
| `turnover_avg` | float | N日平均换手率(%) | `lookback` |
| `amount_avg` | float | N日平均成交额 | `lookback` |
| `high_max` | float | N日最高价 | `lookback` |
| `low_min` | float | N日最低价 | `lookback` |
| `amplitude_max` | float | N日最大振幅(%) | `lookback` |
| `vol_ratio_avg` | float | N日平均量比 | `lookback` |

---

## 三、操作符清单

### 数值比较

| 操作符 | 说明 | 示例 |
|--------|------|------|
| `>` | 大于 | `close > ma5` |
| `>=` | 大于等于 | `kdj_k >= 80` |
| `<` | 小于 | `turnover < 3` |
| `<=` | 小于等于 | `pct_change <= -5` |
| `=` | 等于 | `limit_up_count = 1` |
| `!=` | 不等于 | `trading_status != "停牌"` |

### 集合 / 字符串

| 操作符 | 说明 | 示例 |
|--------|------|------|
| `in` | 属于集合 | `code_prefix in ["00", "60"]` |
| `not_in` | 不属于集合 | `code_prefix not_in ["30", "68"]` |
| `contains` | 包含子串 | `trading_status contains "交易"` |
| `not_contains` | 不包含子串 | — |
| `starts_with` | 以...开头 | `code starts_with "60"` |

### 交叉类（金叉 / 死叉）

| 操作符 | 说明 | 判定逻辑 |
|--------|------|----------|
| `cross_above` | 金叉（上穿） | field[0] > ref[0] 且 field[-1] <= ref[-1] |
| `cross_below` | 死叉（下穿） | field[0] < ref[0] 且 field[-1] >= ref[-1] |

### 趋势类

| 操作符 | 说明 | 必需参数 |
|--------|------|----------|
| `rising` | 连续上升 | `lookback`（连续N日递增） |
| `falling` | 连续下降 | `lookback`（连续N日递减） |
| `is_new_high` | 创N日新高 | `lookback` |
| `is_new_low` | 创N日新低 | `lookback` |

### K线形态

| 操作符 | 说明 |
|--------|------|
| `is_shape` | K线形态判定（value 指定形态名） |

K线形态取值：

| 形态名 | 说明 | 判定规则 |
|--------|------|----------|
| `大阳线` | 涨幅 > 5%，实体饱满 | pct_change > 5, body/range > 0.6 |
| `大阴线` | 跌幅 > 5%，实体饱满 | pct_change < -5, body/range > 0.6 |
| `小阳线` | 小幅上涨 | 0 < pct_change < 3 |
| `小阴线` | 小幅下跌 | -3 < pct_change < 0 |
| `十字星` | 开收接近 | abs(close-open) / (high-low) < 0.1 |
| `长上影` | 上影线 > 实体2倍 | upper_shadow > body * 2 |
| `长下影` | 下影线 > 实体2倍 | lower_shadow > body * 2 |
| `锤子线` | 短上影+长下影+小实体 | 低位长下影形态 |
| `倒锤子` | 长上影+短下影+小实体 | 高位长上影形态 |
| `一字板` | 开=高=低=收 | open=high=low=close |
| `T字板` | 开=收=高≠低 | open=close=high, low<open |
| `倒T字板` | 开=收=低≠高 | open=close=low, high>open |

### 量价形态

| 操作符 | 说明 | 判定 |
|--------|------|------|
| `vol_price` | 量价关系判定 | value 指定关系名 |

量价关系取值：

| 关系名 | 说明 |
|--------|------|
| `放量上涨` | 量比 > 1.5 且 pct_change > 1 |
| `缩量上涨` | 量比 < 0.7 且 pct_change > 0 |
| `放量下跌` | 量比 > 1.5 且 pct_change < -1 |
| `缩量下跌` | 量比 < 0.7 且 pct_change < 0 |
| `放量滞涨` | 量比 > 2 且 abs(pct_change) < 1 |
| `天量` | 120日最大成交量 |
| `地量` | 30日最小成交量 |

### 均线形态

| 操作符 | 说明 | 必需参数 |
|--------|------|----------|
| `ma_align` | 均线排列 | `mas`（均线列表） |

均线排列取值：

| 排列名 | 说明 |
|--------|------|
| `多头排列` | 从短到长依次递增（如 ma5 > ma10 > ma20） |
| `空头排列` | 从短到长依次递减（如 ma5 < ma10 < ma20） |
| `粘合` | 最大与最小均线间距 < 1.2% |

### 指标区间

| 操作符 | 说明 |
|--------|------|
| `in_zone` | 指标处于指定区间 |

预定义区间：

| 区间名 | 适用字段 | 说明 |
|--------|----------|------|
| `超买` | kdj_k, kdj_d | K > 80, D > 70 |
| `超卖` | kdj_k, kdj_d | K < 20, D < 30 |
| `零轴上方` | macd_dif, macd_dea | 值 > 0 |
| `零轴下方` | macd_dif, macd_dea | 值 < 0 |
| `红柱` | macd_hist | hist > 0 |
| `绿柱` | macd_hist | hist < 0 |
| `红柱放大` | macd_hist | hist > 0 且 hist > prev_hist |
| `绿柱放大` | macd_hist | hist < 0 且 hist < prev_hist |
| `布林上轨外` | close | close > bb_upper |
| `布林下轨外` | close | close < bb_lower |
| `布林中轨上` | close | bb_lower < close < bb_upper, close > bb_middle |
| `布林中轨下` | close | bb_lower < close < bb_middle |

---

## 四、特殊参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `offset` | int | 0 | 交易日偏移。0=当天，1=昨天，3=3天前 |
| `lookback` | int | — | 回溯窗口（最近N个交易日），用于统计类字段和趋势操作符 |
| `ref` | string | — | 引用另一个字段做动态比较（如 close > ma5 中的 ma5） |
| `aggregate` | string | — | 聚合函数（见下） |
| `mas` | [string] | — | 均线列表，用于 ma_align 操作符 |

### 聚合函数（aggregate）

| 函数 | 说明 | 示例 |
|------|------|------|
| `avg` | 求均值 | 最近5日平均换手率 > 5% |
| `sum` | 求和 | 最近3日累计涨幅 > 10% |
| `max` | 最大值 | 最近20日最高价 |
| `min` | 最小值 | 最近20日最低价 |

示例：
```json
{"field": "turnover", "op": ">", "value": 5, "lookback": 5, "aggregate": "avg"}
```
含义：最近5日平均换手率 > 5%

---

## 五、操作符与字段速查表

| 需求 | 写法 |
|------|------|
| 收盘价在5日线上 | `{"field": "close", "op": ">", "ref": "ma5"}` |
| 涨幅 > 3% | `{"field": "pct_change", "op": ">", "value": 3}` |
| 涨停 | `{"field": "is_limit_up", "op": "=", "value": true}` |
| 3天前涨停 | `{"field": "is_limit_up", "op": "=", "value": true, "offset": 3}` |
| 7天内涨停1次 | `{"field": "limit_up_count", "op": "=", "value": 1, "lookback": 7}` |
| MACD金叉 | `{"field": "macd_dif", "op": "cross_above", "ref": "macd_dea"}` |
| KDJ金叉 | `{"field": "kdj_k", "op": "cross_above", "ref": "kdj_d"}` |
| 均线5/10多头排列 | `{"field": "ma_align", "op": "=", "value": "多头排列", "mas": ["ma5", "ma10", "ma20"]}` |
| KDJ超买 | `{"field": "kdj_k", "op": "in_zone", "value": "超买"}` |
| MACD零轴上方 | `{"field": "macd_dif", "op": "in_zone", "value": "零轴上方"}` |
| 20日新高 | `{"field": "close", "op": "is_new_high", "lookback": 20}` |
| 连涨3天 | `{"field": "continuous_up", "op": ">=", "value": 3}` |
| 最近5日均线上升 | `{"field": "ma5", "op": "rising", "lookback": 5}` |
| 最近5日平均换手率 > 5% | `{"field": "turnover", "op": ">", "value": 5, "lookback": 5, "aggregate": "avg"}` |
| K线为十字星 | `{"field": "kline", "op": "is_shape", "value": "十字星"}` |
| 放量上涨 | `{"field": "vol_price", "op": "vol_price", "value": "放量上涨"}` |
| 量比 > 2 | `{"field": "vol_ratio", "op": ">", "value": 2}` |
| 换手率 3~8% | `{"field": "turnover", "op": ">=", "value": 3}` + `{"field": "turnover", "op": "<=", "value": 8}` |
| 非ST非停牌 | `{"field": "is_st", "op": "=", "value": false}` + `{"field": "trading_status", "op": "contains", "value": "交易"}` |
| 收盘价在布林中轨上 | `{"field": "close", "op": "in_zone", "value": "布林中轨上"}` |
| MACD红柱放大 | `{"field": "macd_hist", "op": "in_zone", "value": "红柱放大"}` |
