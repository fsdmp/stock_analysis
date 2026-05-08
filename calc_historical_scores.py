"""Calculate daily scores for specified stocks from buy_date-1 to sell_date-1."""

import sys
sys.path.insert(0, '.')

import pandas as pd
from stock_data.scoring import calc_score

# Stock definitions: (code, name, buy_date, sell_date_end)
# sell_date_end = None means "至今不卖出" -> use latest available date (2026-05-06, day before today 05-07)
STOCKS = [
    # 买入时间 4月17日 -> 评分从 04-16 开始
    ("600773", "西藏城投", "2026-04-17", "2026-04-29"),
    ("603682", "锦和商管", "2026-04-17", "2026-05-05"),
    ("002733", "雄韬股份", "2026-04-17", None),  # 低于预期，用长期持有看
    ("002074", "国轩高科", "2026-04-17", None),
    ("603738", "泰晶科技", "2026-04-17", None),  # 不卖出
    ("600654", "中安科",   "2026-04-17", None),
    ("600527", "江南高纤", "2026-04-17", "2026-05-06"),

    # 买入时间 4月20日 -> 评分从 04-17 开始（不，前一天是04-17，但4月18-19是周末）
    # 买入4月20（周一），前一天交易日是04-17（周五）
    ("600855", "航天长峰", "2026-04-20", None),
    ("603109", "神驰机电", "2026-04-20", None),
    ("002943", "宇晶股份", "2026-04-20", None),  # 不卖出
    ("002515", "金字火腿", "2026-04-20", None),  # 不卖出
    ("002436", "兴森科技", "2026-04-20", None),  # 不卖出
    ("000612", "焦作万方", "2026-04-20", None),
    ("000791", "甘肃能源", "2026-04-20", None),

    # 买入时间 4月21日 -> 评分从 04-20 开始
    ("603815", "交建股份", "2026-04-21", None),  # 不卖出
    ("605365", "立达信",   "2026-04-21", "2026-04-23"),
    ("605366", "宏柏新材", "2026-04-21", None),
    ("605098", "行动教育", "2026-04-21", None),
    ("603150", "万朗磁塑", "2026-04-21", None),
    ("600135", "乐凯胶片", "2026-04-21", None),
    ("002463", "沪电股份", "2026-04-21", "2026-04-22"),
]

# For stocks with no sell date, we calculate up to 2026-05-06 (day before latest 05-07)
NO_SELL_END = "2026-05-06"


def get_prev_trading_day(df, date_str):
    """Get the trading day before the given date."""
    target = pd.Timestamp(date_str)
    before = df[df['date'] < target]
    if len(before) == 0:
        return None
    return str(before['date'].iloc[-1].date())


def get_trading_days(df, start_date, end_date):
    """Get list of trading days between start_date and end_date inclusive."""
    s = pd.Timestamp(start_date)
    e = pd.Timestamp(end_date)
    mask = (df['date'] >= s) & (df['date'] <= e)
    days = df.loc[mask, 'date'].sort_values().tolist()
    return [str(d.date()) for d in days]


def main():
    results = {}

    for code, name, buy_date, sell_end in STOCKS:
        print(f"\n{'='*60}")
        print(f"处理: {name}({code}) 买入日:{buy_date} 卖出截止:{sell_end or '不卖出'}")
        print(f"{'='*60}")

        df = pd.read_parquet(f'data/stocks/{code}.parquet')
        df['date'] = pd.to_datetime(df['date'])

        # Find the trading day before buy_date
        start_day = get_prev_trading_day(df, buy_date)
        if start_day is None:
            print(f"  跳过: 找不到买入日前一天")
            continue

        # End day: day before sell_end, or latest if no sell
        if sell_end:
            end_day = get_prev_trading_day(df, sell_end)
        else:
            end_day = get_prev_trading_day(df, NO_SELL_END)

        # Actually, we want from buy_date-1 to sell_end_date-1
        # But for "不卖出", we use the day before today (05-06)
        if sell_end:
            # sell_end is the expected sell date, we want scores up to sell_end-1
            end_day_calc = get_prev_trading_day(df, sell_end)
        else:
            end_day_calc = NO_SELL_END

        trading_days = get_trading_days(df, start_day, end_day_calc)
        print(f"  评分日期范围: {start_day} ~ {end_day_calc} ({len(trading_days)}个交易日)")

        stock_scores = []
        for day in trading_days:
            sub = df[df['date'] <= pd.Timestamp(day)]
            if len(sub) < 30:
                stock_scores.append((day, None, "数据不足"))
                continue
            result = calc_score(sub)
            stock_scores.append((day, result['total'], result['action']))
            print(f"  {day}: 评分={result['total']:>3d}  操作={result['action']}")

        results[code] = {
            'name': name,
            'buy_date': buy_date,
            'sell_end': sell_end,
            'scores': stock_scores,
        }

    # Print summary table
    print("\n\n")
    print("=" * 120)
    print("评分汇总表")
    print("=" * 120)

    # Group by buy date
    groups = {
        "4月17日买入": [s for s in STOCKS if s[2] == "2026-04-17"],
        "4月20日买入": [s for s in STOCKS if s[2] == "2026-04-20"],
        "4月21日买入": [s for s in STOCKS if s[2] == "2026-04-21"],
    }

    for group_name, group_stocks in groups.items():
        print(f"\n{'─'*120}")
        print(f"  {group_name}")
        print(f"{'─'*120}")

        for code, name, buy_date, sell_end in group_stocks:
            r = results[code]
            scores = r['scores']
            score_str = " | ".join([f"{day[5:]}:{sc if sc is not None else 'N/A'}" for day, sc, _ in scores])
            sell_info = f"卖出:{sell_end}" if sell_end else "持有中"
            print(f"  {name}({code}) [{sell_info}]")
            print(f"    {score_str}")
            print()


if __name__ == "__main__":
    main()
