#!/usr/bin/env python3
"""Read parquet file and display last 15 rows of specific columns."""
import sys
import pandas as pd

# Load the parquet file
file_path = '/home/admin/sa/stock_analysis/data/stocks/002747.parquet'
df = pd.read_parquet(file_path)

# Convert date column to datetime
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'])

# Display last 15 rows with specified columns
result = df[['date', 'close', 'pct_change']].tail(15)
print(result.to_string())
