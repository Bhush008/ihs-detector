# prie is not going to neckline after LS and After RS the price is also going belwo RS and IHS is detected


from SmartApi import SmartConnect
import pyotp
import pandas as pd
import numpy as np
import requests
import time
import math
from datetime import datetime
from scipy.signal import argrelextrema
import matplotlib.pyplot as plt

# === CONFIGURATION === #
api_key = 'joebm3IW'
user_id = 'AAAF838728'
pin = '0421'
totpkey = 'SS4OWS4U3LH5YF66ZR63AYPUDE'
num_stocks = 50
to_date = datetime.now().strftime("%Y-%m-%d") + " 15:30"
date_ranges = [
    ("2023-01-01 09:15", to_date),
    ("2023-07-01 09:15", to_date),
    ("2024-01-01 09:15", to_date),
    ("2024-07-01 09:15", to_date),
    ("2025-01-01 09:15", to_date)
]

interval = "ONE_DAY"

# === LOGIN === #
smartApi = SmartConnect(api_key=api_key)
try:
    data = smartApi.generateSession(user_id, pin, pyotp.TOTP(totpkey).now())
    refreshToken = data['data']['refreshToken']
    profile = smartApi.getProfile(refreshToken)
    print("✅ Login successful.")
except Exception as e:
    print("❌ Login failed:", e)
    exit()

# === FETCH TOKEN MASTER & FILTER EQ === #
url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
try:
    token_data = requests.get(url).json()
    token_df = pd.DataFrame(token_data)
    token_df['expiry'] = pd.to_datetime(token_df['expiry'], errors='coerce').dt.date

    eq_stocks = token_df[
        token_df['symbol'].str.upper().str.endswith("-EQ") &
        (token_df['exch_seg'].str.upper() == 'NSE')
    ].head(num_stocks)

    if eq_stocks.empty:
        print("❌ No matching -EQ equity stocks found.")
        exit()

    print(f"✅ Found {len(eq_stocks)} equity stocks: {eq_stocks['symbol'].tolist()}")

except Exception as e:
    print("❌ Error fetching token master:", e)
    exit()

# === IHS Detection Utilities === #
def find_extrema(series, order=3):
    return argrelextrema(series.values, np.less, order=order)[0]

def calculate_dynamic_neckline(df, minima_idx):
    if len(minima_idx) < 2:
        return df['Close'].max()
    start, end = minima_idx[0], minima_idx[-1]
    return df['High'].iloc[start:end].max()

def add_atr_column(df, period=14):
    high = df['High']
    low = df['Low']
    close = df['Close']
    
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    atr = tr.rolling(window=period, min_periods=1).mean()
    df['ATR'] = atr
    return df

# === MODIFIED IHS DETECTION === #
def detect_ihs_custom(df, atr_period=14, lookahead=5):
    df = add_atr_column(df, period=atr_period)
    close = df['Close'].values
    low = df['Low'].values
    n = len(close)

    head_idx = np.argmin(low)
    head_val = low[head_idx]
    neckline = df['High'].max()

    # ✅ Ensure price rises to 92.5% of neckline AFTER HEAD
    post_head_range = close[head_idx+1:head_idx+15]
    if len(post_head_range) == 0 or max(post_head_range) < neckline * 0.925:
        return None

    # 🔍 Look for LS before head
    for ls_idx in range(head_idx - 2, 2, -1):
        ls_val = low[ls_idx]
        if ls_val <= head_val:
            continue
        if low[ls_idx - 1] < ls_val:
            continue
        pre_ls_range = close[max(0, ls_idx - 5):ls_idx]
        if len(pre_ls_range) == 0 or max(pre_ls_range) < neckline * 0.925:
            continue
        break
    else:
        return None

    # 🔍 Look for RS after head
    for rs_idx in range(head_idx + 2, n - lookahead - 1):
        rs_val = low[rs_idx]
        if rs_val <= head_val:
            continue
        if low[rs_idx - 1] < rs_val:
            continue

        # ✅ Price must rise toward neckline BEFORE RS
        pre_rs_range = close[head_idx+1:rs_idx]
        if len(pre_rs_range) == 0 or max(pre_rs_range) < neckline * 0.925:
            continue

        # ✅ After RS: price must rise at least 2%
        post_rs_range = close[rs_idx+1:rs_idx+1+lookahead]
        if len(post_rs_range) == 0 or max(post_rs_range) < rs_val * 1.02:
            continue

        # ✅ After RS: price should NOT fall below RS
        if min(post_rs_range) < rs_val:
            continue

        # ✅ Only accept pattern if price stays strong after RS
        breakout_found = max(post_rs_range) >= neckline
        breakout_index = rs_idx + np.argmax(close[rs_idx+1:rs_idx+1+lookahead] >= neckline) + 1 if breakout_found else None
        breakout_date = pd.to_datetime(df['Date'].iloc[breakout_index]) if breakout_found else None

        return {
            "ls_idx": ls_idx,
            "h_idx": head_idx,
            "rs_idx": rs_idx,
            "ls_val": low[ls_idx],
            "head_val": head_val,
            "rs_val": low[rs_idx],
            "ls_date": pd.to_datetime(df['Date'].iloc[ls_idx]),
            "head_date": pd.to_datetime(df['Date'].iloc[head_idx]),
            "rs_date": pd.to_datetime(df['Date'].iloc[rs_idx]),
            "neckline": neckline,
            "confirmed": breakout_found,
            "breakout_date": breakout_date,
            "df": df
        }

    return None

# === MAIN LOOP: Fetch + Analyze === #
detected_ihs_stocks = []

detected_ihs_all_runs = {}

for from_date, to_date in date_ranges:
    print(f"\n🔁 Running analysis from {from_date} to {to_date}")
    detected_ihs_stocks = []

    for _, row in eq_stocks.iterrows():
        sym = row['symbol']
        token = row['token']
        print(f"\n📊 Processing {sym}...")

        params = {
            "exchange": "NSE",
            "symboltoken": str(token),
            "interval": interval,
            "fromdate": from_date,
            "todate": to_date
        }

        try:
            res = smartApi.getCandleData(params)
            time.sleep(0.8)  # Respect rate limit
            if not res.get('data'):
                print(f"⚠️ No data for {sym}. Skipping.")
                continue

            df = pd.DataFrame(res['data'], columns=["Date", "Open", "High", "Low", "Close", "Volume"])
            df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)

            minima_idx = find_extrema(df['Low'], order=3)
            neckline = calculate_dynamic_neckline(df, minima_idx)
            ihs_pattern = detect_ihs_custom(df)

            if ihs_pattern:
                print(f"✅ IHS detected in {sym}")
                print(f"  LS: {ihs_pattern['ls_val']} on {ihs_pattern['ls_date'].date()}")
                print(f"  Head: {ihs_pattern['head_val']} on {ihs_pattern['head_date'].date()}")
                print(f"  RS: {ihs_pattern['rs_val']} on {ihs_pattern['rs_date'].date()}")
                print(f"  Neckline: {ihs_pattern['neckline']:.2f}")
                print("  ⚠️ Pattern valid but no breakout yet.")

                detected_ihs_stocks.append(sym)
            else:
                print(f"❌ No valid IHS found in {sym}.")

        except Exception as e:
            print(f"❌ Error processing {sym}: {e}")
            continue

    detected_ihs_all_runs[from_date] = detected_ihs_stocks

# === SUMMARY === #
print("\n📋 Summary of IHS Patterns Detected in All Runs:")
for run_start, stocks in detected_ihs_all_runs.items():
    print(f"\n🕒 Run from {run_start}:")
    if stocks:
        for stock in stocks:
            print(f"  - {stock}")
    else:
        print("  ❌ No IHS pattern detected in this run.")

summary_lines = []

# After detecting IHS in a stock:
summary_lines.append(f"Run from {start_date}:\n  - {stock_name}")

# Or if no pattern:
# summary_lines.append(f"Run from {start_date}:\n  ❌ No IHS pattern detected.")

# After all stocks:
with open("final_summary.txt", "w") as f:
    f.write("Summary of IHS Patterns Detected:\n\n")
    for line in summary_lines:
        f.write(line + "\n")

email_body = "\n".join(summary_lines)
print(email_body)  # This print output will be captured for email body


