# Optimized version: Fetch data only once for longest date range, then slice for shorter ranges

from SmartApi import SmartConnect
import pyotp
import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime
from scipy.signal import argrelextrema

# === CONFIGURATION === #
api_key = 'joebm3IW'
user_id = 'AAAF838728'
pin = '0421'
totpkey = 'SS4OWS4U3LH5YF66ZR63AYPUDE'
num_stocks = 200
to_date = datetime.now().strftime("%Y-%m-%d") + " 15:30"
longest_from_date = "2023-01-01 09:15"
date_ranges = [
    ("2023-01-01 09:15", to_date),
    ("2023-04-01 09:15", to_date),
    ("2023-07-01 09:15", to_date),
    ("2023-10-01 09:15", to_date),
    ("2024-01-01 09:15", to_date),
    ("2024-04-01 09:15", to_date),
    ("2024-07-01 09:15", to_date),
    ("2024-10-01 09:15", to_date),
    ("2025-01-01 09:15", to_date),
    ("2025-04-01 09:15", to_date)
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

# === FETCH TOKEN MASTER === #
url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
try:
    token_data = requests.get(url).json()
    token_df = pd.DataFrame(token_data)
    token_df['expiry'] = pd.to_datetime(token_df['expiry'], errors='coerce').dt.date

    eq_stocks = token_df[
        token_df['symbol'].str.upper().str.endswith("-EQ") &
        (token_df['exch_seg'].str.upper() == 'NSE') &
        token_df['token'].notna()
    ].head(num_stocks)

    print(f"✅ Loaded {len(eq_stocks)} equity stocks")
except Exception as e:
    print("❌ Error fetching token master:", e)
    exit()

# === IHS DETECTION FUNCTIONS === #
def find_extrema(series, order=3):
    return argrelextrema(series.values, np.less, order=order)[0]

def calculate_dynamic_neckline(df, minima_idx):
    if len(minima_idx) < 2:
        return df['Close'].max()
    return df['High'].iloc[minima_idx[0]:minima_idx[-1]].max()

def add_atr_column(df, period=14):
    high, low, close = df['High'], df['Low'], df['Close']
    tr = pd.concat([
        high - low,
        abs(high - close.shift(1)),
        abs(low - close.shift(1))
    ], axis=1).max(axis=1)
    df['ATR'] = tr.rolling(window=period, min_periods=1).mean()
    return df

def detect_ihs_custom(df, atr_period=14, lookahead=5):
    df = add_atr_column(df, atr_period)
    close, low = df['Close'].values, df['Low'].values
    n = len(close)

    head_idx = np.argmin(low)
    head_val = low[head_idx]
    neckline = df['High'].max()

    if max(close[head_idx+1:head_idx+15], default=0) < neckline * 0.98:
        return None

    for ls_idx in range(head_idx - 2, 2, -1):
        if low[ls_idx] <= head_val or low[ls_idx - 1] < low[ls_idx]: continue
        if max(close[max(0, ls_idx - 5):ls_idx], default=0) < neckline * 0.98: continue
        break
    else: return None

    for rs_idx in range(head_idx + 2, n - lookahead - 1):
        if low[rs_idx] <= head_val or low[rs_idx - 1] < low[rs_idx]: continue
        if max(close[head_idx+1:rs_idx], default=0) < neckline * 0.98: continue
        post = close[rs_idx+1:rs_idx+1+lookahead]
        if len(post) == 0 or max(post) < low[rs_idx] * 1.02 or min(post) < low[rs_idx]: continue

        return {
            "ls_val": low[ls_idx], "head_val": head_val, "rs_val": low[rs_idx],
            "ls_date": df['Date'].iloc[ls_idx],
            "head_date": df['Date'].iloc[head_idx],
            "rs_date": df['Date'].iloc[rs_idx],
            "neckline": neckline
        }
    return None

# === RETRY MECHANISM + RATE LIMIT === #
def fetch_candle_data_with_retry(symbol, token, retries=3, delay=2):
    for attempt in range(retries):
        try:
            params = {
                "exchange": "NSE", "symboltoken": token,
                "interval": interval,
                "fromdate": longest_from_date, "todate": to_date
            }
            res = smartApi.getCandleData(params)
            time.sleep(0.35)  # Enforce rate limit of max 3 requests/sec
            if res.get('data'):
                return pd.DataFrame(res['data'], columns=["Date", "Open", "High", "Low", "Close", "Volume"])
        except Exception as e:
            print(f"⚠️ Retry {attempt + 1}/{retries} for {symbol} failed: {e}")
        time.sleep(delay)
    print(f"❌ Failed all retries for {symbol}")
    return None

# === FETCH ONCE FOR LONGEST DATE RANGE === #
all_stock_data = {}
print("\n📥 Fetching data for longest date range...")
for _, row in eq_stocks.iterrows():
    sym, token = row['symbol'], str(row['token'])
    print(f"⏳ Fetching {sym}...")
    df = fetch_candle_data_with_retry(sym, token)
    if df is not None:
        df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
        all_stock_data[sym] = df

# === RUN ANALYSIS FOR EACH DATE RANGE === #
detected_ihs_all_runs = {}
print("\n🔍 Starting IHS detection...")
for from_date, to_date in date_ranges:
    print(f"\n📆 Range: {from_date} to {to_date}")
    detected = []
    from_dt = pd.to_datetime(from_date.split()[0])
    to_dt = pd.to_datetime(to_date.split()[0])

    for sym, df in all_stock_data.items():
        sliced_df = df[(df['Date'] >= from_dt) & (df['Date'] <= to_dt)].copy()
        if len(sliced_df) < 50: continue
        ihs = detect_ihs_custom(sliced_df)
        if ihs:
            print(f"✅ {sym} - IHS detected")
            detected.append(sym)
    detected_ihs_all_runs[from_date] = detected

# === SUMMARY (Tabular Format) === #
summary_lines = ["📋 Summary of IHS Patterns:\n", f"{'Date Range':<25} | {'IHS Detected'}", "-" * 60]
for from_date, stocks in detected_ihs_all_runs.items():
    from_clean = from_date.split()[0]
    to_clean = to_date.split()[0]
    detected_str = ", ".join(stocks) if stocks else "None"
    summary_lines.append(f"{from_clean} to {to_clean:<10} | {detected_str}")

summary = "\n".join(summary_lines)
print(summary)

# Save plain text summary for email attachment
with open("ihs_summary.txt", "w") as f:
    f.write(summary)
