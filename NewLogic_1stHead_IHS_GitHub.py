# Optimized version: Fetch data only once for longest date range, then slice for shorter ranges

from SmartApi import SmartConnect
import pyotp
import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime
from scipy.signal import argrelextrema
from dateutil.relativedelta import relativedelta

# === CONFIGURATION === #
api_key = 'joebm3IW'
user_id = 'AAAF838728'
pin = '0421'
totpkey = 'SS4OWS4U3LH5YF66ZR63AYPUDE'
num_stocks = 2500

longest_from_date = "2023-01-01 09:15"
# Set dynamic to_date as current date at 15:30
to_date_obj = datetime.now()
to_date = to_date_obj.strftime("%Y-%m-%d") + " 15:30"

# Generate 'from' dates from Jan 1, 2023 to Jan 1, 2025 (inclusive)
start_from_date = datetime(2023, 1, 1, 9, 15)
end_cutoff = datetime(2025, 1, 1)

date_ranges = []
current = start_from_date

while current <= end_cutoff:
    from_str = current.strftime("%Y-%m-%d %H:%M")
    date_ranges.append((from_str, to_date))
    current += relativedelta(months=1)

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
    close, low, high = df['Close'].values, df['Low'].values, df['High'].values
    n = len(close)
    neckline = high.max()

    for i in range(20, n - lookahead - 1):
        # Step 1: Rally near neckline
        if close[i] < neckline * 0.96:
            continue
        
        # Step 2: Drop to form LS
        ls_idx = i + 1
        while ls_idx < n - lookahead and low[ls_idx] > low[ls_idx + 1]:
            ls_idx += 1
        if ls_idx >= n - lookahead:
            continue
        ls_val = low[ls_idx]

        # Step 3: Rally near neckline again
        peak_after_ls = max(close[ls_idx:ls_idx + 15], default=0)
        if peak_after_ls < neckline * 0.96:
            continue

        # Step 4: Drop below LS to form Head
        head_idx = ls_idx + 1
        while head_idx < n - lookahead and low[head_idx] < low[head_idx + 1]:
            head_idx += 1
        if head_idx >= n - lookahead or low[head_idx] >= ls_val:
            continue
        head_val = low[head_idx]

        # Step 5: Rally again to 98% of neckline
        peak_after_head = max(close[head_idx:head_idx + 15], default=0)
        if peak_after_head < neckline * 0.96:
            continue

        # Step 6: Drop to form RS
        rs_idx = head_idx + 1
        while rs_idx < n - lookahead and low[rs_idx] > low[rs_idx + 1]:
            rs_idx += 1
        if rs_idx >= n - lookahead or low[rs_idx] <= head_val:
            continue
        rs_val = low[rs_idx]

        # Step 7: After RS, price should not fall below RS
        future_prices = close[rs_idx + 1:rs_idx + 1 + lookahead]
        if len(future_prices) == 0 or min(future_prices) < rs_val:
            continue

        return {
            "ls_val": ls_val, "head_val": head_val, "rs_val": rs_val,
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
summary_lines = ["📋 Summary of IHS Patterns:\n", f"{'Date Range':<25} | {'Stock':<12} | {'LS':<25} | {'Head':<25} | {'RS':<25}","-" * 120]

for from_date, stocks in detected_ihs_all_runs.items():
    from_clean = from_date.split()[0]
    to_clean = to_date.split()[0]
    
    for sym in stocks:
        df_sym = all_stock_data[sym]
        sliced_df = df_sym[(df_sym['Date'] >= pd.to_datetime(from_clean)) & (df_sym['Date'] <= pd.to_datetime(to_clean))].copy()
        if len(sliced_df) < 50: continue
        ihs = detect_ihs_custom(sliced_df)
        if not ihs: continue

        ls_str = f"{ihs['ls_val']:.2f} on {ihs['ls_date'].date()}"
        head_str = f"{ihs['head_val']:.2f} on {ihs['head_date'].date()}"
        rs_str = f"{ihs['rs_val']:.2f} on {ihs['rs_date'].date()}"

        summary_lines.append(f"{from_clean} to {to_clean:<10} | {sym:<12} | {ls_str:<25} | {head_str:<25} | {rs_str:<25}")

summary = "\n".join(summary_lines)
print(summary)

# Save plain text summary for email attachment
with open("ihs_summary.txt", "w") as f:
    f.write(summary)
