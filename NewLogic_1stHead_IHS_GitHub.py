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
num_stocks = 10

longest_from_date = "2023-01-01 09:15"
to_date_obj = datetime.now()
to_date = to_date_obj.strftime("%Y-%m-%d") + " 15:30"

start_from_date = datetime(2022, 1, 1, 9, 15)
end_cutoff = datetime(2025, 4, 1)

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
        if close[i] < neckline * 0.96:
            continue
        ls_idx = i + 1
        while ls_idx < n - lookahead and low[ls_idx] > low[ls_idx + 1]:
            ls_idx += 1
        if ls_idx >= n - lookahead:
            continue
        ls_val = low[ls_idx]

        peak_after_ls = max(close[ls_idx:ls_idx + 15], default=0)
        if peak_after_ls < neckline * 0.96:
            continue

        head_idx = ls_idx + 1
        while head_idx < n - lookahead and low[head_idx] < low[head_idx + 1]:
            head_idx += 1
        if head_idx >= n - lookahead or low[head_idx] >= ls_val:
            continue
        head_val = low[head_idx]

        peak_after_head = max(close[head_idx:head_idx + 15], default=0)
        if peak_after_head < neckline * 0.96:
            continue

        rs_idx = head_idx + 1
        while rs_idx < n - lookahead and low[rs_idx] > low[rs_idx + 1]:
            rs_idx += 1
        if rs_idx >= n - lookahead or low[rs_idx] <= head_val:
            continue
        rs_val = low[rs_idx]

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

# ----------- Double Bottom Detection -----------
def detect_double_bottom(df, price_col='Close', min_gap_pct=3, rally_pct_req=5):
    df = df.copy()
    df = df.reset_index(drop=True)
    prices = df[price_col].values
    dates = df['Date'].values

    local_min_idx = argrelextrema(prices, np.less, order=5)[0]
    if len(local_min_idx) < 2:
        return None

    for i in range(len(local_min_idx) - 1):
        first_idx = local_min_idx[i]
        second_idx = local_min_idx[i + 1]

        first_low = prices[first_idx]
        second_low = prices[second_idx]

        # Ensure lows are within 5%
        if abs(first_low - second_low) / first_low > 0.05:
            continue

        neckline_range = prices[first_idx:second_idx + 1]
        peak_idx = first_idx + np.argmax(neckline_range)
        neckline_price = prices[peak_idx]

        mid_range = prices[first_idx:peak_idx + 1]
        peak_between = np.max(mid_range)
        if peak_between < first_low * (1 + rally_pct_req / 100):
            continue
        if peak_between >= neckline_price:
            continue

        # Post second low should not break lower
        if np.min(prices[second_idx:]) < second_low:
            continue

        post_range = prices[second_idx:]
        breakout = np.any(post_range > neckline_price)
        if not breakout:
            continue

        return {
            'first_low_idx': first_idx,
            'second_low_idx': second_idx,
            'neckline_idx': peak_idx,
            'first_low_price': first_low,
            'second_low_price': second_low,
            'neckline_price': neckline_price,
            'first_low_date': dates[first_idx],
            'second_low_date': dates[second_idx],
            'neckline_date': dates[peak_idx]
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
            time.sleep(0.35)
            if res.get('data'):
                return pd.DataFrame(res['data'], columns=["Date", "Open", "High", "Low", "Close", "Volume"])
        except Exception as e:
            print(f"⚠️ Retry {attempt + 1}/{retries} for {symbol} failed: {e}")
        time.sleep(delay)
    print(f"❌ Failed all retries for {symbol}")
    return None

# === FETCH DATA === #
all_stock_data = {}
print("\n📥 Fetching data for longest date range...")
for _, row in eq_stocks.iterrows():
    sym, token = row['symbol'], str(row['token'])
    print(f"⏳ Fetching {sym}...")
    df = fetch_candle_data_with_retry(sym, token)
    if df is not None:
        df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
        all_stock_data[sym] = df

# === ANALYSIS LOOP === #
detected_ihs_all_runs = {}
detected_db_all_runs = {}

print("\n🔍 Starting pattern detection...")
for from_date, to_date in date_ranges:
    print(f"\n📆 Range: {from_date} to {to_date}")
    detected_ihs = []
    detected_db = []

    from_dt = pd.to_datetime(from_date.split()[0])
    to_dt = pd.to_datetime(to_date.split()[0])

    for sym, df in all_stock_data.items():
        sliced_df = df[(df['Date'] >= from_dt) & (df['Date'] <= to_dt)].copy()
        if len(sliced_df) < 50:
            continue

        ihs = detect_ihs_custom(sliced_df)
        if ihs:
            print(f"✅ {sym} - IHS detected")
            detected_ihs.append(sym)

        db = detect_double_bottom(sliced_df)
        if db:
            print(f"🔁 {sym} - Double Bottom detected")
            detected_db.append(sym)

    detected_ihs_all_runs[from_date] = detected_ihs
    detected_db_all_runs[from_date] = detected_db

summary_lines = [
    "📋 Summary of IHS Patterns:\n",
    f"{'Date Range':<25} | {'Pattern':<15} | {'Stock':<12}",
    "-" * 60
]

for from_date in detected_ihs_all_runs:
    to_clean = to_date.split()[0]  # Make sure to_date is defined earlier
    from_clean = from_date.split()[0]

    for sym in detected_ihs_all_runs[from_date]:
        print(f"{from_clean} to {to_clean:<10} | {'IHS':<15} | {sym:<12}")
        summary_lines.append(f"{from_clean} to {to_clean:<10} | {'IHS':<15} | {sym:<12}")

summary = "\n".join(summary_lines)
print(summary)

# Save plain text summary for email attachment
with open("ihs_summary.txt", "w") as f:
    f.write(summary)
