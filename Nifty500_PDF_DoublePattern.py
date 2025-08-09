from SmartApi import SmartConnect
import pyotp
import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime
from scipy.signal import argrelextrema
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import math
from matplotlib.backends.backend_pdf import PdfPages
import os
print(f"INFO: The current working directory is: {os.getcwd()}")

# === CONFIGURATION === #
# Load credentials securely from environment variables
api_key = os.environ.get('API_KEY')
user_id = os.environ.get('USER_ID')
pin = os.environ.get('PIN')
totpkey = os.environ.get('TOTP_KEY')

# Check if all secrets are loaded
if not all([api_key, user_id, pin, totpkey]):
    print("❌ ERROR: One or more required secrets (API_KEY, USER_ID, PIN, TOTP_KEY) are not set.")
    exit()

#num_stocks = 2500

longest_from_date = "2023-01-01 09:15"
to_date_obj = datetime.now()
to_date = to_date_obj.strftime("%Y-%m-%d") + " 15:30"

start_from_date = datetime(2022, 1, 1, 9, 15)
end_cutoff = datetime(2025, 2, 1)

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

    # Define the specific list of stocks you want to fetch
    target_stocks = ['360ONE-EQ',
'3MINDIA-EQ',
'ABB-EQ',
'ACC-EQ',
'ACMESOLAR-EQ',
'AIAENG-EQ',
'APLAPOLLO-EQ',
'AUBANK-EQ',
'AWL-EQ',
'AADHARHFC-EQ',
'AARTIIND-EQ',
'AAVAS-EQ',
'ABBOTINDIA-EQ',
'ACE-EQ',
'ADANIENSOL-EQ',
'ADANIENT-EQ',
'ADANIGREEN-EQ',
'ADANIPORTS-EQ',
'ADANIPOWER-EQ',
'ATGL-EQ',
'ABCAPITAL-EQ',
'ABFRL-EQ',
'ABREL-EQ',
'ABSLAMC-EQ',
'AEGISLOG-EQ',
'AFCONS-EQ',
'AFFLE-EQ',
'AJANTPHARM-EQ',
'AKUMS-EQ',
'APLLTD-EQ',
'ALIVUS-EQ',
'ALKEM-EQ',
'ALKYLAMINE-EQ',
'ALOKINDS-EQ',
'ARE&M-EQ',
'AMBER-EQ',
'AMBUJACEM-EQ',
'ANANDRATHI-EQ',
'ANANTRAJ-EQ',
'ANGELONE-EQ',
'APARINDS-EQ',
'APOLLOHOSP-EQ',
'APOLLOTYRE-EQ',
'APTUS-EQ',
'ASAHIINDIA-EQ',
'ASHOKLEY-EQ',
'ASIANPAINT-EQ',
'ASTERDM-EQ',
'ASTRAZEN-EQ',
'ASTRAL-EQ',
'ATUL-EQ',
'AUROPHARMA-EQ',
'AIIL-EQ',
'DMART-EQ',
'AXISBANK-EQ',
'BASF-EQ',
'BEML-EQ',
'BLS-EQ',
'BSE-EQ',
'BAJAJ-AUTO-EQ',
'BAJFINANCE-EQ',
'BAJAJFINSV-EQ',
'BAJAJHLDNG-EQ',
'BAJAJHFL-EQ',
'BALKRISIND-EQ',
'BALRAMCHIN-EQ',
'BANDHANBNK-EQ',
'BANKBARODA-EQ',
'BANKINDIA-EQ',
'MAHABANK-EQ',
'BATAINDIA-EQ',
'BAYERCROP-EQ',
'BERGEPAINT-EQ',
'BDL-EQ',
'BEL-EQ',
'BHARATFORG-EQ',
'BHEL-EQ',
'BPCL-EQ',
'BHARTIARTL-EQ',
'BHARTIHEXA-EQ',
'BIKAJI-EQ',
'BIOCON-EQ',
'BSOFT-EQ',
'BLUEDART-EQ',
'BLUESTARCO-EQ',
'BBTC-EQ',
'BOSCHLTD-EQ',
'FIRSTCRY-EQ',
'BRIGADE-EQ',
'BRITANNIA-EQ',
'MAPMYINDIA-EQ',
'CCL-EQ',
'CESC-EQ',
'CGPOWER-EQ',
'CRISIL-EQ',
'CAMPUS-EQ',
'CANFINHOME-EQ',
'CANBK-EQ',
'CAPLIPOINT-EQ',
'CGCL-EQ',
'CARBORUNIV-EQ',
'CASTROLIND-EQ',
'CEATLTD-EQ',
'CENTRALBK-EQ',
'CDSL-EQ',
'CENTURYPLY-EQ',
'CERA-EQ',
'CHALET-EQ',
'CHAMBLFERT-EQ',
'CHENNPETRO-EQ',
'CHOLAHLDNG-EQ',
'CHOLAFIN-EQ',
'CIPLA-EQ',
'CUB-EQ',
'CLEAN-EQ',
'COALINDIA-EQ',
'COCHINSHIP-EQ',
'COFORGE-EQ',
'COHANCE-EQ',
'COLPAL-EQ',
'CAMS-EQ',
'CONCORDBIO-EQ',
'CONCOR-EQ',
'COROMANDEL-EQ',
'CRAFTSMAN-EQ',
'CREDITACC-EQ',
'CROMPTON-EQ',
'CUMMINSIND-EQ',
'CYIENT-EQ',
'DCMSHRIRAM-EQ',
'DLF-EQ',
'DOMS-EQ',
'DABUR-EQ',
'DALBHARAT-EQ',
'DATAPATTNS-EQ',
'DEEPAKFERT-EQ',
'DEEPAKNTR-EQ',
'DELHIVERY-EQ',
'DEVYANI-EQ',
'DIVISLAB-EQ',
'DIXON-EQ',
'LALPATHLAB-EQ',
'DRREDDY-EQ',
'DUMMYDBRLT-BE',
'EIDPARRY-EQ',
'EIHOTEL-EQ',
'EICHERMOT-EQ',
'ELECON-EQ',
'ELGIEQUIP-EQ',
'EMAMILTD-EQ',
'EMCURE-EQ',
'ENDURANCE-EQ',
'ENGINERSIN-EQ',
'ERIS-EQ',
'ESCORTS-EQ',
'ETERNAL-EQ',
'EXIDEIND-EQ',
'NYKAA-EQ',
'FEDERALBNK-EQ',
'FACT-EQ',
'FINCABLES-EQ',
'FINPIPE-EQ',
'FSL-EQ',
'FIVESTAR-EQ',
'FORTIS-EQ',
'GAIL-EQ',
'GVT&D-EQ',
'GMRAIRPORT-EQ',
'GRSE-EQ',
'GICRE-EQ',
'GILLETTE-EQ',
'GLAND-EQ',
'GLAXO-EQ',
'GLENMARK-EQ',
'MEDANTA-EQ',
'GODIGIT-EQ',
'GPIL-EQ',
'GODFRYPHLP-EQ',
'GODREJAGRO-EQ',
'GODREJCP-EQ',
'GODREJIND-EQ',
'GODREJPROP-EQ',
'GRANULES-EQ',
'GRAPHITE-EQ',
'GRASIM-EQ',
'GRAVITA-EQ',
'GESHIP-EQ',
'FLUOROCHEM-EQ',
'GUJGASLTD-EQ',
'GMDCLTD-EQ',
'GNFC-EQ',
'GPPL-EQ',
'GSPL-EQ',
'HEG-EQ',
'HBLENGINE-EQ',
'HCLTECH-EQ',
'HDFCAMC-EQ',
'HDFCBANK-EQ',
'HDFCLIFE-EQ',
'HFCL-EQ',
'HAPPSTMNDS-EQ',
'HAVELLS-EQ',
'HEROMOTOCO-EQ',
'HSCL-EQ',
'HINDALCO-EQ',
'HAL-EQ',
'HINDCOPPER-EQ',
'HINDPETRO-EQ',
'HINDUNILVR-EQ',
'HINDZINC-EQ',
'POWERINDIA-EQ',
'HOMEFIRST-EQ',
'HONASA-EQ',
'HONAUT-EQ',
'HUDCO-EQ',
'HYUNDAI-EQ',
'ICICIBANK-EQ',
'ICICIGI-EQ',
'ICICIPRULI-EQ',
'IDBI-EQ',
'IDFCFIRSTB-EQ',
'IFCI-EQ',
'IIFL-EQ',
'INOXINDIA-EQ',
'IRB-EQ',
'IRCON-EQ',
'ITC-EQ',
'ITI-EQ',
'INDGN-EQ',
'INDIACEM-EQ',
'INDIAMART-EQ',
'INDIANB-EQ',
'IEX-EQ',
'INDHOTEL-EQ',
'IOC-EQ',
'IOB-EQ',
'IRCTC-EQ',
'IRFC-EQ',
'IREDA-EQ',
'IGL-EQ',
'INDUSTOWER-EQ',
'INDUSINDBK-EQ',
'NAUKRI-EQ',
'INFY-EQ',
'INOXWIND-EQ',
'INTELLECT-EQ',
'INDIGO-EQ',
'IGIL-EQ',
'IKS-EQ',
'IPCALAB-EQ',
'JBCHEPHARM-EQ',
'JKCEMENT-EQ',
'JBMA-EQ',
'JKTYRE-EQ',
'JMFINANCIL-EQ',
'JSWENERGY-EQ',
'JSWHL-EQ',
'JSWINFRA-EQ',
'JSWSTEEL-EQ',
'JPPOWER-EQ',
'J&KBANK-EQ',
'JINDALSAW-EQ',
'JSL-EQ',
'JINDALSTEL-EQ',
'JIOFIN-EQ',
'JUBLFOOD-EQ',
'JUBLINGREA-EQ',
'JUBLPHARMA-EQ',
'JWL-EQ',
'JUSTDIAL-EQ',
'JYOTHYLAB-EQ',
'JYOTICNC-EQ',
'KPRMILL-EQ',
'KEI-EQ',
'KNRCON-EQ',
'KPITTECH-EQ',
'KAJARIACER-EQ',
'KPIL-EQ',
'KALYANKJIL-EQ',
'KANSAINER-EQ',
'KARURVYSYA-EQ',
'KAYNES-EQ',
'KEC-EQ',
'KFINTECH-EQ',
'KIRLOSBROS-EQ',
'KIRLOSENG-EQ',
'KOTAKBANK-EQ',
'KIMS-EQ',
'LTF-EQ',
'LTTS-EQ',
'LICHSGFIN-EQ',
'LTFOODS-EQ',
'LTIM-EQ',
'LT-EQ',
'LATENTVIEW-EQ',
'LAURUSLABS-EQ',
'LEMONTREE-EQ',
'LICI-EQ',
'LINDEINDIA-EQ',
'LLOYDSME-EQ',
'LODHA-EQ',
'LUPIN-EQ',
'MMTC-EQ',
'MRF-EQ',
'MGL-EQ',
'MAHSEAMLES-EQ',
'M&MFIN-EQ',
'M&M-EQ',
'MANAPPURAM-EQ',
'MRPL-EQ',
'MANKIND-EQ',
'MARICO-EQ',
'MARUTI-EQ',
'MASTEK-EQ',
'MFSL-EQ',
'MAXHEALTH-EQ',
'MAZDOCK-EQ',
'METROPOLIS-EQ',
'MINDACORP-EQ',
'MSUMI-EQ',
'MOTILALOFS-EQ',
'MPHASIS-EQ',
'MCX-EQ',
'MUTHOOTFIN-EQ',
'NATCOPHARM-EQ',
'NBCC-EQ',
'NCC-EQ',
'NHPC-EQ',
'NLCINDIA-EQ',
'NMDC-EQ',
'NSLNISP-EQ',
'NTPCGREEN-EQ',
'NTPC-EQ',
'NH-EQ',
'NATIONALUM-EQ',
'NAVA-EQ',
'NAVINFLUOR-EQ',
'NESTLEIND-EQ',
'NETWEB-EQ',
'NETWORK18-EQ',
'NEULANDLAB-EQ',
'NEWGEN-EQ',
'NAM-INDIA-EQ',
'NIVABUPA-EQ',
'NUVAMA-EQ',
'OBEROIRLTY-EQ',
'ONGC-EQ',
'OIL-EQ',
'OLAELEC-EQ',
'OLECTRA-EQ',
'PAYTM-EQ',
'OFSS-EQ',
'POLICYBZR-EQ',
'PCBL-EQ',
'PGEL-EQ',
'PIIND-EQ',
'PNBHOUSING-EQ',
'PNCINFRA-EQ',
'PTCIL-EQ',
'PVRINOX-EQ',
'PAGEIND-EQ',
'PATANJALI-EQ',
'PERSISTENT-EQ',
'PETRONET-EQ',
'PFIZER-EQ',
'PHOENIXLTD-EQ',
'PIDILITIND-EQ',
'PEL-EQ',
'PPLPHARMA-EQ',
'POLYMED-EQ',
'POLYCAB-EQ',
'POONAWALLA-EQ',
'PFC-EQ',
'POWERGRID-EQ',
'PRAJIND-EQ',
'PREMIERENE-EQ',
'PRESTIGE-EQ',
'PNB-EQ',
'RRKABEL-EQ',
'RBLBANK-EQ',
'RECLTD-EQ',
'RHIM-EQ',
'RITES-EQ',
'RADICO-EQ',
'RVNL-EQ',
'RAILTEL-EQ',
'RAINBOW-EQ',
'RKFORGE-EQ',
'RCF-EQ',
'RTNINDIA-EQ',
'RAYMONDLSL-EQ',
'RAYMOND-EQ',
'REDINGTON-EQ',
'RELIANCE-EQ',
'RPOWER-BE',
'ROUTE-EQ',
'SBFC-EQ',
'SBICARD-EQ',
'SBILIFE-EQ',
'SJVN-EQ',
'SKFINDIA-EQ',
'SRF-EQ',
'SAGILITY-EQ',
'SAILIFE-EQ',
'SAMMAANCAP-EQ',
'MOTHERSON-EQ',
'SAPPHIRE-EQ',
'SARDAEN-EQ',
'SAREGAMA-EQ',
'SCHAEFFLER-EQ',
'SCHNEIDER-BE',
'SCI-EQ',
'SHREECEM-EQ',
'RENUKA-EQ',
'SHRIRAMFIN-EQ',
'SHYAMMETL-EQ',
'SIEMENS-EQ',
'SIGNATURE-EQ',
'SOBHA-EQ',
'SOLARINDS-EQ',
'SONACOMS-EQ',
'SONATSOFTW-EQ',
'STARHEALTH-EQ',
'SBIN-EQ',
'SAIL-EQ',
'SWSOLAR-EQ',
'SUMICHEM-EQ',
'SUNPHARMA-EQ',
'SUNTV-EQ',
'SUNDARMFIN-EQ',
'SUNDRMFAST-EQ',
'SUPREMEIND-EQ',
'SUZLON-EQ',
'SWANENERGY-EQ',
'SWIGGY-EQ',
'SYNGENE-EQ',
'SYRMA-EQ',
'TBOTEK-EQ',
'TVSMOTOR-EQ',
'TANLA-EQ',
'TATACHEM-EQ',
'TATACOMM-EQ',
'TCS-EQ',
'TATACONSUM-EQ',
'TATAELXSI-EQ',
'TATAINVEST-EQ',
'TATAMOTORS-EQ',
'TATAPOWER-EQ',
'TATASTEEL-EQ',
'TATATECH-EQ',
'TTML-BE',
'TECHM-EQ',
'TECHNOE-EQ',
'TEJASNET-EQ',
'NIACL-EQ',
'RAMCOCEM-EQ',
'THERMAX-EQ',
'TIMKEN-EQ',
'TITAGARH-EQ',
'TITAN-EQ',
'TORNTPHARM-EQ',
'TORNTPOWER-EQ',
'TARIL-EQ',
'TRENT-EQ',
'TRIDENT-EQ',
'TRIVENI-EQ',
'TRITURBINE-EQ',
'TIINDIA-EQ',
'UCOBANK-EQ',
'UNOMINDA-EQ',
'UPL-EQ',
'UTIAMC-EQ',
'ULTRACEMCO-EQ',
'UNIONBANK-EQ',
'UBL-EQ',
'UNITDSPR-EQ',
'USHAMART-EQ',
'VGUARD-EQ',
'DBREALTY-EQ',
'VTL-EQ',
'VBL-EQ',
'MANYAVAR-EQ',
'VEDL-EQ',
'VIJAYA-EQ',
'VMM-EQ',
'IDEA-EQ',
'VOLTAS-EQ',
'WAAREEENER-EQ',
'WELCORP-EQ',
'WELSPUNLIV-EQ',
'WESTLIFE-EQ',
'WHIRLPOOL-EQ',
'WIPRO-EQ',
'WOCKPHARMA-EQ',
'YESBANK-EQ',
'ZFCVINDIA-EQ',
'ZEEL-EQ',
'ZENTEC-EQ',
'ZENSARTECH-EQ',
'ZYDUSLIFE-EQ',
'ECLERX-EQ'
]
    
    # Filter the token dataframe for ONLY the target stocks on the NSE exchange
    eq_stocks = token_df[
        token_df['symbol'].isin(target_stocks) &
        (token_df['exch_seg'].str.upper() == 'NSE') &
        token_df['token'].notna()
    ]

    print(f"✅ Loaded {len(eq_stocks)} equity stocks from the target list.")
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


# === VALIDATION FUNCTION FOR ±6% LOWS === #
def is_valid_double_bottom(first_low, second_low):
    if first_low is not None and second_low is not None:
        lower = min(first_low, second_low)
        upper = max(first_low, second_low)
        if upper <= lower * 1.06:
            return True
        else:
            return False
    else:
        return False

# ----------- Double Bottom Detection (Modified for Dynamic Order) -----------
def detect_double_bottom(df, price_col='Close', low_tolerance_pct=3, rally_pct_req=10, lookback_for_lows_factor=0.7):
    df = df.copy().reset_index(drop=True)
    prices = df[price_col].values
    dates = df['Date'].values
    n_points = len(df)

    dynamic_order = max(5, int(n_points * 0.1))
    lookback_for_lows = max(20, int(n_points * lookback_for_lows_factor))

    potential_db_info = {
        'first_low_idx': None, 'second_low_idx': None, 'neckline_idx': None,
        'first_low_price': None, 'second_low_price': None, 'neckline_price': None,
        'first_low_date': None, 'second_low_date': None, 'neckline_date': None
    }
    detected = False

    local_min_idx = argrelextrema(prices, np.less, order=dynamic_order)[0]
    if len(local_min_idx) < 2:
        return {'potential_db_info': potential_db_info, 'detected': detected}

    recent_local_min_idx = [idx for idx in local_min_idx if idx >= len(df) - lookback_for_lows]
    if len(recent_local_min_idx) < 2:
        return {'potential_db_info': potential_db_info, 'detected': detected}

    sorted_minima_chronological = sorted([(prices[idx], idx) for idx in recent_local_min_idx], key=lambda x: x[1])

    for i in range(len(sorted_minima_chronological)):
        for j in range(i + 1, len(sorted_minima_chronological)):
            first_low_price, first_idx = sorted_minima_chronological[i]
            second_low_price, second_idx = sorted_minima_chronological[j]
            if first_idx >= second_idx:
                continue

            low_diff_pct = abs(first_low_price - second_low_price) / first_low_price
            if low_diff_pct <= 0.04:
                continue

            peak_range = prices[first_idx:second_idx + 1]
            if not len(peak_range):
                continue
            peak_idx_rel = np.argmax(peak_range)
            neckline_idx = first_idx + peak_idx_rel
            neckline_price = prices[neckline_idx]
            if neckline_idx in [first_idx, second_idx]:
                continue

            rally_pct = (neckline_price - first_low_price) / first_low_price
            if rally_pct < rally_pct_req / 100:
                continue

            pre_low_peak_price = np.max(prices[:first_idx]) if first_idx > 0 else 0
            if pre_low_peak_price < neckline_price:
                continue
            drop_pct = (pre_low_peak_price - first_low_price) / first_low_price
            if drop_pct < rally_pct:
                continue

            if second_idx + 1 < len(prices) and np.any(prices[second_idx + 1:] >= neckline_price):
                continue

            post_window = prices[second_idx + 1: second_idx + 1 + dynamic_order]
            if not len(post_window) or np.max(post_window) < second_low_price * (1 + rally_pct_req / 200):
                continue
            prices_after_l2 = prices[second_idx + 1:]
            if len(prices_after_l2) > 0 and np.min(prices_after_l2) < second_low_price:
                continue

            potential_db_info.update({
                'first_low_idx': first_idx, 'second_low_idx': second_idx,
                'neckline_idx': neckline_idx,
                'first_low_price': first_low_price,
                'second_low_price': second_low_price,
                'neckline_price': neckline_price,
                'first_low_date': dates[first_idx],
                'second_low_date': dates[second_idx],
                'neckline_date': dates[neckline_idx]
            })
            detected = True
            return {'potential_db_info': potential_db_info, 'detected': detected}

    return {'potential_db_info': potential_db_info, 'detected': detected}


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
potential_db_data_all_runs = {}
already_detected_ihs = set()
already_detected_db = set()

print("\n🔍 Starting pattern detection...")
for from_str, to_str in date_ranges:
    print(f"\n📆 Range: {from_str} to {to_str}")
    detected_ihs = []
    detected_db = []
    potential_db_for_range = {}

    from_dt = pd.to_datetime(from_str.split()[0])
    to_dt = pd.to_datetime(to_str.split()[0])

    for sym, df in all_stock_data.items():
        if sym in already_detected_ihs and sym in already_detected_db:
            continue

        sliced = df[(df['Date'] >= from_dt) & (df['Date'] <= to_dt)].copy()
        if len(sliced) < 50:
            # print(f"Skipping {sym} ({len(sliced)} bars)")
            continue

        # --- IHS detection (unchanged) ---
        # ...

        # --- Double bottom detection ---
        if sym not in already_detected_db:
            db_result = detect_double_bottom(sliced, rally_pct_req=10)
            info = db_result['potential_db_info']
            first_low = info.get('first_low_price')
            second_low = info.get('second_low_price')

            # Validate that lows are within the required percentage range
            if not is_valid_double_bottom(first_low, second_low):
                continue

            # If validation passes, check for full detection
            if db_result['detected']:
                print(f"✅ [DETECTED] {sym} – Double Bottom in range {from_dt.date()} to {to_dt.date()}")
                already_detected_db.add(sym)
                detected_db.append(sym)
                potential_db_for_range[sym] = info
            else:
                # Optional: log patterns that have key points but are not fully confirmed
                # print(f"--- {sym} – Potential points found but not confirmed.")
                pass


    detected_ihs_all_runs[from_str] = detected_ihs
    detected_db_all_runs[from_str] = detected_db
    potential_db_data_all_runs[from_str] = potential_db_for_range

# === [MODIFIED] PLOT UNIQUE DB STOCKS TO A MULTI-PAGE PDF === #

unique_db_stocks = list(already_detected_db)
n_stocks = len(unique_db_stocks)

if n_stocks > 0:
    pdf_filename = 'double_bottom_charts.pdf'

     # --- ADD THIS LINE ---
    full_path = os.path.abspath(pdf_filename) 
    
    print(f"\n📊 Generating PDF with {n_stocks} unique double bottom patterns to '{pdf_filename}'...")

    # Define the layout for each page
    stocks_per_page = 6
    cols = 2
    rows_per_page = 3 # 3 stocks per column

    # Calculate the total number of pages needed
    num_pages = math.ceil(n_stocks / stocks_per_page)

    with PdfPages(pdf_filename) as pdf:
        for page_num in range(num_pages):
            # Create a new figure for each page with a size that accommodates 3 rows well
            fig, axes = plt.subplots(rows_per_page, cols, figsize=(15, 20))
            fig.suptitle(f'Double Bottom Patterns - Page {page_num + 1}', fontsize=16, y=0.98)

            # Get the slice of stocks for the current page
            start_index = page_num * stocks_per_page
            end_index = start_index + stocks_per_page
            page_stocks = unique_db_stocks[start_index:end_index]

            # Flatten the axes array for easy iteration
            axes = axes.flatten()

            for i, sym in enumerate(page_stocks):
                ax = axes[i] # Get the current subplot

                # Find the most recent date range where the stock was detected
                latest_range = None
                for dr in reversed(date_ranges):
                    dr_key = dr[0]
                    if sym in detected_db_all_runs.get(dr_key, []):
                        latest_range = dr_key
                        break

                if latest_range is None:
                    ax.set_title(f"{sym} - Data Error", fontsize=9)
                    ax.text(0.5, 0.5, 'Could not find detection range.', ha='center', va='center')
                    continue

                info = potential_db_data_all_runs.get(latest_range, {}).get(sym)
                df = all_stock_data[sym]

                # Slice the DF for plotting range
                from_dt = pd.to_datetime(latest_range.split()[0])
                sliced = df[df['Date'] >= from_dt].copy()

                if len(sliced) == 0:
                    ax.set_title(f"{sym} - No Data", fontsize=9)
                    ax.text(0.5, 0.5, 'No data for this range.', ha='center', va='center')
                    continue

                # --- Plotting on the subplot (ax) ---
                ax.plot(sliced['Date'], sliced['Close'], label='Close Price', color='black', linewidth=1)

                if info:
                    # Combine labels for lows to avoid clutter
                    lows_labeled = False
                    if info.get('first_low_date') and info.get('first_low_price'):
                        ax.plot(info['first_low_date'], info['first_low_price'], 'go', markersize=5, label='Lows')
                        lows_labeled = True
                    if info.get('second_low_date') and info.get('second_low_price'):
                        ax.plot(info['second_low_date'], info['second_low_price'], 'go', markersize=5, label='Lows' if not lows_labeled else "")

                    if info.get('neckline_price'):
                        ax.axhline(info['neckline_price'], color='red', linestyle='--', linewidth=1, label='Neckline')

                ax.set_title(f"{sym}", fontsize=10)
                ax.tick_params(axis='x', rotation=45, labelsize=8)
                ax.tick_params(axis='y', labelsize=8)
                ax.grid(True, linestyle='--', alpha=0.6)
                ax.legend(fontsize=8)

            # Hide any unused subplots on the last page
            for j in range(len(page_stocks), stocks_per_page):
                fig.delaxes(axes[j])

            # Adjust layout and save the figure (the whole page) to the PDF
            fig.tight_layout(rect=[0, 0.03, 1, 0.96])
            pdf.savefig(fig)
            plt.close(fig) # IMPORTANT: Close the figure to free memory

    print(f"✅ PDF saved successfully to: {full_path}")
else:
    print("❌ No double bottom stocks detected to plot.")


# === SUMMARY === #
print("\n📋 Summary of Patterns Detected:")
print(f"{'Date Range':<25} | {'Pattern':<15} | {'Stock':<12}")
print("-" * 60)
has_patterns = False

for from_date_key in detected_db_all_runs:
    matching_range = next((dr for dr in date_ranges if dr[0] == from_date_key), None)
    to_clean = matching_range[1].split()[0] if matching_range else "N/A"
    from_clean = from_date_key.split()[0]

    for sym in detected_db_all_runs[from_date_key]:
        print(f"{from_clean} to {to_clean:<10} | {'Double Bottom':<15} | {sym:<12}")
        has_patterns = True

if not has_patterns:

    print("No patterns were detected in any range.")
