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
api_key = 'joebm3IW'
user_id = 'AAAF838728'
pin = '0421'
totpkey = 'SS4OWS4U3LH5YF66ZR63AYPUDE'
num_stocks = 100 # Adjust as needed

# 1. THE END DATE FOR ALL ANALYSIS WINDOWS IS THE CURRENT DATE
to_date_obj = datetime.now() # This is currently August 10, 2025
to_date = to_date_obj.strftime("%Y-%m-%d") + " 15:30"

# 2. DEFINE THE CUTOFF FOR THE *START DATE* OF THE ANALYSIS WINDOW
# The loop will not start a new range after this date.
end_cutoff_date = datetime(2025, 1, 31)

# Start date for the entire data fetch (8 years for multi-year breakout)
multi_year_start_date = to_date_obj - relativedelta(years=8)
longest_from_date = multi_year_start_date.strftime("%Y-%m-%d %H:%M")

# Start date for the first iterative analysis window (IHS and Double Bottom)
ihs_db_start_date = datetime(2022, 1, 1, 9, 15)

interval = "ONE_DAY"

# === Date Range Generation (Modified Logic) === #
# This loop now creates analysis windows where the start date slides forward
# monthly, but the end date is always the current date. The loop stops
# creating new windows once the start date passes the end_cutoff_date.
date_ranges = []
current = ihs_db_start_date
while current <= end_cutoff_date:
    from_str = current.strftime("%Y-%m-%d %H:%M")
    # The 'to_date' is fixed to the current date for every range
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
    print(f"❌ Login failed: {e}")
    exit()

# === FETCH TOKEN MASTER (MODIFIED FOR SPECIFIC STOCKS) === #

# 1. Define the specific list of stocks you want to analyze here
target_symbols = ['360ONE-EQ',
'3MINDIA-EQ',
'TYROCARE-EQ',
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

# The num_stocks variable is no longer needed
# num_stocks = 100 

print(f"🎯 Target stocks defined: {len(target_symbols)}")

url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
try:
    token_data = requests.get(url).json()
    token_df = pd.DataFrame(token_data)

    # Convert target symbols to uppercase for reliable matching
    target_symbols_upper = [s.upper() for s in target_symbols]

    # Filter the master dataframe to get data ONLY for the target stocks
    eq_stocks = token_df[
        token_df['symbol'].str.upper().isin(target_symbols_upper) &
        (token_df['exch_seg'].str.upper() == 'NSE')
    ].copy()

    # Verify that all target stocks were found
    if len(eq_stocks) != len(target_symbols):
        found_symbols = list(eq_stocks['symbol'].str.upper())
        missing_symbols = [s for s in target_symbols_upper if s not in found_symbols]
        print(f"⚠️ Warning: Could not find all target stocks. Missing: {missing_symbols}")

    print(f"✅ Found {len(eq_stocks)} of {len(target_symbols)} target stocks in the master list.")

except Exception as e:
    print(f"❌ Error fetching token master or filtering stocks: {e}")
    exit()

# === PATTERN DETECTION FUNCTIONS === #

def detect_multi_year_breakout(df, proximity_pct=5.0):
    """
    Detects if the latest price is near its multi-year high.
    The All-Time High (ATH) is considered the neckline.
    """
    df = df.copy().reset_index(drop=True)
    if len(df) < 2:
        return None

    # All-time high is the neckline
    neckline_ath = df['High'].max()
    latest_price = df['Close'].iloc[-1]

    # Check if the latest price is within the specified percentage of the ATH
    if latest_price >= (neckline_ath * (1 - proximity_pct / 100)):
        return {
            "neckline_price": neckline_ath,
            "latest_price": latest_price,
            "detected": True
        }
    return None

def detect_forming_ihs(df, order=5, shoulder_tolerance_pct=10.0, rally_req_pct=0.80,
                       min_shoulder_head_diff_pct=2.0, rs_ls_tolerance_pct=2.0,
                       min_time_separation=10, min_rs_recovery_pct=0.60):
    df = df.copy().reset_index(drop=True)
    lows = df['Low'].values
    highs = df['High'].values
    closes = df['Close'].values
    dates = df['Date'].values
    n = len(df)
    minima_idx = argrelextrema(lows, np.less, order=order)[0]
    maxima_idx = argrelextrema(highs, np.greater, order=order)[0]
    if len(minima_idx) < 3 or len(maxima_idx) < 2: return None
    for i in range(len(minima_idx) - 2):
        ls_idx, head_idx, rs_idx = minima_idx[i], minima_idx[i + 1], minima_idx[i + 2]
        if (head_idx - ls_idx < min_time_separation) or (rs_idx - head_idx < min_time_separation): continue
        ls_price, head_price, rs_price = lows[ls_idx], lows[head_idx], lows[rs_idx]
        if not (head_price < ls_price and head_price < rs_price): continue
        if (abs(ls_price - head_price) / head_price * 100 < min_shoulder_head_diff_pct or
            abs(rs_price - head_price) / head_price * 100 < min_shoulder_head_diff_pct or
            abs(ls_price - rs_price) / min(ls_price, rs_price) * 100 > shoulder_tolerance_pct): continue
        if (ls_price - rs_price) / ls_price * 100 > rs_ls_tolerance_pct: continue
        peak1_idx = max((p for p in maxima_idx if ls_idx < p < head_idx), key=lambda p: highs[p], default=None)
        peak2_idx = max((p for p in maxima_idx if head_idx < p < rs_idx), key=lambda p: highs[p], default=None)
        if peak1_idx is None or peak2_idx is None: continue
        neckline_price = min(highs[peak1_idx], highs[peak2_idx])
        rally1_dist = highs[peak1_idx] - ls_price
        neckline1_dist = neckline_price - ls_price
        if neckline1_dist <= 0 or (rally1_dist / neckline1_dist) < rally_req_pct: continue
        rally2_dist = highs[peak2_idx] - head_price
        neckline2_dist = neckline_price - head_price
        if neckline2_dist <= 0 or (rally2_dist / neckline2_dist) < rally_req_pct: continue
        if rs_idx + 1 >= n: continue
        closes_after_rs = closes[rs_idx + 1:]
        if np.max(closes_after_rs) >= neckline_price: continue
        if np.min(lows[rs_idx + 1:]) < rs_price: continue
        distance_to_neckline = neckline_price - rs_price
        if distance_to_neckline > 0:
            min_confirmation_price = rs_price + (distance_to_neckline * min_rs_recovery_pct)
            if np.max(closes_after_rs) < min_confirmation_price: continue
        else: continue
        return {"ls_price": ls_price, "head_price": head_price, "rs_price": rs_price, "ls_date": dates[ls_idx], "head_date": dates[head_idx], "rs_date": dates[rs_idx], "neckline_price": neckline_price, "detected": True}
    return None

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

def detect_double_bottom(df, price_col='Close', low_tolerance_pct=3, rally_pct_req=10, lookback_for_lows_factor=0.7):
    df = df.copy().reset_index(drop=True)
    prices = df[price_col].values
    dates = df['Date'].values
    n_points = len(df)
    dynamic_order = max(5, int(n_points * 0.1))
    lookback_for_lows = max(20, int(n_points * lookback_for_lows_factor))
    potential_db_info = {'first_low_idx': None, 'second_low_idx': None, 'neckline_idx': None, 'first_low_price': None, 'second_low_price': None, 'neckline_price': None, 'first_low_date': None, 'second_low_date': None, 'neckline_date': None}
    detected = False
    local_min_idx = argrelextrema(prices, np.less, order=dynamic_order)[0]
    if len(local_min_idx) < 2: return {'potential_db_info': potential_db_info, 'detected': detected}
    recent_local_min_idx = [idx for idx in local_min_idx if idx >= len(df) - lookback_for_lows]
    if len(recent_local_min_idx) < 2: return {'potential_db_info': potential_db_info, 'detected': detected}
    sorted_minima_chronological = sorted([(prices[idx], idx) for idx in recent_local_min_idx], key=lambda x: x[1])
    for i in range(len(sorted_minima_chronological)):
        for j in range(i + 1, len(sorted_minima_chronological)):
            first_low_price, first_idx = sorted_minima_chronological[i]
            second_low_price, second_idx = sorted_minima_chronological[j]
            if first_idx >= second_idx: continue
            low_diff_pct = abs(first_low_price - second_low_price) / first_low_price
            if low_diff_pct <= 0.04: continue
            peak_range = prices[first_idx:second_idx + 1]
            if not len(peak_range): continue
            peak_idx_rel = np.argmax(peak_range)
            neckline_idx = first_idx + peak_idx_rel
            neckline_price = prices[neckline_idx]
            if neckline_idx in [first_idx, second_idx]: continue
            rally_pct = (neckline_price - first_low_price) / first_low_price
            if rally_pct < rally_pct_req / 100: continue
            pre_low_peak_price = np.max(prices[:first_idx]) if first_idx > 0 else 0
            if pre_low_peak_price < neckline_price: continue
            drop_pct = (pre_low_peak_price - first_low_price) / first_low_price
            if drop_pct < rally_pct: continue
            if second_idx + 1 < len(prices) and np.any(prices[second_idx + 1:] >= neckline_price): continue
            post_window = prices[second_idx + 1: second_idx + 1 + dynamic_order]
            if not len(post_window) or np.max(post_window) < second_low_price * (1 + rally_pct_req / 200): continue
            prices_after_l2 = prices[second_idx + 1:]
            if len(prices_after_l2) > 0 and np.min(prices_after_l2) < second_low_price: continue
            potential_db_info.update({'first_low_idx': first_idx, 'second_low_idx': second_idx, 'neckline_idx': neckline_idx, 'first_low_price': first_low_price, 'second_low_price': second_low_price, 'neckline_price': neckline_price, 'first_low_date': dates[first_idx], 'second_low_date': dates[second_idx], 'neckline_date': dates[neckline_idx]})
            detected = True
            return {'potential_db_info': potential_db_info, 'detected': detected}
    return {'potential_db_info': potential_db_info, 'detected': detected}

def fetch_candle_data_with_retry(symbol, token, retries=3, delay=2):
    for attempt in range(retries):
        try:
            # Use the longest_from_date to get all necessary data at once
            params = {"exchange": "NSE", "symboltoken": token, "interval": interval, "fromdate": longest_from_date, "todate": to_date}
            res = smartApi.getCandleData(params)
            time.sleep(0.4)
            if res.get('data'):
                df = pd.DataFrame(res['data'], columns=["Date", "Open", "High", "Low", "Close", "Volume"])
                df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
                return df
        except Exception as e: print(f"⚠️ Retry {attempt + 1}/{retries} for {symbol} failed: {e}")
        time.sleep(delay)
    print(f"❌ Failed all retries for {symbol}")
    return None

# === FETCH DATA === #
all_stock_data = {}
print("\n📥 Fetching historical data...")
for index, row in eq_stocks.iterrows():
    sym, token = row['symbol'], str(row['token'])
    print(f"⏳ Fetching ({index + 1}/{len(eq_stocks)}) {sym}...")
    df = fetch_candle_data_with_retry(sym, token)
    if df is not None and not df.empty: all_stock_data[sym] = df

# === ANALYSIS LOOP === #
# Dictionaries to store results
potential_ihs_data, potential_db_data, potential_myb_data = {}, {}, {}
detected_stocks = {} # Stores detected pattern info for each stock

print("\n🔍 Starting pattern detection...")
for sym, df_full in all_stock_data.items():
    
    # --- Multi-Year Breakout Detection ---
    # This uses the full 8-year dataframe
    myb_result = detect_multi_year_breakout(df_full)
    if myb_result and myb_result.get("detected"):
        print(f"✅ [MULTI-YEAR BREAKOUT] {sym}")
        # Store under the symbol key, with pattern type
        detected_stocks[sym] = {"pattern": "Multi-Year Breakout", "data": myb_result, "date_range": "8-Year"}
        potential_myb_data[sym] = myb_result # Keep for summary
        continue # If a multi-year breakout is found, we can skip other patterns

    # --- IHS and Double Bottom Detection (Iterative) ---
    # Use the date ranges defined earlier for these patterns
    for from_str, to_str in date_ranges:
        if sym in detected_stocks: continue # Already found a pattern for this stock in a previous range

        from_dt, to_dt = pd.to_datetime(from_str.split()[0]), pd.to_datetime(to_str.split()[0])
        sliced = df_full[(df_full['Date'] >= from_dt) & (df_full['Date'] <= to_dt)].copy()
        if len(sliced) < 60: continue

        # --- IHS Detection ---
        ihs_result = detect_forming_ihs(sliced)
        if ihs_result and ihs_result.get("detected"):
            print(f"✅ [FORMING IHS] {sym} in range {from_str.split()[0]} to {to_str.split()[0]}")
            date_key = f"{from_str.split()[0]} to {to_str.split()[0]}"
            detected_stocks[sym] = {"pattern": "Inverse H&S", "data": ihs_result, "date_range": date_key}
            potential_ihs_data[sym] = ihs_result # Keep for summary
            break # Move to the next stock once a pattern is found

        # --- Double Bottom Detection ---
        db_result = detect_double_bottom(sliced, rally_pct_req=10)
        info = db_result['potential_db_info']
        if db_result['detected'] and is_valid_double_bottom(info.get('first_low_price'), info.get('second_low_price')):
            print(f"✅ [FORMING DB] {sym} in range {from_str.split()[0]} to {to_str.split()[0]}")
            date_key = f"{from_str.split()[0]} to {to_str.split()[0]}"
            detected_stocks[sym] = {"pattern": "Double Bottom", "data": info, "date_range": date_key}
            potential_db_data[sym] = info # Keep for summary
            break # Move to the next stock

# === UNIFIED PLOTTING LOOP (NOW GROUPED BY PATTERN) === #

# Define a custom order for sorting patterns in the PDF and summary
pattern_order = {"Multi-Year Breakout": 0, "Inverse H&S": 1, "Double Bottom": 2}

# Sort the detected stocks primarily by pattern type, then by symbol
sorted_detections = sorted(
    detected_stocks.items(),
    key=lambda item: (pattern_order.get(item[1]['pattern'], 99), item[0])
)

if sorted_detections:
    pdf_filename = 'NIFTY500_Different_Patterns.pdf'
    full_path_pdf = os.path.abspath(pdf_filename)
    print(f"\n📊 Generating single PDF with {len(sorted_detections)} patterns (grouped by type) to '{pdf_filename}'...")

    stocks_per_page, cols, rows_per_page = 6, 2, 3
    num_pages = math.ceil(len(sorted_detections) / stocks_per_page)

    with PdfPages(pdf_filename) as pdf:
        for page_num in range(num_pages):
            fig, axes = plt.subplots(rows_per_page, cols, figsize=(15, 20), constrained_layout=True)
            fig.suptitle(f'All Detected Patterns - Page {page_num + 1}', fontsize=20)
            axes = axes.flatten()
            
            page_items = sorted_detections[page_num * stocks_per_page : (page_num + 1) * stocks_per_page]
            
            # Loop through the sorted items (tuple of symbol and info)
            for i, (sym, detection_info) in enumerate(page_items):
                ax = axes[i]
                pattern_type = detection_info["pattern"]
                info = detection_info["data"]
                df_stock = all_stock_data[sym]

                # Determine the correct date slice for plotting
                if pattern_type == "Multi-Year Breakout":
                    plot_df = df_stock.copy()
                    plot_title = f"{sym}\nMulti-Year Breakout (near ATH)"
                else:
                    date_range_str = detection_info["date_range"]
                    from_dt_str = date_range_str.split(" to ")[0]
                    plot_df = df_stock[df_stock['Date'] >= pd.to_datetime(from_dt_str)].copy()
                    plot_title = f"{sym}\n{pattern_type}"

                ax.plot(plot_df['Date'], plot_df['Close'], label='Close Price', color='black', linewidth=1)
                
                # --- Plot pattern-specific details ---
                if pattern_type == "Inverse H&S":
                    points_dates = [info['ls_date'], info['head_date'], info['rs_date']]
                    points_prices = [info['ls_price'], info['head_price'], info['rs_price']]
                    point_labels = ['LS', 'H', 'RS']
                    ax.plot(points_dates, points_prices, 'bo', markersize=7, alpha=0.8, label='Shoulders & Head')
                    for j, label in enumerate(point_labels): ax.text(points_dates[j], points_prices[j], f' {label}', color='blue', va='top')
                    ax.axhline(info['neckline_price'], color='red', linestyle='--', linewidth=1.5, label=f"Neckline ({info['neckline_price']:.2f})")

                elif pattern_type == "Double Bottom":
                    if info and info.get('first_low_date') and info.get('second_low_date'):
                        ax.plot([info['first_low_date'], info['second_low_date']], [info['first_low_price'], info['second_low_price']], 'go', markersize=7, label='Lows', linestyle='None')
                        ax.axhline(info['neckline_price'], color='red', linestyle='--', linewidth=1.5, label=f"Neckline ({info['neckline_price']:.2f})")
                
                elif pattern_type == "Multi-Year Breakout":
                     ax.axhline(info['neckline_price'], color='purple', linestyle='--', linewidth=1.5, label=f"All-Time High ({info['neckline_price']:.2f})")
                     ax.plot(df_stock['Date'].iloc[-1], info['latest_price'], 'r*', markersize=10, label=f"Latest Price ({info['latest_price']:.2f})")


                ax.set_title(plot_title, fontsize=12)
                ax.tick_params(axis='x', rotation=45, labelsize=8)
                ax.grid(True, linestyle='--', alpha=0.6)
                ax.legend(fontsize=8)
            
            # Hide unused subplots
            for j in range(len(page_items), stocks_per_page):
                fig.delaxes(axes[j])
            
            pdf.savefig(fig)
            plt.close(fig)
            
    print(f"✅ Unified PDF report saved successfully to: {full_path_pdf}")
else:
    print("ℹ️ No patterns of any type were detected.")


# === SUMMARY (NOW GROUPED BY PATTERN) === #
print("\n📋📋📋 Final Summary of Detected Patterns 📋📋📋")
print("-" * 70)
print(f"{'Stock':<15} | {'Pattern':<22} | {'Date Range / Scope':<25}")
print("-" * 70)

if not sorted_detections:
    print("No patterns were detected in any of the analyzed stocks.")
else:
    # Use the same sorted list for the summary to maintain consistency
    for sym, info in sorted_detections:
        pattern_name = info['pattern']
        date_scope = info['date_range']
        print(f"{sym:<15} | {pattern_name:<22} | {date_scope:<25}")

print("-" * 70)

