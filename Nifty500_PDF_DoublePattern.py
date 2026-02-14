from SmartApi import SmartConnect
import pyotp
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import math
from matplotlib.backends.backend_pdf import PdfPages
import os

# =====================================================
# CONFIGURATION
# =====================================================
api_key = 'joebm3IW'
user_id = 'AAAF838728'
pin = '0421'
totpkey = 'SS4OWS4U3LH5YF66ZR63AYPUDE'

INTERVAL = "ONE_DAY"
HISTORY_YEARS = 12
EMA_PROX_PCT = 0.008
MAX_API_DAYS = 2000
SPIKE_MULTIPLIER = 2
RECENT_DAYS = 3

# =====================================================
# DATE SETUP
# =====================================================
to_date = datetime.now()
from_date = to_date - relativedelta(years=HISTORY_YEARS)
ath_cutoff_date = to_date - timedelta(days=365)
recent_cutoff = to_date - timedelta(days=RECENT_DAYS)

# =====================================================
# LOGIN
# =====================================================
smartApi = SmartConnect(api_key=api_key)
data = smartApi.generateSession(user_id, pin, pyotp.TOTP(totpkey).now())
smartApi.getProfile(data['data']['refreshToken'])
print("✅ Login successful")

# =====================================================
# TARGET STOCKS
# =====================================================
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

# =====================================================
# TOKEN MASTER
# =====================================================
token_df = pd.DataFrame(
    requests.get(
        "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    ).json()
)

eq_stocks = token_df[
    token_df['symbol'].str.upper().isin([s.upper() for s in target_symbols]) &
    (token_df['exch_seg'].str.upper() == 'NSE')
]

# =====================================================
# FUNCTIONS
# =====================================================
def detect_multi_year_breakout(df):
    hist_df = df[df['Date'] <= ath_cutoff_date]
    if hist_df.empty:
        return None

    # --- ATH value ---
    ath = hist_df['High'].max()

    # --- FIRST ATH date (force scalar) ---
    ath_row = hist_df[hist_df['High'] == ath].iloc[0]
    ath_date = ath_row['Date']

    # --- Post ATH data ---
    post_ath_df = df[df['Date'] > ath_date]
    if post_ath_df.empty:
        return None

    # --- Rule 2: +2% / -2% logic ---
    crossed_4pct = False

    for close in post_ath_df['Close']:
        if close > ath * 1.02:
            crossed_4pct = True

        if crossed_4pct and close < ath * 0.98:
            return None   # ❌ Invalid MYB

    # --- Rule 3: Today condition (unchanged) ---
    today_close = df['Close'].iloc[-1]
    today_high  = df['High'].iloc[-1]

    if not (ath * 0.98 <= today_close <= ath * 1.10):
        return None

    return {
        "ath": ath,
        "ath_date": ath_date,
        "close": today_close,
        "high": today_high
    }



def fetch_full_history(symbol, token):
    chunks = []
    current_end = to_date
    window_days = MAX_API_DAYS + 30

    while current_end > from_date:
        current_start = max(from_date, current_end - timedelta(days=window_days))

        res = smartApi.getCandleData({
            "exchange": "NSE",
            "symboltoken": token,
            "interval": INTERVAL,
            "fromdate": current_start.strftime("%Y-%m-%d %H:%M"),
            "todate": current_end.strftime("%Y-%m-%d %H:%M")
        })
        time.sleep(0.4)

        if not res.get("data"):
            break

        df = pd.DataFrame(res["data"],
                          columns=["Date","Open","High","Low","Close","Volume"])
        df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
        chunks.append(df)

        oldest = df["Date"].min()
        if oldest <= from_date:
            break
        current_end = oldest - timedelta(days=1)

    if not chunks:
        return None

    df = pd.concat(chunks).drop_duplicates("Date").sort_values("Date")
    return df

# =====================================================
# FETCH + CALCULATIONS
# =====================================================
all_data = {}
myb_stocks = {}
ema50_near, ema100_near, ema200_near = [], [], []

for _, r in eq_stocks.iterrows():
    sym, token = r['symbol'], str(r['token'])
    df = fetch_full_history(sym, token)
    if df is None or df.empty:
        continue

    df['EMA20']  = df['Close'].ewm(span=20, adjust=False).mean()
    df['EMA50']  = df['Close'].ewm(span=50, adjust=False).mean()
    df['EMA100'] = df['Close'].ewm(span=100, adjust=False).mean()
    df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()
    df['VOL_MA20'] = df['Volume'].rolling(20).mean()

    all_data[sym] = df
    close = df['Close'].iloc[-1]

    myb = detect_multi_year_breakout(df)
    if myb:
        myb_stocks[sym] = myb

    ema50, ema100, ema200 = df['EMA50'].iloc[-1], df['EMA100'].iloc[-1], df['EMA200'].iloc[-1]
    if ema50 > ema100 > ema200:
        if close >= ema50 and abs(close - ema50) / ema50 <= EMA_PROX_PCT:
            ema50_near.append(sym)
        if close >= ema100 and abs(close - ema100) / ema100 <= EMA_PROX_PCT:
            ema100_near.append(sym)
        if close >= ema200 and abs(close - ema200) / ema200 <= EMA_PROX_PCT:
            ema200_near.append(sym)

# =====================================================
# PDF REPORT
# =====================================================
pdf_file = "NIFTY500_Different_Patterns.pdf"
stocks_per_page, cols, rows = 6, 2, 3

sections = [
    ("Multi-Year Breakout Stocks", list(myb_stocks.keys()), "MYB"),
    ("Near EMA-50 (≤0.8%)", ema50_near, "EMA"),
    ("Near EMA-100 (≤0.8%)", ema100_near, "EMA"),
    ("Near EMA-200 (≤0.8%)", ema200_near, "EMA"),
]

with PdfPages(pdf_file) as pdf:
    for title, symbols, mode in sections:
        if not symbols:
            continue

        pages = math.ceil(len(symbols) / stocks_per_page)
        for p in range(pages):
            fig, axes = plt.subplots(rows, cols, figsize=(15, 20), constrained_layout=True)
            fig.suptitle(title, fontsize=20)
            axes = axes.flatten()

            subset = symbols[p*stocks_per_page:(p+1)*stocks_per_page]

            for i, sym in enumerate(subset):
                ax = axes[i]
                df = all_data[sym]

                if mode == "MYB":
                    plot_df = df[df['Date'] >= df['Date'].max() - relativedelta(months=6)]
                    ax.plot(plot_df['Date'], plot_df['Close'], color='black')
                    ax.plot(plot_df['Date'], plot_df['EMA20'], color='green')

                    info = myb_stocks[sym]
                    ath = info['ath']
                    close = info['close']
                    high = info['high']

                    ax.axhline(ath, color='purple', linestyle='--')
                    ax.relim(); ax.autoscale_view()
                    y_min, y_max = ax.get_ylim()
                    ax.set_ylim(y_min, y_max)

                    mid_date = plot_df['Date'].iloc[len(plot_df)//2]

                    ax.text(mid_date, ath-(y_max-y_min)*0.04,
                            f'ATH : {ath:.2f}', color='purple',
                            ha='center', va='top',
                            bbox=dict(facecolor='white', alpha=0.8))

                    last_date = plot_df['Date'].iloc[-1]

                    # --- Today Close ---
                    ax.scatter(
                        last_date,
                        close,
                        color='red',
                        zorder=6
                    )
                    ax.text(
                        last_date,
                        close,
                        f' Close : {close:.2f}',
                        color='red',
                        fontsize=9,
                        va='bottom',
                        ha='left'
                    )

                    # --- Today High ---
                    ax.scatter(
                        last_date,
                        high,
                        marker='^',
                        color='blue',
                        zorder=6
                    )
                    ax.text(
                        last_date,
                        high,
                        f' High : {high:.2f}',
                        color='blue',
                        fontsize=9,
                        va='bottom',
                        ha='left'
                    )


                    pct = ((close - ath) / ath) * 100
                    ath_date = info['ath_date'].strftime('%d-%b-%Y')

                    ax.text(
                        0.02, 0.95,
                        f'Δ from ATH : {pct:.2f}%\nATH Date : {ath_date}',
                        transform=ax.transAxes,
                        fontsize=10,
                        ha='left',
                        va='top',
                        bbox=dict(facecolor='white', alpha=0.85)
                    )


                else:
                    plot_df = df[df['Date'] >= df['Date'].max() - relativedelta(years=1)]
                    ax.plot(plot_df['Date'], plot_df['Close'], color='black')
                    ax.plot(plot_df['Date'], plot_df['EMA50'], color='blue')
                    ax.plot(plot_df['Date'], plot_df['EMA100'], color='orange')
                    ax.plot(plot_df['Date'], plot_df['EMA200'], color='red')

                    ax2 = ax.twinx()
                    vol = plot_df[plot_df['Date'] >= plot_df['Date'].max() - timedelta(days=365)]
                    ax2.bar(vol['Date'], vol['Volume'], alpha=0.25)
                    ax2.plot(plot_df['Date'], plot_df['VOL_MA20'], color='pink')
                    spikes = plot_df[
                        (plot_df['Volume'] >= SPIKE_MULTIPLIER * plot_df['VOL_MA20']) &
                        (plot_df['Date'] >= recent_cutoff)
                    ]
                    ax2.bar(spikes['Date'], spikes['Volume'], color='red')
                    ax2.set_yticks([])

                ax.set_title(sym)
                ax.grid(True)

            for j in range(len(subset), stocks_per_page):
                fig.delaxes(axes[j])

            pdf.savefig(fig)
            plt.close(fig)


print(f"\n✅ FINAL PDF generated at:\n{os.path.abspath(pdf_file)}")

