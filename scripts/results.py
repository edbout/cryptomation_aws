import requests
import pandas as pd
import time
from pathlib import Path

# === CONFIG ===
USER_ADDRESS = '0x4fF44F5E2c039122Daab3047F03D390AACda8915'
ACTIVITY_URL  = 'https://data-api.polymarket.com/activity'
POSITIONS_URL = 'https://data-api.polymarket.com/positions'
SYMS       = ['btc', 'eth', 'sol', 'xrp']
START_DATE = '2026-05-01'   # Only report data from this date onwards

DAY_ORDER = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# === HELPERS ===
def fetch_all(url: str, label: str, user: str, limit: int = 500) -> pd.DataFrame:
    rows, offset = [], 0
    while True:
        try:
            r = requests.get(url, params={'user': user, 'limit': limit, 'offset': offset}, timeout=20)
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            rows.extend(batch)
            print(f"[{label}] fetched {len(batch)} (total {len(rows)})")
            if len(batch) < limit:
                break
            offset += limit
            time.sleep(0.25)
        except Exception as e:
            print(f"Error at offset {offset}: {e}")
            time.sleep(1)
    return pd.DataFrame(rows)

def infer_symbol(slug: str) -> str:
    s = str(slug).lower()
    for sym in SYMS:
        if sym in s and (s.startswith(sym + '-') or f'-{sym}-' in s or f'-{sym}' in s):
            return sym.upper()
    return 'OTHER'

# === FETCH ===
print("Fetching activity...")
activity_df = fetch_all(ACTIVITY_URL, 'activity', USER_ADDRESS)
print(f"Total activity: {len(activity_df)}\n")

print("Fetching positions...")
positions_df = fetch_all(POSITIONS_URL, 'positions', USER_ADDRESS)
print(f"Total positions: {len(positions_df)}\n")

# === CLEAN ACTIVITY ===
ts = pd.to_numeric(activity_df['timestamp'], errors='coerce')
activity_df['timestamp'] = pd.to_datetime(ts, unit='s', utc=True, errors='coerce')
activity_df['date']    = activity_df['timestamp'].dt.normalize()
activity_df['weekday'] = activity_df['timestamp'].dt.day_name()
activity_df['usdcSize'] = pd.to_numeric(activity_df.get('usdcSize', 0), errors='coerce').fillna(0)
activity_df['size']     = pd.to_numeric(activity_df.get('size', 0), errors='coerce').fillna(0)
activity_df['side']     = activity_df.get('side', '').astype(str).str.upper()
activity_df['type']     = activity_df.get('type', '').astype(str).str.upper()
activity_df['eventSlug']   = activity_df.get('eventSlug', '').astype(str)
activity_df['conditionId'] = activity_df.get('conditionId', '').astype(str)
activity_df['symbol']  = activity_df['eventSlug'].apply(infer_symbol)

# Apply May-1 filter
activity_df = activity_df[activity_df['date'] >= START_DATE]
print(f"Activity records from {START_DATE}: {len(activity_df)}")

# Keep only bot symbols + trade/redeem types
cashflow_df = activity_df[
    activity_df['symbol'].isin([s.upper() for s in SYMS]) &
    activity_df['type'].isin(['TRADE', 'REDEEM', 'REDEMPTION'])
].copy()

print(f"Cashflow records: {len(cashflow_df)}")
print(f"  types:   {cashflow_df['type'].value_counts().to_dict()}")
print(f"  symbols: {cashflow_df['symbol'].value_counts().to_dict()}\n")

# Cash flow: buys are outflow (-), sells/redeems are inflow (+)
def get_flow(row):
    if row['type'] in ('REDEEM', 'REDEMPTION'):
        return row['usdcSize']
    if row['type'] == 'TRADE':
        return row['usdcSize'] if row['side'] == 'SELL' else -row['usdcSize']
    return 0.0

cashflow_df['flow'] = cashflow_df.apply(get_flow, axis=1)

# === CLEAN POSITIONS ===
for col in ('size', 'avgPrice', 'currentValue', 'cashPnl', 'percentPnl'):
    positions_df[col] = pd.to_numeric(positions_df[col], errors='coerce').fillna(0)
positions_df['redeemable'] = positions_df['redeemable'].astype(bool)
positions_df['symbol']     = positions_df['slug'].apply(infer_symbol)

# === OVERALL PnL BY SYMBOL ===
buys_by_sym    = cashflow_df[cashflow_df['side'] == 'BUY'].groupby('symbol')['usdcSize'].sum()
sells_by_sym   = cashflow_df[cashflow_df['side'] == 'SELL'].groupby('symbol')['usdcSize'].sum()
redeem_by_sym  = cashflow_df[cashflow_df['type'].isin(['REDEEM', 'REDEMPTION'])].groupby('symbol')['usdcSize'].sum()
trades_by_sym  = cashflow_df[cashflow_df['type'] == 'TRADE'].groupby('symbol')['type'].count()
pnl_by_sym     = cashflow_df.groupby('symbol')['flow'].sum()

overall = pd.DataFrame({
    'trades':   trades_by_sym,
    'bought':   buys_by_sym,
    'sold':     sells_by_sym,
    'redeemed': redeem_by_sym,
    'net_pnl':  pnl_by_sym,
}).fillna(0).round(2)
overall['roi_%'] = (overall['net_pnl'] / overall['bought'].replace(0, float('nan')) * 100).round(1)

# === DAILY SUMMARY ===
buys_d    = cashflow_df[cashflow_df['side'] == 'BUY'].groupby(['date', 'symbol'])['usdcSize'].sum()
sells_d   = cashflow_df[cashflow_df['side'] == 'SELL'].groupby(['date', 'symbol'])['usdcSize'].sum()
redeems_d = cashflow_df[cashflow_df['type'].isin(['REDEEM', 'REDEMPTION'])].groupby(['date', 'symbol'])['usdcSize'].sum()

daily_summary = (
    cashflow_df.groupby(['date', 'symbol'])
    .agg(
        trades       = ('type', lambda x: (x == 'TRADE').sum()),
        redeems      = ('type', lambda x: x.isin(['REDEEM', 'REDEMPTION']).sum()),
        buys         = ('side', lambda x: (x == 'BUY').sum()),
        sells        = ('side', lambda x: (x == 'SELL').sum()),
        total_usdc   = ('usdcSize', 'sum'),
        pnl_cashflow = ('flow', 'sum'),
        markets      = ('conditionId', 'nunique'),
    )
    .round(2)
    .reset_index()
)
# Join pre-computed buy/sell/redeem totals cleanly
daily_summary = daily_summary.join(buys_d.rename('buy_usdc'),    on=['date', 'symbol'])
daily_summary = daily_summary.join(sells_d.rename('sell_usdc'),   on=['date', 'symbol'])
daily_summary = daily_summary.join(redeems_d.rename('redeem_usdc'), on=['date', 'symbol'])
daily_summary[['buy_usdc', 'sell_usdc', 'redeem_usdc']] = \
    daily_summary[['buy_usdc', 'sell_usdc', 'redeem_usdc']].fillna(0).round(2)
daily_summary['date'] = daily_summary['date'].astype(str)

# === WEEKLY PATTERNS ===
weekly = (
    cashflow_df.groupby(['weekday', 'symbol'])
    .agg(
        records      = ('timestamp', 'count'),
        total_usdc   = ('usdcSize', 'sum'),
        pnl_cashflow = ('flow', 'sum'),
    )
    .round(2)
    .reset_index()
)
weekly['weekday'] = pd.Categorical(weekly['weekday'], categories=DAY_ORDER, ordered=True)
weekly = weekly.sort_values(['weekday', 'symbol'])

# === POSITIONS ===
# Only include positions with currentValue > 0 (active/redeemable).
# Polymarket returns ALL historical positions — expired losing ones have currentValue=0
# and cashPnl = -cost, making unrealized PnL look massive when there's nothing open.
active_pos = positions_df[
    positions_df['symbol'].isin([s.upper() for s in SYMS]) &
    (positions_df['currentValue'] > 0)
]

pos_summary = (
    active_pos
    .groupby('symbol')
    .agg(
        count            = ('title', 'count'),
        total_size       = ('size', 'sum'),
        avg_price        = ('avgPrice', 'mean'),
        total_value      = ('currentValue', 'sum'),
        unrealized_pnl   = ('cashPnl', 'sum'),
        redeemable_count = ('redeemable', 'sum'),
    )
    .round(3)
    .reset_index()
)

# === DISPLAY ===
SEP  = "=" * 100
SEP2 = "-" * 100

total_pnl       = cashflow_df['flow'].sum().round(2)
total_bought    = cashflow_df[cashflow_df['side'] == 'BUY']['usdcSize'].sum().round(2)
total_sold      = cashflow_df[cashflow_df['side'] == 'SELL']['usdcSize'].sum().round(2)
total_redeemed  = cashflow_df[cashflow_df['type'].isin(['REDEEM', 'REDEMPTION'])]['usdcSize'].sum().round(2)
unrealized_pnl  = pos_summary['unrealized_pnl'].sum().round(2)

print(SEP)
print(f"REPORT  (from {START_DATE})")
print(SEP)

print("\n📊 OVERALL BY SYMBOL")
print(SEP2)
print(overall.to_string())

print(f"\n📅 DAILY SUMMARY (newest first)")
print(SEP2)
cols = ['date', 'symbol', 'trades', 'redeems', 'buys', 'sells',
        'buy_usdc', 'sell_usdc', 'redeem_usdc', 'pnl_cashflow']
print(daily_summary[cols].sort_values('date', ascending=False).to_string(index=False))

print(f"\n📆 WEEKLY PATTERNS")
print(SEP2)
print(weekly.to_string(index=False))

print(f"\n🗂 CURRENT POSITIONS (currentValue > 0 only)")
print(SEP2)
if pos_summary.empty:
    print("  No open positions.")
else:
    print(pos_summary.to_string(index=False))

print(f"\n{SEP}")
print("🎯 FINAL SUMMARY")
print(SEP)
print(f"  Period:          {START_DATE} → today")
print(f"  Realized PnL:    ${total_pnl:>10.2f}")
print(f"  Total bought:    ${total_bought:>10.2f}")
print(f"  Total sold:      ${total_sold:>10.2f}")
print(f"  Total redeemed:  ${total_redeemed:>10.2f}")
if not pos_summary.empty:
    print(f"  Unrealized PnL:  ${unrealized_pnl:>10.2f}")
    print(f"  Total PnL:       ${(total_pnl + unrealized_pnl):>10.2f}")
else:
    print(f"  Unrealized PnL:  $      0.00  (no open positions)")
    print(f"  Total PnL:       ${total_pnl:>10.2f}")
print(SEP2)

# === SAVE ===
out = Path('output')
out.mkdir(exist_ok=True)
cashflow_df.to_csv(out / 'cashflow_raw.csv', index=False)
daily_summary.to_csv(out / 'daily_summary.csv', index=False)
overall.to_csv(out / 'overall_summary.csv')
weekly.to_csv(out / 'weekly_patterns.csv', index=False)
pos_summary.to_csv(out / 'positions_summary.csv', index=False)
activity_df.to_csv(out / 'activity_raw.csv', index=False)
positions_df.to_csv(out / 'positions_raw.csv', index=False)
print("\n✅ Saved to ./output/")
