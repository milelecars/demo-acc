"""
Fix trade id=116 — TRXUSDT MANUAL_CLOSE Apr 23 02:30 UTC
=========================================================
1. Fetches userTrades from Binance around the emergency close
2. Finds the closing fill (SELL side, same timestamp)
3. Calculates real P&L
4. Updates Supabase row id=116
"""

import os, sys, hmac, hashlib, time, requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

API_KEY    = os.environ['BINANCE_API_KEY']
API_SECRET = os.environ['BINANCE_API_SECRET']
SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']

BASE_URL = 'https://demo-fapi.binance.com/fapi'  # testnet

# ── Known trade details ──────────────────────────────────────────────────────
TRADE_ID   = 116
SYMBOL     = 'TRXUSDT'
DIRECTION  = 'SHORT'
ENTRY      = 0.32909
LEVERAGE   = 25
MARGIN     = 20.0
QTY        = 1519.0

# Emergency close was at 2026-04-23 02:30 UTC — search ±5 min
OPEN_TS_UTC = datetime(2026, 4, 23, 2, 30, 0, tzinfo=timezone.utc)
START_MS    = int(OPEN_TS_UTC.timestamp() * 1000) - 5 * 60 * 1000   # -5 min
END_MS      = int(OPEN_TS_UTC.timestamp() * 1000) + 5 * 60 * 1000   # +5 min

# ── Helpers ──────────────────────────────────────────────────────────────────
def sign(params: dict) -> dict:
    params['timestamp'] = int(time.time() * 1000)
    qs  = '&'.join(f'{k}={v}' for k, v in params.items())
    sig = hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    params['signature'] = sig
    return params

def binance_get(path, params={}):
    p   = sign(dict(params))
    qs  = '&'.join(f'{k}={v}' for k, v in p.items())
    url = f'{BASE_URL}{path}?{qs}'
    r   = requests.get(url, headers={'X-MBX-APIKEY': API_KEY}, timeout=10)
    r.raise_for_status()
    return r.json()

def supabase_update(table, row_id, data):
    url  = f'{SUPABASE_URL}/rest/v1/{table}?id=eq.{row_id}'
    hdrs = {
        'apikey':        SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type':  'application/json',
        'Prefer':        'return=representation',
    }
    r = requests.patch(url, headers=hdrs, json=data, timeout=10)
    r.raise_for_status()
    return r.json()

# ── Step 1: Fetch userTrades ─────────────────────────────────────────────────
print(f'Fetching userTrades for {SYMBOL} around {OPEN_TS_UTC} ...')
trades = binance_get('/v1/userTrades', {
    'symbol':    SYMBOL,
    'startTime': START_MS,
    'endTime':   END_MS,
    'limit':     50,
})

if not trades:
    print('No trades found in that window.')
    sys.exit(1)

print(f'Found {len(trades)} trade(s):')
for t in trades:
    ts  = datetime.fromtimestamp(t['time'] / 1000, tz=timezone.utc)
    print(f"  id={t['id']}  side={t['side']}  qty={t['qty']}  "
          f"price={t['price']}  realizedPnl={t.get('realizedPnl')}  time={ts}")

# ── Step 2: Find the closing fill ────────────────────────────────────────────
# SHORT closed with BUY; the entry was also a SELL so look for BUY side
close_fills = [t for t in trades if t['side'] == 'BUY']

if not close_fills:
    # fallback: just take the last trade
    print('\nNo BUY fills found — using last trade as fallback.')
    close_fills = trades[-1:]

# Use the fill closest to qty=1519
best = min(close_fills, key=lambda t: abs(float(t['qty']) - QTY))
exit_price   = float(best['price'])
realized_pnl = float(best.get('realizedPnl', 0))
fee          = abs(float(best.get('commission', 0)))
close_ts     = datetime.fromtimestamp(best['time'] / 1000, tz=timezone.utc)

print(f'\nSelected closing fill:')
print(f'  price        = {exit_price}')
print(f'  realizedPnl  = {realized_pnl}')
print(f'  fee          = {fee}')
print(f'  time         = {close_ts}')

# ── Step 3: Calculate P&L ────────────────────────────────────────────────────
# SHORT: profit when price falls
# pnl_pct = (entry - exit) / entry * leverage * 100
pnl_pct  = round((ENTRY - exit_price) / ENTRY * LEVERAGE * 100, 4)
pnl_usdt = round(pnl_pct / 100 * MARGIN, 4)
outcome  = 'WIN' if pnl_usdt > 0 else 'LOSS'
close_time_str = close_ts.strftime('%Y-%m-%d %H:%M UTC')

print(f'\nCalculated:')
print(f'  pnl_pct   = {pnl_pct}%')
print(f'  pnl_usdt  = ${pnl_usdt}')
print(f'  outcome   = {outcome}')
print(f'  close_time= {close_time_str}')

# ── Step 4: Update Supabase ──────────────────────────────────────────────────
update_payload = {
    'outcome':    outcome,
    'exit_price': exit_price,
    'close_time': close_time_str,
    'pnl_pct':    pnl_pct,
    'pnl_usdt':   pnl_usdt,
}

print(f'\nUpdating Supabase trades id={TRADE_ID} ...')
result = supabase_update('trades', TRADE_ID, update_payload)
print(f'Updated: {result}')
print('\nDone.')