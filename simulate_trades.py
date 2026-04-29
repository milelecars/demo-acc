"""
simulate_trades.py — Replay backtest signals as if the live bot had been running
=================================================================================

Reads backtest_report.csv and produces trade rows in the same shape the live bot
writes to Supabase, applying the live bot's open-position gates so the result
matches what would actually have been recorded:

  1. Per-symbol lock — a new signal is skipped if that symbol still has an
     open position from a prior signal (step3_order_manager._handle_signal).
  2. S2 consecutive-loss pause — once two S2 trades have closed LOSS without
     an intervening WIN, subsequent S2 signals are skipped.

Run modes:
  python simulate_trades.py              # dry run: print summary + write
                                         #   simulated_trades.sql for review
  python simulate_trades.py --apply      # also POST rows to Supabase trades table
"""

import os
import sys
import csv
import argparse
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

CSV_PATH        = 'backtest_report.csv'
SQL_OUT_PATH    = 'simulated_trades.sql'

ENTRY_FEE_RATE  = 0.0005    # MARKET entry = taker
EXIT_FEE_TAKER  = 0.0005    # SL_MARKET   = taker
EXIT_FEE_MAKER  = 0.0002    # TP limit    = maker

DB_COLUMNS = [
    'open_time', 'close_time', 'symbol', 'strategy', 'direction',
    'signal_price', 'entry_price', 'sl_price', 'tp_price',
    'quantity', 'margin_usdt', 'leverage', 'outcome',
    'pnl_pct', 'pnl_usdt', 'fee_usdt', 'slippage_pct', 'signal_time',
]


def parse_time(s):
    return datetime.strptime(s.replace(' UTC', '').strip(), '%Y-%m-%d %H:%M')


def read_signals():
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def build_trade_row(r):
    """Map a backtest CSV row to the live trades-table schema."""
    entry    = float(r['entry'])
    notional = float(r['notional'])
    margin   = float(r['margin'])
    leverage = round(notional / margin) if margin else 0
    quantity = round(notional / entry, 8) if entry else 0
    outcome  = r['outcome']

    if outcome in ('WIN', 'LOSS'):
        pnl_pct    = float(r['pnl_pct'])
        pnl_usdt   = float(r['pnl_usdt'])
        exit_fee   = EXIT_FEE_MAKER if outcome == 'WIN' else EXIT_FEE_TAKER
        fee_usdt   = round(notional * (ENTRY_FEE_RATE + exit_fee), 4)
        close_time = r['exit_time']
    else:   # OPEN
        pnl_pct = pnl_usdt = fee_usdt = None
        close_time = None

    return {
        'open_time':    r['signal_time'],
        'close_time':   close_time,
        'symbol':       r['symbol'],
        'strategy':     r['strategy'],
        'direction':    r['direction'],
        'signal_price': round(entry, 8),    # backtest assumes zero slippage
        'entry_price':  round(entry, 8),
        'sl_price':     round(float(r['sl']), 8),
        'tp_price':     round(float(r['tp']), 8),
        'quantity':     quantity,
        'margin_usdt':  margin,
        'leverage':     leverage,
        'outcome':      outcome,
        'pnl_pct':      round(pnl_pct, 3) if pnl_pct is not None else None,
        'pnl_usdt':     round(pnl_usdt, 2) if pnl_usdt is not None else None,
        'fee_usdt':     fee_usdt,
        'slippage_pct': 0.0,
        'signal_time':  r['signal_time'],
    }


def simulate(rows):
    """
    Walk OPEN/CLOSE events in time order, evolving live-bot state:
      - open_symbols: which symbols currently have a position
      - s2_consec_losses: counter that gates S2 entries
    Returns (kept_rows, skipped) where skipped is [(row, reason), ...].
    """
    events = []
    for idx, r in enumerate(rows):
        sig_t = parse_time(r['signal_time'])
        events.append((sig_t, 0, idx, 'OPEN'))    # priority 0: opens before closes at same ts
        if r['outcome'] in ('WIN', 'LOSS') and r.get('exit_time'):
            cls_t = parse_time(r['exit_time'])
            events.append((cls_t, 1, idx, 'CLOSE'))

    events.sort()

    open_by_symbol   = {}    # symbol -> idx of open trade
    s2_consec_losses = 0
    kept             = set()
    skipped          = []

    for ts, prio, idx, kind in events:
        r = rows[idx]
        if kind == 'OPEN':
            sym = r['symbol']
            if sym in open_by_symbol:
                prior = rows[open_by_symbol[sym]]
                skipped.append((r, f"per-symbol lock (open since {prior['signal_time']})"))
                continue
            if r['strategy'].startswith('S2') and s2_consec_losses >= 2:
                skipped.append((r, f"S2 paused (consec_losses={s2_consec_losses})"))
                continue
            open_by_symbol[sym] = idx
            kept.add(idx)
        else:   # CLOSE
            if idx not in kept:
                continue
            open_by_symbol.pop(r['symbol'], None)
            if r['strategy'].startswith('S2'):
                if r['outcome'] == 'LOSS':
                    s2_consec_losses += 1
                elif r['outcome'] == 'WIN':
                    s2_consec_losses = 0

    kept_rows = [rows[i] for i in sorted(kept)]
    return kept_rows, skipped


def to_sql(trades):
    out = [
        "-- Simulated trades, generated from backtest_report.csv by simulate_trades.py",
        "-- Review before running. Each row matches the live bot's trades-table schema.",
        "",
    ]
    for t in trades:
        vals = []
        for c in DB_COLUMNS:
            v = t[c]
            if v is None:
                vals.append('NULL')
            elif isinstance(v, str):
                vals.append("'" + v.replace("'", "''") + "'")
            else:
                vals.append(str(v))
        out.append(f"INSERT INTO trades ({', '.join(DB_COLUMNS)}) VALUES ({', '.join(vals)});")
    return '\n'.join(out) + '\n'


def insert_to_supabase(trades):
    url = os.getenv('SUPABASE_URL', '').rstrip('/')
    key = os.getenv('SUPABASE_KEY', '')
    if not url or not key:
        print("ERROR: SUPABASE_URL / SUPABASE_KEY not set in .env. Aborting --apply.")
        sys.exit(1)

    headers = {
        'apikey':        key,
        'Authorization': f'Bearer {key}',
        'Content-Type':  'application/json',
        'Prefer':        'return=minimal',
    }
    inserted = 0
    failed   = 0
    for t in trades:
        try:
            resp = requests.post(f"{url}/rest/v1/trades",
                                 json=t, headers=headers, timeout=10)
            if resp.status_code in (200, 201, 204):
                inserted += 1
            else:
                failed += 1
                print(f"  FAIL {resp.status_code} on {t['symbol']} {t['signal_time']}: "
                      f"{resp.text[:200]}")
        except Exception as e:
            failed += 1
            print(f"  ERROR on {t['symbol']} {t['signal_time']}: {e}")
    return inserted, failed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true',
                    help='POST rows to Supabase trades table (default: dry run)')
    args = ap.parse_args()

    rows = read_signals()
    kept_rows, skipped = simulate(rows)
    trades = [build_trade_row(r) for r in kept_rows]

    n_total = len(rows)
    n_kept  = len(trades)
    n_open  = sum(1 for t in trades if t['outcome'] == 'OPEN')
    n_win   = sum(1 for t in trades if t['outcome'] == 'WIN')
    n_loss  = sum(1 for t in trades if t['outcome'] == 'LOSS')
    pnl_sum = sum(t['pnl_usdt'] for t in trades if t['pnl_usdt'] is not None)
    fee_sum = sum(t['fee_usdt'] for t in trades if t['fee_usdt'] is not None)

    print(f"Backtest signals : {n_total}")
    print(f"Skipped by gates : {len(skipped)}")
    for r, reason in skipped:
        print(f"  - {r['signal_time']}  {r['symbol']:10}  {r['strategy']:14}  "
              f"{r['direction']:5}  -> {reason}")
    print(f"To insert        : {n_kept}  ({n_win}W / {n_loss}L / {n_open} OPEN)")
    print(f"Gross P&L        : ${pnl_sum:+.2f}")
    print(f"Total fees       : ${fee_sum:.2f}")
    print(f"Net P&L          : ${pnl_sum - fee_sum:+.2f}")

    with open(SQL_OUT_PATH, 'w', encoding='utf-8') as f:
        f.write(to_sql(trades))
    print(f"\nSQL written to {SQL_OUT_PATH}  ({n_kept} INSERT statements)")

    if not args.apply:
        print("\nDry run. Re-run with --apply to POST these rows to Supabase.")
        return

    print(f"\nInserting {n_kept} rows into Supabase trades table...")
    inserted, failed = insert_to_supabase(trades)
    print(f"Done: {inserted} inserted, {failed} failed.")
    if failed:
        sys.exit(1)


if __name__ == '__main__':
    main()
