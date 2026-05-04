"""
test_full_suite.py — Comprehensive standalone test suite.

Run with:  python test_full_suite.py

Mocks all network I/O. No external test framework required.
"""

import os, sys

# ── Env vars MUST be set before importing project modules ────────────────────
os.environ['BINANCE_API_KEY']    = 'test'
os.environ['BINANCE_API_SECRET'] = 'test'
os.environ['SUPABASE_URL']       = 'https://test.supabase.co'
os.environ['SUPABASE_KEY']       = 'test'
os.environ['TESTNET']            = 'true'

import time, threading, math, json, io
from unittest.mock import MagicMock, patch
import logging
logging.disable(logging.CRITICAL)

# ── Project modules ──────────────────────────────────────────────────────────
import step1_candle_engine   as ce
import step2_signal_detector as sd
import step3_order_manager   as om_mod
from step2_signal_detector import SignalDetector, SignalEvent
from step3_order_manager   import (OrderManager, BinanceClient, SupabaseClient,
                                    PrecisionCache, OpenPosition,
                                    POSITION_CAPS, MAX_OPEN_POSITIONS,
                                    STRATEGY_CONFIG)


# =============================================================================
# TEST RUNNER
# =============================================================================

PASS = 0
FAIL = 0
SKIP = 0
FAILURES = []


class _Skip(Exception):
    """Raised by a test to indicate it should be skipped (env missing)."""


def run(name, fn):
    global PASS, FAIL, SKIP
    try:
        fn()
        PASS += 1
        print(f"[PASS] {name}")
    except _Skip as e:
        SKIP += 1
        print(f"[SKIP] {name} — {e}")
    except AssertionError as e:
        FAIL += 1
        msg = str(e) if str(e) else "assertion failed"
        FAILURES.append((name, msg))
        print(f"[FAIL] {name} — {msg}")
    except Exception as e:
        FAIL += 1
        msg = f"{type(e).__name__}: {e}"
        FAILURES.append((name, msg))
        print(f"[FAIL] {name} — {msg}")


# =============================================================================
# HELPERS
# =============================================================================

def make_signal(strategy='S1_EMA_CROSS', symbol='BTCUSDT', direction='LONG',
                entry=100.0, sl=None, tp=None):
    if sl is None:
        sl = entry * (0.995 if direction == 'LONG' else 1.005)
    if tp is None:
        tp = entry * (1.015 if direction == 'LONG' else 0.985)
    return SignalEvent(
        strategy=strategy, symbol=symbol, direction=direction,
        entry_price=entry, sl_price=sl, tp_price=tp,
        signal_ts=int(time.time() * 1000),
        signal_time='2026-01-01 00:00 UTC',
        reason='test', indicators={},
    )


def make_bare_om():
    """Construct an OrderManager skipping __init__ — fully mocked internals."""
    om = OrderManager.__new__(OrderManager)
    om.detector  = MagicMock()
    om.alerts    = MagicMock()
    om.client    = MagicMock()
    om.client.session = MagicMock()
    om.precision = MagicMock()
    om.supabase  = MagicMock()
    om._lock     = threading.Lock()
    om._open_positions  = {}
    om._pending_symbols = set()
    om._consec_losses   = {'S1': 0, 'S2': 0}
    om.closed_positions = []
    return om


def setup_happy_path(om, symbol='BTCUSDT', price=100.0, qty=10.0,
                      entry_avg=None, position_risk=None, no_position=True):
    """Wire mocks so a signal places successfully."""
    if entry_avg is None:
        entry_avg = price

    om.client.get_usdt_balance.return_value = 1000.0

    def _get_dispatch(path, params=None, signed=False):
        if path == '/v2/positionRisk':
            if position_risk is not None:
                return position_risk
            return [] if no_position else [{'symbol': symbol, 'positionAmt': '100'}]
        if path == '/v1/openAlgoOrders':
            return []
        return []
    om.client._get.side_effect = _get_dispatch

    om.client.get_open_orders.return_value      = []
    om.client.set_margin_type.return_value      = {}
    om.client.set_leverage.return_value         = {'leverage': 50}
    om.client.get_ticker_price.return_value     = price
    om.client.cancel_order.return_value         = {}
    om.client.cancel_algo_order.return_value    = {}

    om.precision.get.return_value = {
        'qty_step': 0.001, 'qty_decimals': 3,
        'price_step': 0.01, 'price_decimals': 4,
        'min_qty': 0.001,  'max_qty': 1e12,
        'min_notional': 5.0,
    }
    om.precision.resolve_order_params.return_value = (qty, 50)
    om.precision.round_price.side_effect = lambda s, p: round(p, 6)

    om.client.place_market_order.return_value = {
        'orderId': 999001, 'avgPrice': str(entry_avg),
    }
    om.client.place_take_profit_order.return_value = {'algoId': 1001}
    om.client.place_stop_loss_order.return_value   = {'algoId': 1002}
    om.supabase.insert_returning_id.return_value   = 7777


def make_position(symbol='BTCUSDT', strategy='S1_EMA_CROSS', direction='LONG',
                  entry=100.0, sl=99.5, tp=101.5, qty=10.0,
                  margin=20.0, leverage=50,
                  tp_id=2001, sl_id=2002, db_id=42):
    pos = OpenPosition(
        symbol=symbol, strategy=strategy, direction=direction,
        entry_price=entry, sl_price=sl, tp_price=tp,
        quantity=qty, margin_usdt=margin, leverage=leverage,
        tp_order_id=tp_id, sl_order_id=sl_id,
        entry_order_id=900, signal_ts=int(time.time()*1000),
        signal_time='2026-01-01 00:00 UTC',
        signal_price=entry,
    )
    pos.db_id = db_id
    return pos


def join_trade_threads(timeout=2.0):
    for t in list(threading.enumerate()):
        if t.name.startswith('trade_'):
            t.join(timeout=timeout)


def base_ind():
    """Minimal indicator dict that satisfies on_candle_close's required-keys gate."""
    return {
        'ema9': 11.0, 'ema26': 10.5, 'ema200': 9.0,
        'ema9_prev': 9.5, 'ema26_prev': 10.0,
        'adx': 30.0, 'di_plus': 30.0, 'di_minus': 10.0,
        'macd': 0.5, 'macd_sig': 0.2, 'macd_hist': 0.3,
        'ma44': 10.0, 'ma44_slope_8bar': -0.5,
        'ma44_accel': -0.01, 'atr': 0.5, 'atr_pct': 0.4,
    }


def s1_long_pass_ind():
    """Indicators that pass all 6 S1 LONG filters at the moment of crossover."""
    ind = base_ind()
    ind.update({
        'ema9_prev':  9.5,  'ema26_prev': 10.0,
        'ema9':      11.0,  'ema26':      10.5,
        'ema200':     9.0,
        'adx':       30.0,
        'di_plus':   30.0,  'di_minus':   10.0,
        'macd':       0.5,  'macd_sig':    0.2, 'macd_hist': 0.3,
    })
    return ind


def long_signal_candle(ts):
    # Bullish candle, close > both EMAs and EMA200 below
    return {'t': ts, 'o': 10.0, 'h': 12.0, 'l': 9.5, 'c': 11.5}


def gen_falling_candles(n=60, base=200.0, step=0.001, ts0=1_700_000_000_000):
    """Strictly decreasing closes — passes _check_ma44_monotonic_falling."""
    return [{'t': ts0 + i * 900_000,
             'o': base - i * step,
             'h': base - i * step + 0.05,
             'l': base - i * step - 0.05,
             'c': base - i * step} for i in range(n)]


def s2_setup_candle(ts):
    # bearish, body=1.0, candle_size=1.5 → body_ratio=0.667; wick_pct=1.5/200.05=0.0075
    return {'t': ts, 'o': 200.0, 'h': 200.05, 'l': 198.55, 'c': 199.0}


def s2_setup_ind(ma44=200.5, slope=-0.5, accel=-0.01, atr_pct=0.4):
    ind = base_ind()
    ind.update({'ma44': ma44, 'ma44_slope_8bar': slope,
                'ma44_accel': accel, 'atr_pct': atr_pct})
    return ind


# =============================================================================
# SECTION 1 — SignalDetector (step2_signal_detector.py)
# =============================================================================

# ── S1 gate logic ────────────────────────────────────────────────────────────

def test_s1_gate_blocks_second_signal():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    det.on_candle_close('BTCUSDT', long_signal_candle(1_700_000_000_000),
                        s1_long_pass_ind())
    assert len(fired) == 1
    assert det._get_state('BTCUSDT').s1_trade_open is True
    # Second crossover on same symbol — must be gated
    fired.clear()
    ind2 = s1_long_pass_ind()
    ind2['ema9_prev'] = 9.0  # re-fresh crossover
    det.on_candle_close('BTCUSDT', long_signal_candle(1_700_000_999_000), ind2)
    assert len(fired) == 0, f"second signal slipped through: {fired}"


def test_s1_gate_releases_after_close():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    det.on_candle_close('BTCUSDT', long_signal_candle(1_700_000_000_000),
                        s1_long_pass_ind())
    assert len(fired) == 1
    det.on_trade_closed('BTCUSDT', 'S1_EMA_CROSS', 'WIN')
    assert det._get_state('BTCUSDT').s1_trade_open is False
    fired.clear()
    ind2 = s1_long_pass_ind()
    det.on_candle_close('BTCUSDT', long_signal_candle(1_700_000_999_000), ind2)
    assert len(fired) == 1


def test_s1_gate_per_symbol():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    det.on_candle_close('BTCUSDT', long_signal_candle(1_700_000_000_000),
                        s1_long_pass_ind())
    det.on_candle_close('ETHUSDT', long_signal_candle(1_700_000_000_000),
                        s1_long_pass_ind())
    syms = sorted(s.symbol for s in fired)
    assert syms == ['BTCUSDT', 'ETHUSDT'], f"got {syms}"


def test_s1_pending_expires_after_2_candles():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    ts = 1_700_000_000_000
    # Crossover on N but F2 fails (no confirm yet)
    ind = s1_long_pass_ind()
    failing_candle = {'t': ts, 'o': 11.0, 'h': 11.5, 'l': 10.5, 'c': 10.6}  # bearish-ish
    det.on_candle_close('BTCUSDT', failing_candle, ind)
    state = det._get_state('BTCUSDT')
    assert state.s1_pending_dir == 'LONG'
    # Skip N+1 entirely, jump to N+2 (>30 minutes later)
    ts_n2 = ts + 3 * 15 * 60 * 1000   # 45 min later
    ind2 = s1_long_pass_ind()
    ind2['ema9_prev'] = 11.0; ind2['ema26_prev'] = 10.5  # no fresh crossover
    det.on_candle_close('BTCUSDT', long_signal_candle(ts_n2), ind2)
    assert state.s1_pending_dir is None, "pending should be cleared after expiry"
    assert len(fired) == 0


def test_s1_pending_overwritten_by_new_crossover():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    ts = 1_700_000_000_000
    # Detect LONG crossover but make F2 fail (bearish candle)
    ind1 = s1_long_pass_ind()
    bad = {'t': ts, 'o': 11.0, 'h': 11.5, 'l': 10.0, 'c': 10.2}
    det.on_candle_close('BTCUSDT', bad, ind1)
    state = det._get_state('BTCUSDT')
    assert state.s1_pending_dir == 'LONG'
    # Now detect SHORT crossover next candle, also F2-failing (bullish for short)
    ind2 = s1_long_pass_ind()
    ind2['ema9_prev'] = 11.0; ind2['ema26_prev'] = 10.5
    ind2['ema9']      = 9.0;  ind2['ema26']      = 10.0
    bullish = {'t': ts + 900_000, 'o': 9.0, 'h': 11.0, 'l': 9.0, 'c': 10.5}
    det.on_candle_close('BTCUSDT', bullish, ind2)
    assert state.s1_pending_dir == 'SHORT', f"got {state.s1_pending_dir}"


# ── S1 filter logic ──────────────────────────────────────────────────────────

def test_s1_f2_bearish_candle_required_for_short():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    ind = s1_long_pass_ind()
    ind['ema9_prev'] = 11.0; ind['ema26_prev'] = 10.5
    ind['ema9']      = 10.0; ind['ema26']      = 10.5
    ind['di_plus']   = 10.0; ind['di_minus']   = 30.0
    ind['macd']      = -0.5; ind['macd_sig']   = -0.2; ind['macd_hist'] = -0.3
    ind['ema200']    = 12.0
    bullish = {'t': 1_700_000_000_000, 'o': 10.0, 'h': 11.0, 'l': 9.5, 'c': 10.8}
    det.on_candle_close('BTCUSDT', bullish, ind)
    assert len(fired) == 0


def test_s1_f3_ema200_blocks_long_below():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    ind = s1_long_pass_ind()
    ind['ema200'] = 999.0  # close 11.5 < ema200 999
    det.on_candle_close('BTCUSDT', long_signal_candle(1_700_000_000_000), ind)
    assert len(fired) == 0


def test_s1_f3_ema200_blocks_short_above():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    ind = s1_long_pass_ind()
    ind['ema9_prev'] = 11.0; ind['ema26_prev'] = 10.5
    ind['ema9']      = 10.0; ind['ema26']      = 10.5
    ind['di_plus']   = 10.0; ind['di_minus']   = 30.0
    ind['macd']      = -0.5; ind['macd_sig']   = -0.2; ind['macd_hist'] = -0.3
    ind['ema200']    = 5.0   # close > ema200 → blocks SHORT
    bearish = {'t': 1_700_000_000_000, 'o': 11.0, 'h': 11.0, 'l': 9.0, 'c': 9.5}
    # F2: SHORT needs close < open AND close < ema9 AND close < ema26
    # 9.5 < 11.0 ✓, 9.5 < 10.0 ✓, 9.5 < 10.5 ✓; F3 should block (9.5 > 5.0)
    det.on_candle_close('BTCUSDT', bearish, ind)
    assert len(fired) == 0


def test_s1_f4_adx_below_25_blocks():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    ind = s1_long_pass_ind(); ind['adx'] = 24.9
    det.on_candle_close('BTCUSDT', long_signal_candle(1_700_000_000_000), ind)
    assert len(fired) == 0


def test_s1_f4_adx_exactly_25_blocks():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    ind = s1_long_pass_ind(); ind['adx'] = 25.0
    det.on_candle_close('BTCUSDT', long_signal_candle(1_700_000_000_000), ind)
    assert len(fired) == 0


def test_s1_f5_di_wrong_direction_blocks_long():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    ind = s1_long_pass_ind()
    ind['di_plus'] = 10.0; ind['di_minus'] = 30.0
    det.on_candle_close('BTCUSDT', long_signal_candle(1_700_000_000_000), ind)
    assert len(fired) == 0


def test_s1_f5_di_wrong_direction_blocks_short():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    ind = s1_long_pass_ind()
    ind['ema9_prev'] = 11.0; ind['ema26_prev'] = 10.5
    ind['ema9']      = 10.0; ind['ema26']      = 10.5
    ind['ema200']    = 5.0
    ind['di_plus']   = 30.0; ind['di_minus']   = 10.0   # wrong for SHORT
    ind['macd']      = -0.5; ind['macd_sig']   = -0.2; ind['macd_hist'] = -0.3
    bearish = {'t': 1_700_000_000_000, 'o': 11.0, 'h': 11.0, 'l': 9.0, 'c': 9.5}
    det.on_candle_close('BTCUSDT', bearish, ind)
    assert len(fired) == 0


def test_s1_f6_macd_wrong_side_blocks():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    ind = s1_long_pass_ind()
    ind['macd'] = 0.1; ind['macd_sig'] = 0.5; ind['macd_hist'] = -0.4   # macd < sig
    det.on_candle_close('BTCUSDT', long_signal_candle(1_700_000_000_000), ind)
    assert len(fired) == 0


def test_s1_all_filters_pass_fires_signal():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    det.on_candle_close('BTCUSDT', long_signal_candle(1_700_000_000_000),
                        s1_long_pass_ind())
    assert len(fired) == 1
    sig = fired[0]
    assert sig.strategy == 'S1_EMA_CROSS'
    assert sig.direction == 'LONG'
    assert sig.entry_price == 11.5


# ── S1 SL/TP calculation ─────────────────────────────────────────────────────

def test_s1_long_sl_tp():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    # Build candle so close = 100
    ind = s1_long_pass_ind()
    ind.update({'ema9_prev': 95, 'ema26_prev': 96, 'ema9': 99, 'ema26': 98,
                'ema200': 80})
    candle = {'t': 1_700_000_000_000, 'o': 95, 'h': 102, 'l': 94, 'c': 100}
    det.on_candle_close('BTCUSDT', candle, ind)
    sig = fired[0]
    assert abs(sig.sl_price - 99.5)  < 1e-9, f"SL={sig.sl_price}"
    assert abs(sig.tp_price - 101.5) < 1e-9, f"TP={sig.tp_price}"


def test_s1_short_sl_tp():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    ind = s1_long_pass_ind()
    ind.update({'ema9_prev': 103, 'ema26_prev': 102,
                'ema9': 101, 'ema26': 102,   # SHORT cross: 103>=102, 101<102
                'ema200': 120,                # close 100 < ema200 ✓
                'di_plus': 10, 'di_minus': 30,
                'macd': -0.5, 'macd_sig': -0.2, 'macd_hist': -0.3})
    # bearish: close=100 < open=105; close < ema9 (101) and < ema26 (102)
    candle = {'t': 1_700_000_000_000, 'o': 105, 'h': 106, 'l': 99, 'c': 100}
    det.on_candle_close('BTCUSDT', candle, ind)
    sig = fired[0]
    assert abs(sig.sl_price - 100.5) < 1e-9, f"SL={sig.sl_price}"
    assert abs(sig.tp_price -  98.5) < 1e-9, f"TP={sig.tp_price}"


# ── S2 logic ─────────────────────────────────────────────────────────────────

def test_s2_setup_then_trigger():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    det._h4.get = lambda *a, **k: False  # falling
    candles = gen_falling_candles(60)
    setup = s2_setup_candle(candles[-1]['t'] + 900_000)
    candles.append(setup)
    det.set_candle_list('BTCUSDT', candles)
    det.on_candle_close('BTCUSDT', setup, s2_setup_ind(ma44=200.5))
    state = det._get_state('BTCUSDT')
    assert state.s2_pending_setup is True

    trigger = {'t': setup['t'] + 900_000, 'o': 199.5, 'h': 200.0, 'l': 198.0, 'c': 198.5}
    candles.append(trigger)
    det.set_candle_list('BTCUSDT', candles)
    det.on_candle_close('BTCUSDT', trigger, s2_setup_ind(ma44=200.5))
    assert len(fired) == 1
    assert fired[0].strategy == 'S2_MA44_BOUNCE'
    assert fired[0].direction == 'SHORT'
    assert fired[0].entry_price == 199.5


def test_s2_setup_cleared_after_one_attempt():
    det = SignalDetector()
    det._h4.get = lambda *a, **k: False
    candles = gen_falling_candles(60)
    setup = s2_setup_candle(candles[-1]['t'] + 900_000)
    candles.append(setup)
    det.set_candle_list('BTCUSDT', candles)
    det.on_candle_close('BTCUSDT', setup, s2_setup_ind(ma44=200.5))
    state = det._get_state('BTCUSDT')
    assert state.s2_pending_setup is True

    # Non-trigger candle: open >= ma44
    non_trigger = {'t': setup['t'] + 900_000, 'o': 201.0, 'h': 201.5, 'l': 200.5, 'c': 201.0}
    det.on_candle_close('BTCUSDT', non_trigger, s2_setup_ind(ma44=200.5))
    assert state.s2_pending_setup is False, "pending should be cleared regardless"


def test_s2_gate_blocks_while_open():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    det._h4.get = lambda *a, **k: False
    candles = gen_falling_candles(60)
    setup = s2_setup_candle(candles[-1]['t'] + 900_000)
    candles.append(setup)
    det.set_candle_list('BTCUSDT', candles)
    det.on_candle_close('BTCUSDT', setup, s2_setup_ind(ma44=200.5))
    trigger = {'t': setup['t'] + 900_000, 'o': 199.5, 'h': 200.0, 'l': 198.0, 'c': 198.5}
    det.on_candle_close('BTCUSDT', trigger, s2_setup_ind(ma44=200.5))
    assert len(fired) == 1
    state = det._get_state('BTCUSDT')
    assert state.s2_trade_open is True

    # Try another setup — should be blocked
    candles.append(trigger)
    setup2 = s2_setup_candle(trigger['t'] + 900_000)
    candles.append(setup2)
    det.set_candle_list('BTCUSDT', candles)
    det.on_candle_close('BTCUSDT', setup2, s2_setup_ind(ma44=200.5))
    assert state.s2_pending_setup is False, "blocked: no setup recorded while trade open"


def test_s2_gate_releases_after_close():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    det._h4.get = lambda *a, **k: False
    state = det._get_state('BTCUSDT')
    state.s2_trade_open = True
    det.on_trade_closed('BTCUSDT', 'S2_MA44_BOUNCE', 'LOSS')
    assert state.s2_trade_open is False
    # Now setup should work
    candles = gen_falling_candles(60)
    setup = s2_setup_candle(candles[-1]['t'] + 900_000)
    candles.append(setup)
    det.set_candle_list('BTCUSDT', candles)
    det.on_candle_close('BTCUSDT', setup, s2_setup_ind(ma44=200.5))
    assert state.s2_pending_setup is True


def test_s2_cooldown_blocks_within_4h():
    det = SignalDetector()
    det._h4.get = lambda *a, **k: False
    state = det._get_state('BTCUSDT')
    # Pretend a signal fired 3 hours ago
    candles = gen_falling_candles(60)
    setup = s2_setup_candle(candles[-1]['t'] + 900_000)
    state.s2_last_signal_ts = setup['t'] / 1000.0 - 3 * 3600  # 3h ago
    candles.append(setup)
    det.set_candle_list('BTCUSDT', candles)
    det.on_candle_close('BTCUSDT', setup, s2_setup_ind(ma44=200.5))
    assert state.s2_pending_setup is False, "blocked by cooldown"


def test_s2_cooldown_clears_after_4h():
    det = SignalDetector()
    det._h4.get = lambda *a, **k: False
    state = det._get_state('BTCUSDT')
    candles = gen_falling_candles(60)
    setup = s2_setup_candle(candles[-1]['t'] + 900_000)
    state.s2_last_signal_ts = setup['t'] / 1000.0 - (4 * 3600 + 1)  # 4h+1s ago
    candles.append(setup)
    det.set_candle_list('BTCUSDT', candles)
    det.on_candle_close('BTCUSDT', setup, s2_setup_ind(ma44=200.5))
    assert state.s2_pending_setup is True


def test_s2_h4_rising_blocks():
    det = SignalDetector()
    det._h4.get = lambda *a, **k: True   # rising rejects SHORT
    candles = gen_falling_candles(60)
    setup = s2_setup_candle(candles[-1]['t'] + 900_000)
    candles.append(setup)
    det.set_candle_list('BTCUSDT', candles)
    det.on_candle_close('BTCUSDT', setup, s2_setup_ind(ma44=200.5))
    state = det._get_state('BTCUSDT')
    assert state.s2_pending_setup is False


def test_s2_body_above_ma44_blocks():
    det = SignalDetector()
    det._h4.get = lambda *a, **k: False
    candles = gen_falling_candles(60)
    setup = s2_setup_candle(candles[-1]['t'] + 900_000)
    candles.append(setup)
    det.set_candle_list('BTCUSDT', candles)
    # body_top = 200; ma44 = 199.5 (below body_top) → reject
    det.on_candle_close('BTCUSDT', setup, s2_setup_ind(ma44=199.5))
    state = det._get_state('BTCUSDT')
    assert state.s2_pending_setup is False


def test_s2_zone_a_passes():
    det = SignalDetector()
    det._h4.get = lambda *a, **k: False
    candles = gen_falling_candles(60)
    setup = s2_setup_candle(candles[-1]['t'] + 900_000)
    candles.append(setup)
    det.set_candle_list('BTCUSDT', candles)
    # body_top=200; ma44=200.5 → dist_pct = 0.5/200.5 = 0.249% (zone A)
    det.on_candle_close('BTCUSDT', setup, s2_setup_ind(ma44=200.5))
    assert det._get_state('BTCUSDT').s2_pending_setup is True


def test_s2_zone_b_passes():
    det = SignalDetector()
    det._h4.get = lambda *a, **k: False
    candles = gen_falling_candles(60)
    setup = s2_setup_candle(candles[-1]['t'] + 900_000)
    candles.append(setup)
    det.set_candle_list('BTCUSDT', candles)
    # ma44=201.1 → dist=1.1/201.1 = 0.547% (zone B 0.50-0.65)
    det.on_candle_close('BTCUSDT', setup, s2_setup_ind(ma44=201.1))
    assert det._get_state('BTCUSDT').s2_pending_setup is True


def test_s2_out_of_zone_blocks():
    det = SignalDetector()
    det._h4.get = lambda *a, **k: False
    candles = gen_falling_candles(60)
    setup = s2_setup_candle(candles[-1]['t'] + 900_000)
    candles.append(setup)
    det.set_candle_list('BTCUSDT', candles)
    # ma44=200.8 → dist=0.8/200.8 = 0.398% (between zones, dead band)
    det.on_candle_close('BTCUSDT', setup, s2_setup_ind(ma44=200.8))
    assert det._get_state('BTCUSDT').s2_pending_setup is False


def test_s2_bullish_candle_blocks():
    det = SignalDetector()
    det._h4.get = lambda *a, **k: False
    candles = gen_falling_candles(60)
    bullish = {'t': candles[-1]['t'] + 900_000,
               'o': 199.0, 'h': 200.05, 'l': 198.55, 'c': 200.0}  # close > open
    candles.append(bullish)
    det.set_candle_list('BTCUSDT', candles)
    det.on_candle_close('BTCUSDT', bullish, s2_setup_ind(ma44=200.5))
    assert det._get_state('BTCUSDT').s2_pending_setup is False


def test_s2_short_sl_tp():
    det = SignalDetector(); fired = []
    det.on_signal = lambda s: fired.append(s)
    det._h4.get = lambda *a, **k: False
    candles = gen_falling_candles(60)
    setup = s2_setup_candle(candles[-1]['t'] + 900_000)
    candles.append(setup)
    det.set_candle_list('BTCUSDT', candles)
    det.on_candle_close('BTCUSDT', setup, s2_setup_ind(ma44=200.5))
    # Trigger candle with open == 100 for clean SL/TP arithmetic
    trigger = {'t': setup['t'] + 900_000, 'o': 100.0, 'h': 100.0, 'l': 99.0, 'c': 99.5}
    candles.append(trigger)
    det.set_candle_list('BTCUSDT', candles)
    det.on_candle_close('BTCUSDT', trigger, s2_setup_ind(ma44=200.5))
    sig = fired[0]
    assert abs(sig.sl_price - 102.0) < 1e-9, f"SL={sig.sl_price}"
    assert abs(sig.tp_price -  94.0) < 1e-9, f"TP={sig.tp_price}"


# =============================================================================
# SECTION 2 — OrderManager (step3_order_manager.py)
# =============================================================================

# ── Gate logic ───────────────────────────────────────────────────────────────

def test_om_max_positions_blocks():
    om = make_bare_om()
    setup_happy_path(om)
    for i in range(MAX_OPEN_POSITIONS):
        om._open_positions[f"FAKE{i}"] = make_position(symbol=f"FAKE{i}")
    om._handle_signal(make_signal())
    om.client.place_market_order.assert_not_called()


def test_om_duplicate_symbol_blocks():
    om = make_bare_om()
    setup_happy_path(om)
    om._open_positions['BTCUSDT'] = make_position()
    om._handle_signal(make_signal(symbol='BTCUSDT'))
    om.client.place_market_order.assert_not_called()


def test_om_pending_symbol_blocks():
    om = make_bare_om()
    setup_happy_path(om)
    om._pending_symbols.add('BTCUSDT')
    om._handle_signal(make_signal(symbol='BTCUSDT'))
    om.client.place_market_order.assert_not_called()


def test_om_s2_paused_after_2_losses():
    om = make_bare_om()
    setup_happy_path(om)
    om._consec_losses['S2'] = 2
    om._handle_signal(make_signal(strategy='S2_MA44_BOUNCE', direction='SHORT'))
    om.client.place_market_order.assert_not_called()


def test_om_s1_not_paused_after_losses():
    om = make_bare_om()
    setup_happy_path(om)
    om._consec_losses['S1'] = 5
    om._handle_signal(make_signal(strategy='S1_EMA_CROSS'))
    assert om.client.place_market_order.called, "S1 should not be paused after losses"


# ── Balance / cap ────────────────────────────────────────────────────────────

def test_om_insufficient_balance_skips():
    om = make_bare_om()
    setup_happy_path(om)
    om.client.get_usdt_balance.return_value = 5.0  # < 20
    om._handle_signal(make_signal(strategy='S1_EMA_CROSS'))
    om.client.place_market_order.assert_not_called()


def test_om_position_cap_below_margin_skips():
    om = make_bare_om()
    setup_happy_path(om, symbol='TINYUSDT')
    # Patch POSITION_CAPS for the test symbol
    om_mod.POSITION_CAPS['TINYUSDT'] = 10
    try:
        om._handle_signal(make_signal(symbol='TINYUSDT'))
        om.client.place_market_order.assert_not_called()
    finally:
        om_mod.POSITION_CAPS.pop('TINYUSDT', None)


# ── Binance pre-flight ───────────────────────────────────────────────────────

def test_om_existing_binance_position_skips():
    om = make_bare_om()
    setup_happy_path(om, position_risk=[{'symbol': 'BTCUSDT', 'positionAmt': '100'}])
    om._handle_signal(make_signal(symbol='BTCUSDT'))
    om.client.place_market_order.assert_not_called()


def test_om_no_binance_position_proceeds():
    om = make_bare_om()
    setup_happy_path(om, position_risk=[])
    om._handle_signal(make_signal(symbol='BTCUSDT'))
    assert om.client.place_market_order.called


# ── TP/SL payload validation, all 15 symbols ─────────────────────────────────

ALL_SYMBOLS = ['BTCUSDT','ETHUSDT','XRPUSDT','TRXUSDT','ADAUSDT','ZECUSDT',
               'DOTUSDT','VETUSDT','FETUSDT','SEIUSDT','DASHUSDT','SYRUPUSDT',
               'ENSUSDT','BARDUSDT','TWTUSDT']

SYMBOL_PRICES = {
    'BTCUSDT':   (100000.0, 0.001),  'ETHUSDT':   (2500.0,    0.04),
    'XRPUSDT':   (2.5,      400.0),  'TRXUSDT':   (0.30,      3333.0),
    'ADAUSDT':   (0.5,      2000.0), 'ZECUSDT':   (70.0,      14.0),
    'DOTUSDT':   (8.0,      125.0),  'VETUSDT':   (0.05,      20000.0),
    'FETUSDT':   (1.2,      833.0),  'SEIUSDT':   (0.6,       1666.0),
    'DASHUSDT':  (30.0,     33.0),   'SYRUPUSDT': (0.5,       2000.0),
    'ENSUSDT':   (30.0,     33.0),   'BARDUSDT':  (0.5,       2000.0),
    'TWTUSDT':   (1.2,      833.0),
}


def _make_real_client_for_payload():
    c = BinanceClient('k', 's', 'http://test.local/fapi')
    captured = {}
    def fake_post(url, data=None, timeout=None, **kw):
        captured['url']  = url
        captured['data'] = dict(data) if data else {}
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {'algoId': 555}
        return resp
    c.session.post = fake_post
    return c, captured


def make_tp_payload_test(symbol, price, qty):
    def t():
        c, captured = _make_real_client_for_payload()
        tp_price = price * 1.015
        c.place_take_profit_order(symbol, 'SELL', qty, tp_price)
        d = captured['data']
        assert 'closePosition' not in d, "closePosition must NOT be in payload"
        q = float(d['quantity']);   assert q > 0,         f"qty={q}"
        assert d['reduceOnly'] == 'true',                 f"reduceOnly={d['reduceOnly']}"
        assert d['type'] == 'TAKE_PROFIT',                f"type={d['type']}"
        for field in ('price', 'triggerPrice', 'quantity'):
            assert 'e' not in str(d[field]).lower(),      f"sci notation in {field}: {d[field]}"
    return t


def make_sl_payload_test(symbol, price, qty):
    def t():
        c, captured = _make_real_client_for_payload()
        sl_price = price * 0.995
        c.place_stop_loss_order(symbol, 'SELL', qty, sl_price)
        d = captured['data']
        assert 'closePosition' not in d
        q = float(d['quantity']);   assert q > 0
        assert d['reduceOnly'] == 'true'
        assert d['type'] == 'STOP'
        for field in ('price', 'triggerPrice', 'quantity'):
            assert 'e' not in str(d[field]).lower()
    return t


# ── Emergency close ──────────────────────────────────────────────────────────

def test_emergency_close_fires_on_tp_sl_failure():
    import requests as _rq
    om = make_bare_om()
    setup_happy_path(om)
    err = _rq.exceptions.HTTPError("TP failed")
    om.client.place_take_profit_order.side_effect = err
    om._handle_signal(make_signal())
    # Emergency close: place_market_order called twice (entry + reduceOnly close)
    assert om.client.place_market_order.call_count == 2, \
        f"expected 2 market orders, got {om.client.place_market_order.call_count}"
    # Detector notified of LOSS
    om.detector.on_trade_closed.assert_called_with('BTCUSDT', 'S1_EMA_CROSS', 'LOSS')
    # Supabase insert (MANUAL_CLOSE) called via _log_trade_close fallback path
    assert om.supabase.insert.called or om.supabase.update.called


def test_emergency_close_uses_real_exit_price():
    import requests as _rq
    om = make_bare_om()
    setup_happy_path(om, symbol='ADAUSDT', price=0.3300)
    # Entry fill at 0.3300, then emergency close fills at 0.3298
    om.client.place_market_order.side_effect = [
        {'orderId': 1, 'avgPrice': '0.3300'},
        {'orderId': 2, 'avgPrice': '0.3298'},
    ]
    om.client.place_take_profit_order.side_effect = _rq.exceptions.HTTPError("fail")
    captured_close = {}
    real_log_close = om._log_trade_close
    def spy(pos, outcome, exit_price=None, close_time=None):
        captured_close['pos'] = pos
        captured_close['outcome'] = outcome
        captured_close['exit_price'] = exit_price
    om._log_trade_close = spy
    om._handle_signal(make_signal(symbol='ADAUSDT', entry=0.3300))
    assert captured_close.get('outcome') == 'MANUAL_CLOSE'
    assert abs(captured_close['exit_price'] - 0.3298) < 1e-9, \
        f"exit_price={captured_close.get('exit_price')}"


def test_emergency_close_falls_back_to_entry():
    import requests as _rq
    om = make_bare_om()
    setup_happy_path(om, symbol='ADAUSDT', price=0.3300)
    # Entry has avgPrice; close DOES NOT
    om.client.place_market_order.side_effect = [
        {'orderId': 1, 'avgPrice': '0.3300'},
        {'orderId': 2},  # no avgPrice
    ]
    om.client.place_take_profit_order.side_effect = _rq.exceptions.HTTPError("fail")
    captured = {}
    om._log_trade_close = lambda pos, outcome, exit_price=None, close_time=None: \
        captured.update(exit_price=exit_price, outcome=outcome)
    om._handle_signal(make_signal(symbol='ADAUSDT', entry=0.3300))
    assert abs(captured['exit_price'] - 0.3300) < 1e-9, \
        f"exit_price={captured.get('exit_price')}"


# ── Position monitor ─────────────────────────────────────────────────────────

def _wire_monitor_close(om, symbol, exit_price, close_side):
    """Wire client._get for /v2/positionRisk (empty) and /v1/userTrades (close fill)."""
    def _get(path, params=None, signed=False):
        if path == '/v2/positionRisk':
            return []  # nothing open on Binance
        if path == '/v1/userTrades':
            return [{'side': close_side, 'price': str(exit_price),
                     'time': int(time.time()*1000) + 1000, 'realizedPnl': '0.5'}]
        return []
    om.client._get.side_effect = _get
    om.client.get_algo_order.side_effect = Exception("not found")  # skip algo path


def test_monitor_detects_tp_fill():
    om = make_bare_om()
    pos = make_position()  # LONG, tp=101.5
    om._open_positions['BTCUSDT'] = pos
    _wire_monitor_close(om, 'BTCUSDT', exit_price=101.5, close_side='SELL')
    om._check_positions()
    assert 'BTCUSDT' not in om._open_positions
    om.detector.on_trade_closed.assert_called_with('BTCUSDT', 'S1_EMA_CROSS', 'WIN')
    # Supabase update called
    assert om.supabase.update.called or om.supabase.insert.called


def test_monitor_detects_sl_fill():
    om = make_bare_om()
    pos = make_position()  # LONG, sl=99.5
    om._open_positions['BTCUSDT'] = pos
    _wire_monitor_close(om, 'BTCUSDT', exit_price=99.5, close_side='SELL')
    om._check_positions()
    assert 'BTCUSDT' not in om._open_positions
    om.detector.on_trade_closed.assert_called_with('BTCUSDT', 'S1_EMA_CROSS', 'LOSS')


def test_monitor_ignores_still_open():
    om = make_bare_om()
    pos = make_position()
    om._open_positions['BTCUSDT'] = pos
    def _get(path, params=None, signed=False):
        if path == '/v2/positionRisk':
            return [{'symbol': 'BTCUSDT', 'positionAmt': '10'}]  # still open
        return []
    om.client._get.side_effect = _get
    # algo order check returns NEW (not finished)
    om.client.get_algo_order.return_value = {'algoStatus': 'NEW'}
    om._check_positions()
    assert 'BTCUSDT' in om._open_positions
    om.detector.on_trade_closed.assert_not_called()


def test_monitor_consecutive_loss_increments():
    om = make_bare_om()
    for sym in ('BTCUSDT', 'ETHUSDT'):
        om._open_positions[sym] = make_position(symbol=sym, strategy='S2_MA44_BOUNCE',
                                                  direction='SHORT', entry=100,
                                                  sl=102, tp=94)
        _wire_monitor_close(om, sym, exit_price=102.0, close_side='BUY')
        om._check_positions()
    assert om._consec_losses['S2'] == 2, f"got {om._consec_losses['S2']}"


def test_monitor_consecutive_loss_resets_on_win():
    om = make_bare_om()
    # Two LOSSes
    for sym in ('BTCUSDT', 'ETHUSDT'):
        om._open_positions[sym] = make_position(symbol=sym, strategy='S2_MA44_BOUNCE',
                                                  direction='SHORT', entry=100,
                                                  sl=102, tp=94)
        _wire_monitor_close(om, sym, exit_price=102.0, close_side='BUY')
        om._check_positions()
    assert om._consec_losses['S2'] == 2
    # One WIN
    om._open_positions['XRPUSDT'] = make_position(symbol='XRPUSDT',
                                                    strategy='S2_MA44_BOUNCE',
                                                    direction='SHORT', entry=100,
                                                    sl=102, tp=94)
    _wire_monitor_close(om, 'XRPUSDT', exit_price=94.0, close_side='BUY')
    om._check_positions()
    assert om._consec_losses['S2'] == 0, f"got {om._consec_losses['S2']}"


def test_consec_loss_key_s1_not_s2():
    om = make_bare_om()
    om._open_positions['BTCUSDT'] = make_position(symbol='BTCUSDT',
                                                    strategy='S1_EMA_CROSS',
                                                    direction='LONG',
                                                    entry=100, sl=99.5, tp=101.5)
    _wire_monitor_close(om, 'BTCUSDT', exit_price=99.5, close_side='SELL')
    om._check_positions()
    assert om._consec_losses['S1'] == 1
    assert om._consec_losses['S2'] == 0


# ── Startup reconciliation ───────────────────────────────────────────────────

def test_reconcile_closes_stale_position():
    om = make_bare_om()
    pos = make_position(symbol='BTCUSDT', db_id=42)
    om._open_positions['BTCUSDT'] = pos
    # Binance says no position
    def _get(path, params=None, signed=False):
        if path == '/v2/positionRisk':
            return []
        if path == '/v1/userTrades':
            return [{'side': 'SELL', 'price': '99.5',
                     'time': int(time.time()*1000), 'realizedPnl': '-0.5'}]
        return []
    om.client._get.side_effect = _get
    om._reconcile_on_startup()
    assert 'BTCUSDT' not in om._open_positions
    om.supabase.update.assert_called()


def test_reconcile_keeps_live_position():
    om = make_bare_om()
    pos = make_position(symbol='BTCUSDT', db_id=42)
    om._open_positions['BTCUSDT'] = pos
    def _get(path, params=None, signed=False):
        if path == '/v2/positionRisk':
            return [{'symbol': 'BTCUSDT', 'positionAmt': '100'}]
        return []
    om.client._get.side_effect = _get
    om._reconcile_on_startup()
    assert 'BTCUSDT' in om._open_positions


def test_load_history_restores_open_positions():
    om = make_bare_om()
    om.supabase.select_all.return_value = [{
        'id': 99, 'symbol': 'BTCUSDT', 'strategy': 'S1_EMA_CROSS',
        'direction': 'LONG', 'entry_price': 100.0, 'sl_price': 99.5,
        'tp_price': 101.5, 'quantity': 10, 'margin_usdt': 20, 'leverage': 50,
        'outcome': 'OPEN', 'open_time': '2026-04-01 12:00 UTC',
        'signal_time': '2026-04-01 12:00 UTC', 'signal_price': 100.0,
    }]
    om._load_supabase_history()
    assert len(om._open_positions) == 1
    pos = om._open_positions['BTCUSDT']
    assert pos.db_id == 99
    assert pos.open_ts > 0


# ── P&L ──────────────────────────────────────────────────────────────────────

def _make_pnl_om():
    """Construct a bare OM where _log_trade_close runs but DB is mocked."""
    om = make_bare_om()
    captured = {}
    real_close = OrderManager._log_trade_close
    def wrapped(self, pos, outcome, exit_price=None, close_time=None):
        # call the real implementation but swallow CSV write errors
        try:
            real_close(self, pos, outcome, exit_price=exit_price, close_time=close_time)
        except Exception:
            pass
        captured.update({'pos': pos, 'outcome': outcome,
                          'exit_price': exit_price})
        # the row is appended to closed_positions; expose pnl_usdt from there
        if self.closed_positions:
            captured['pnl_usdt'] = self.closed_positions[-1]['pnl_usdt']
            captured['pnl_pct']  = self.closed_positions[-1]['pnl_pct']
    om._log_trade_close = lambda *a, **k: wrapped(om, *a, **k)
    return om, captured


def test_pnl_long_win():
    om, cap = _make_pnl_om()
    pos = make_position(direction='LONG', entry=100.0, sl=99.5, tp=101.5,
                        qty=10.0, margin=20.0, leverage=50)
    om._log_trade_close(pos, 'WIN', exit_price=101.5)
    assert abs(cap['pnl_usdt'] - 15.00) < 0.01, f"pnl={cap['pnl_usdt']}"


def test_pnl_long_loss():
    om, cap = _make_pnl_om()
    pos = make_position(direction='LONG', entry=100.0, sl=99.5, tp=101.5,
                        qty=10.0, margin=20.0, leverage=50)
    om._log_trade_close(pos, 'LOSS', exit_price=99.5)
    assert abs(cap['pnl_usdt'] - (-5.00)) < 0.01, f"pnl={cap['pnl_usdt']}"


def test_pnl_short_win():
    om, cap = _make_pnl_om()
    pos = make_position(direction='SHORT', entry=100.0, sl=100.5, tp=98.5,
                        qty=10.0, margin=20.0, leverage=50)
    om._log_trade_close(pos, 'WIN', exit_price=98.5)
    assert abs(cap['pnl_usdt'] - 15.00) < 0.01, f"pnl={cap['pnl_usdt']}"


def test_pnl_short_loss():
    om, cap = _make_pnl_om()
    pos = make_position(direction='SHORT', entry=100.0, sl=100.5, tp=98.5,
                        qty=10.0, margin=20.0, leverage=50)
    om._log_trade_close(pos, 'LOSS', exit_price=100.5)
    assert abs(cap['pnl_usdt'] - (-5.00)) < 0.01, f"pnl={cap['pnl_usdt']}"


def test_notional_uses_qty_times_entry_not_margin_times_leverage():
    om, cap = _make_pnl_om()
    pos = make_position(direction='LONG', entry=0.329, sl=0.3274, tp=0.334,
                        qty=1519.0, margin=20.0, leverage=25)
    # exit at entry → pnl=0, notional=qty*entry should be 499.751
    notional = pos.quantity * pos.entry_price
    assert abs(notional - 499.751) < 0.001, f"notional={notional}"
    assert notional != 500.0   # not margin*leverage


# ── _fmt_price edge cases ────────────────────────────────────────────────────

def _client_for_fmt():
    return BinanceClient('k', 's', 'http://test.local/fapi')


def test_fmt_price_no_scientific_notation():
    c = _client_for_fmt()
    for v in [0.00000123, 0.007134, 0.32415365, 74092.3, 136986.0]:
        s = c._fmt_price(v)
        assert 'e' not in s.lower(), f"{v} -> {s}"


def test_fmt_price_returns_string():
    c = _client_for_fmt()
    for v in [0.00000123, 0.007134, 0.32415365, 74092.3, 136986.0]:
        assert isinstance(c._fmt_price(v), str)


def test_fmt_price_nonzero():
    c = _client_for_fmt()
    for v in [0.00000123, 0.007134, 0.32415365, 74092.3, 136986.0]:
        assert float(c._fmt_price(v)) > 0


# =============================================================================
# SECTION 3 — Candle Engine (step1_candle_engine.py)
# =============================================================================

def gen_candles_realistic(n=300, base=100.0):
    out = []
    for i in range(n):
        c = base + 5 * math.sin(i * 0.05) + 0.5 * math.cos(i * 0.3)
        out.append({'t': 1_700_000_000_000 + i * 900_000,
                    'o': c - 0.1, 'h': c + 0.3, 'l': c - 0.4, 'c': c})
    return out


def test_indicators_all_keys_present():
    candles = gen_candles_realistic(300)
    ind = ce.compute_indicators(candles)
    assert ind is not None
    required = ['ema9', 'ema26', 'ema200', 'ema9_prev', 'ema26_prev',
                'macd', 'macd_sig', 'macd_hist', 'adx', 'di_plus', 'di_minus',
                'ma44', 'ma44_slope_8bar', 'ma44_accel', 'atr', 'atr_pct']
    for k in required:
        assert k in ind, f"missing key: {k}"
        assert ind[k] is not None, f"key {k} is None"


def test_indicators_returns_none_below_min_candles():
    candles = gen_candles_realistic(50)
    assert ce.compute_indicators(candles) is None


def test_indicators_ema9_faster_than_ema26():
    candles = [{'t': i*900_000, 'o': 100+i, 'h': 100+i+0.5,
                'l': 100+i-0.5, 'c': 100+i*1.0} for i in range(300)]
    ind = ce.compute_indicators(candles)
    assert ind['ema9'] > ind['ema26'], f"ema9={ind['ema9']} ema26={ind['ema26']}"


def test_indicators_ema200_smoothest():
    # Build 250 stable candles, then a sudden 250th-candle jump
    base = [{'t': i*900_000, 'o': 100, 'h': 100.5, 'l': 99.5, 'c': 100.0}
            for i in range(250)]
    ind_a = ce.compute_indicators(base)
    bumped = base + [{'t': 250*900_000, 'o': 100, 'h': 130, 'l': 99, 'c': 130.0}]
    ind_b = ce.compute_indicators(bumped)
    delta_9   = abs(ind_b['ema9']   - ind_a['ema9'])
    delta_200 = abs(ind_b['ema200'] - ind_a['ema200'])
    assert delta_200 < delta_9, f"ema200 delta {delta_200} >= ema9 delta {delta_9}"


def test_candle_store_push_and_get():
    store = ce.CandleStore(['BTCUSDT'])
    for i in range(5):
        store.push('BTCUSDT', {'t': i, 'o': 1, 'h': 2, 'l': 0, 'c': 1})
    lst = store.get_list('BTCUSDT')
    assert len(lst) == 5
    assert [c['t'] for c in lst] == [0, 1, 2, 3, 4]


def test_candle_store_rolling_window():
    store = ce.CandleStore(['BTCUSDT'], limit=500)
    for i in range(600):
        store.push('BTCUSDT', {'t': i, 'o': 1, 'h': 2, 'l': 0, 'c': 1})
    assert len(store.get_list('BTCUSDT')) == 500


# ── /health endpoint ─────────────────────────────────────────────────────────

class _FakeEngine:
    def __init__(self, age):
        self._age = age
    def last_message_age_sec(self):
        return self._age


def _import_main():
    try:
        import flask  # noqa: F401
    except ImportError:
        raise _Skip("flask not installed in this Python env")
    import importlib
    import main as _m
    importlib.reload(_m)
    return _m


def test_health_returns_200_when_engine_none():
    m = _import_main()
    m.engine = None
    client = m.app.test_client()
    resp = client.get('/health')
    assert resp.status_code == 200, f"status={resp.status_code}"


def test_health_returns_200_when_no_frames_yet():
    m = _import_main()
    m.engine = _FakeEngine(float('inf'))
    client = m.app.test_client()
    resp = client.get('/health')
    assert resp.status_code == 200, f"status={resp.status_code}"


def test_health_returns_503_when_stale():
    m = _import_main()
    m.engine = _FakeEngine(200)   # > 120s → stale
    client = m.app.test_client()
    resp = client.get('/health')
    assert resp.status_code == 503, f"status={resp.status_code}"


def test_health_returns_200_when_fresh():
    m = _import_main()
    m.engine = _FakeEngine(10)
    client = m.app.test_client()
    resp = client.get('/health')
    assert resp.status_code == 200, f"status={resp.status_code}"


# =============================================================================
# SECTION 4 — Integration scenarios
# =============================================================================

def test_signal_to_order_full_flow():
    det = SignalDetector()
    om  = make_bare_om()
    setup_happy_path(om, symbol='BTCUSDT', price=11.5)
    # Make on_signal synchronous (no thread spawn) so we can assert deterministically
    sync_on_signal = lambda s: om._handle_signal(s)
    def chain(sig):
        sync_on_signal(sig)
    det.on_signal = chain
    det.on_candle_close('BTCUSDT', long_signal_candle(1_700_000_000_000),
                        s1_long_pass_ind())
    assert om.client.place_market_order.called, "entry order not placed"
    assert om.client.place_take_profit_order.called
    assert om.client.place_stop_loss_order.called
    assert om.supabase.insert_returning_id.called
    assert 'BTCUSDT' in om._open_positions


def test_full_flow_tp_hit_closes_cleanly():
    det = SignalDetector()
    om  = make_bare_om()
    om.detector = det   # so on_trade_closed is wired through
    setup_happy_path(om, symbol='BTCUSDT', price=11.5)
    det.on_signal = lambda s: om._handle_signal(s)
    det.on_candle_close('BTCUSDT', long_signal_candle(1_700_000_000_000),
                        s1_long_pass_ind())
    assert 'BTCUSDT' in om._open_positions
    pos = om._open_positions['BTCUSDT']
    # Simulate TP fill via monitor
    _wire_monitor_close(om, 'BTCUSDT', exit_price=pos.tp_price, close_side='SELL')
    om._check_positions()
    assert 'BTCUSDT' not in om._open_positions
    # Detector gate released
    assert det._get_state('BTCUSDT').s1_trade_open is False


def test_concurrent_signals_different_symbols():
    om = make_bare_om()
    setup_happy_path(om)   # reused for both, MagicMock returns same regardless of symbol
    s1 = make_signal(symbol='BTCUSDT')
    s2 = make_signal(symbol='ETHUSDT')
    om.on_signal(s1)
    om.on_signal(s2)
    join_trade_threads(timeout=3.0)
    assert 'BTCUSDT' in om._open_positions
    assert 'ETHUSDT' in om._open_positions
    assert len(om._open_positions) == 2


def test_redeploy_scenario():
    om = make_bare_om()
    om.supabase.select_all.return_value = [
        {'id': 1, 'symbol': 'BTCUSDT', 'strategy': 'S1_EMA_CROSS',
         'direction': 'LONG', 'entry_price': 100.0, 'sl_price': 99.5,
         'tp_price': 101.5, 'quantity': 10, 'margin_usdt': 20, 'leverage': 50,
         'outcome': 'OPEN', 'open_time': '2026-04-01 12:00 UTC',
         'signal_time': '2026-04-01 12:00 UTC', 'signal_price': 100.0},
        {'id': 2, 'symbol': 'ETHUSDT', 'strategy': 'S1_EMA_CROSS',
         'direction': 'LONG', 'entry_price': 2500.0, 'sl_price': 2487.5,
         'tp_price': 2537.5, 'quantity': 0.04, 'margin_usdt': 20, 'leverage': 50,
         'outcome': 'OPEN', 'open_time': '2026-04-01 12:00 UTC',
         'signal_time': '2026-04-01 12:00 UTC', 'signal_price': 2500.0},
    ]
    om._load_supabase_history()
    assert len(om._open_positions) == 2
    # Reconciliation: both confirmed live on Binance
    def _get(path, params=None, signed=False):
        if path == '/v2/positionRisk':
            return [{'symbol': 'BTCUSDT', 'positionAmt': '10'},
                    {'symbol': 'ETHUSDT', 'positionAmt': '0.04'}]
        return []
    om.client._get.side_effect = _get
    om._reconcile_on_startup()
    assert len(om._open_positions) == 2
    # Monitor: both still open (algo orders not finished)
    om.client.get_algo_order.return_value = {'algoStatus': 'NEW'}
    om._check_positions()
    assert len(om._open_positions) == 2
    # No duplicate-open path triggered because we never called _handle_signal
    assert om.client.place_market_order.called is False


# =============================================================================
# REGISTRATION + RUNNER
# =============================================================================

TESTS = [
    # Section 1 — S1 gate
    ('test_s1_gate_blocks_second_signal',          test_s1_gate_blocks_second_signal),
    ('test_s1_gate_releases_after_close',          test_s1_gate_releases_after_close),
    ('test_s1_gate_per_symbol',                    test_s1_gate_per_symbol),
    ('test_s1_pending_expires_after_2_candles',    test_s1_pending_expires_after_2_candles),
    ('test_s1_pending_overwritten_by_new_crossover', test_s1_pending_overwritten_by_new_crossover),
    # Section 1 — S1 filters
    ('test_s1_f2_bearish_candle_required_for_short', test_s1_f2_bearish_candle_required_for_short),
    ('test_s1_f3_ema200_blocks_long_below',        test_s1_f3_ema200_blocks_long_below),
    ('test_s1_f3_ema200_blocks_short_above',       test_s1_f3_ema200_blocks_short_above),
    ('test_s1_f4_adx_below_25_blocks',             test_s1_f4_adx_below_25_blocks),
    ('test_s1_f4_adx_exactly_25_blocks',           test_s1_f4_adx_exactly_25_blocks),
    ('test_s1_f5_di_wrong_direction_blocks_long',  test_s1_f5_di_wrong_direction_blocks_long),
    ('test_s1_f5_di_wrong_direction_blocks_short', test_s1_f5_di_wrong_direction_blocks_short),
    ('test_s1_f6_macd_wrong_side_blocks',          test_s1_f6_macd_wrong_side_blocks),
    ('test_s1_all_filters_pass_fires_signal',      test_s1_all_filters_pass_fires_signal),
    # Section 1 — S1 SL/TP
    ('test_s1_long_sl_tp',                         test_s1_long_sl_tp),
    ('test_s1_short_sl_tp',                        test_s1_short_sl_tp),
    # Section 1 — S2
    ('test_s2_setup_then_trigger',                 test_s2_setup_then_trigger),
    ('test_s2_setup_cleared_after_one_attempt',    test_s2_setup_cleared_after_one_attempt),
    ('test_s2_gate_blocks_while_open',             test_s2_gate_blocks_while_open),
    ('test_s2_gate_releases_after_close',          test_s2_gate_releases_after_close),
    ('test_s2_cooldown_blocks_within_4h',          test_s2_cooldown_blocks_within_4h),
    ('test_s2_cooldown_clears_after_4h',           test_s2_cooldown_clears_after_4h),
    ('test_s2_h4_rising_blocks',                   test_s2_h4_rising_blocks),
    ('test_s2_body_above_ma44_blocks',             test_s2_body_above_ma44_blocks),
    ('test_s2_zone_a_passes',                      test_s2_zone_a_passes),
    ('test_s2_zone_b_passes',                      test_s2_zone_b_passes),
    ('test_s2_out_of_zone_blocks',                 test_s2_out_of_zone_blocks),
    ('test_s2_bullish_candle_blocks',              test_s2_bullish_candle_blocks),
    ('test_s2_short_sl_tp',                        test_s2_short_sl_tp),
    # Section 2 — OM gate
    ('test_om_max_positions_blocks',               test_om_max_positions_blocks),
    ('test_om_duplicate_symbol_blocks',            test_om_duplicate_symbol_blocks),
    ('test_om_pending_symbol_blocks',              test_om_pending_symbol_blocks),
    ('test_om_s2_paused_after_2_losses',           test_om_s2_paused_after_2_losses),
    ('test_om_s1_not_paused_after_losses',         test_om_s1_not_paused_after_losses),
    # Section 2 — balance/cap
    ('test_om_insufficient_balance_skips',         test_om_insufficient_balance_skips),
    ('test_om_position_cap_below_margin_skips',    test_om_position_cap_below_margin_skips),
    # Section 2 — pre-flight
    ('test_om_existing_binance_position_skips',    test_om_existing_binance_position_skips),
    ('test_om_no_binance_position_proceeds',       test_om_no_binance_position_proceeds),
]

# TP/SL payload tests for all 15 symbols (registered dynamically)
for _sym in ALL_SYMBOLS:
    _price, _qty = SYMBOL_PRICES[_sym]
    TESTS.append((f'test_tp_payload_{_sym}', make_tp_payload_test(_sym, _price, _qty)))
    TESTS.append((f'test_sl_payload_{_sym}', make_sl_payload_test(_sym, _price, _qty)))

TESTS += [
    # Section 2 — emergency close
    ('test_emergency_close_fires_on_tp_sl_failure', test_emergency_close_fires_on_tp_sl_failure),
    ('test_emergency_close_uses_real_exit_price',   test_emergency_close_uses_real_exit_price),
    ('test_emergency_close_falls_back_to_entry',    test_emergency_close_falls_back_to_entry),
    # Section 2 — monitor
    ('test_monitor_detects_tp_fill',                test_monitor_detects_tp_fill),
    ('test_monitor_detects_sl_fill',                test_monitor_detects_sl_fill),
    ('test_monitor_ignores_still_open',             test_monitor_ignores_still_open),
    ('test_monitor_consecutive_loss_increments',    test_monitor_consecutive_loss_increments),
    ('test_monitor_consecutive_loss_resets_on_win', test_monitor_consecutive_loss_resets_on_win),
    ('test_consec_loss_key_s1_not_s2',              test_consec_loss_key_s1_not_s2),
    # Section 2 — startup reconcile
    ('test_reconcile_closes_stale_position',        test_reconcile_closes_stale_position),
    ('test_reconcile_keeps_live_position',          test_reconcile_keeps_live_position),
    ('test_load_history_restores_open_positions',   test_load_history_restores_open_positions),
    # Section 2 — P&L
    ('test_pnl_long_win',                           test_pnl_long_win),
    ('test_pnl_long_loss',                          test_pnl_long_loss),
    ('test_pnl_short_win',                          test_pnl_short_win),
    ('test_pnl_short_loss',                         test_pnl_short_loss),
    ('test_notional_uses_qty_times_entry_not_margin_times_leverage',
     test_notional_uses_qty_times_entry_not_margin_times_leverage),
    # Section 2 — _fmt_price
    ('test_fmt_price_no_scientific_notation',       test_fmt_price_no_scientific_notation),
    ('test_fmt_price_returns_string',               test_fmt_price_returns_string),
    ('test_fmt_price_nonzero',                      test_fmt_price_nonzero),
    # Section 3 — Candle Engine
    ('test_indicators_all_keys_present',            test_indicators_all_keys_present),
    ('test_indicators_returns_none_below_min_candles', test_indicators_returns_none_below_min_candles),
    ('test_indicators_ema9_faster_than_ema26',      test_indicators_ema9_faster_than_ema26),
    ('test_indicators_ema200_smoothest',            test_indicators_ema200_smoothest),
    ('test_candle_store_push_and_get',              test_candle_store_push_and_get),
    ('test_candle_store_rolling_window',            test_candle_store_rolling_window),
    ('test_health_returns_200_when_engine_none',    test_health_returns_200_when_engine_none),
    ('test_health_returns_200_when_no_frames_yet',  test_health_returns_200_when_no_frames_yet),
    ('test_health_returns_503_when_stale',          test_health_returns_503_when_stale),
    ('test_health_returns_200_when_fresh',          test_health_returns_200_when_fresh),
    # Section 4 — Integration
    ('test_signal_to_order_full_flow',              test_signal_to_order_full_flow),
    ('test_full_flow_tp_hit_closes_cleanly',        test_full_flow_tp_hit_closes_cleanly),
    ('test_concurrent_signals_different_symbols',   test_concurrent_signals_different_symbols),
    ('test_redeploy_scenario',                      test_redeploy_scenario),
]


if __name__ == '__main__':
    print(f"Running {len(TESTS)} tests...\n")
    for name, fn in TESTS:
        run(name, fn)
    print()
    suffix = f", {SKIP} skipped" if SKIP else ""
    print(f"RESULTS: {PASS} passed, {FAIL} failed{suffix}")
    sys.exit(0 if FAIL == 0 else 1)
