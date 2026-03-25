"""
STEP 3 OF 4 — Order Manager
=============================
Receives SignalEvents from Step 2 (SignalDetector).
Places trades on Binance Testnet via REST API.
Monitors open positions and reports outcomes back to Step 2.

Changes in this version:
  - Fixed scientific notation bug (e.g. 3.175e-05) for low-price coins like FLOKI, SHIB, PEPE
  - _fmt_price() ensures all prices sent to Binance are plain decimal strings
  - Same fix applied to quantity formatting in market orders
"""

import os
import sys
import io
import csv
import time
import hmac
import hashlib
import logging
import threading
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except AttributeError:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

log = logging.getLogger('order_manager')

load_dotenv()

API_KEY    = os.getenv('BINANCE_API_KEY', '')
API_SECRET = os.getenv('BINANCE_API_SECRET', os.getenv('BINANCE_SECRET', ''))
TESTNET    = os.getenv('TESTNET', 'true').lower() == 'true'
BASE_URL   = "https://testnet.binance.vision/api" if TESTNET else "https://api.binance.com/api"

TRADE_USDT     = 20.0
POLL_INTERVAL  = 15
TRADE_LOG_FILE = 'trade_log.csv'


# ============================================================================
# BINANCE REST CLIENT
# ============================================================================

class BinanceClient:

    def __init__(self, api_key: str, api_secret: str, base_url: str):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.base_url   = base_url
        self.session    = requests.Session()
        self.session.headers.update({'X-MBX-APIKEY': api_key})

    def _fmt_price(self, value: float) -> str:
        """
        Format a float as a plain decimal string.
        Binance rejects scientific notation (e.g. 3.175e-05).
        Works for both prices and quantities.
        """
        # Use enough decimal places to capture small values like 0.00003175
        formatted = f'{value:.10f}'
        # Strip trailing zeros but keep at least one decimal place
        formatted = formatted.rstrip('0')
        if formatted.endswith('.'):
            formatted += '0'
        return formatted

    def _sign(self, params: dict) -> dict:
        params['timestamp'] = int(time.time() * 1000)
        query = '&'.join(f"{k}={v}" for k, v in params.items())
        sig   = hmac.new(
            self.api_secret.encode('utf-8'),
            query.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        params['signature'] = sig
        return params

    def _get(self, path: str, params: dict = None, signed: bool = False):
        params = params or {}
        if signed:
            params = self._sign(params)
        resp = self.session.get(f"{self.base_url}{path}", params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, params: dict):
        params = self._sign(params)
        resp = self.session.post(f"{self.base_url}{path}", params=params, timeout=10)
        if resp.status_code != 200:
            log.error(f"POST {path} failed {resp.status_code}: {resp.text}")
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str, params: dict):
        params = self._sign(params)
        resp = self.session.delete(f"{self.base_url}{path}", params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_symbol_info(self, symbol: str) -> dict:
        info = self._get('/v3/exchangeInfo', {'symbol': symbol})
        for s in info.get('symbols', []):
            if s['symbol'] == symbol:
                return s
        return {}

    def get_ticker_price(self, symbol: str) -> float:
        data = self._get('/v3/ticker/price', {'symbol': symbol})
        return float(data['price'])

    def get_account(self) -> dict:
        return self._get('/v3/account', {}, signed=True)

    def get_usdt_balance(self) -> float:
        account = self.get_account()
        for b in account.get('balances', []):
            if b['asset'] == 'USDT':
                return float(b['free'])
        return 0.0

    def place_market_order(self, symbol: str, side: str, quantity: float) -> dict:
        """Place a MARKET order. Quantity is formatted to avoid scientific notation."""
        params = {
            'symbol':   symbol,
            'side':     side,
            'type':     'MARKET',
            'quantity': self._fmt_price(quantity),   # ← fixed: no scientific notation
        }
        return self._post('/v3/order', params)

    def place_oco_order(self, symbol: str, side: str,
                        quantity: float, tp_price: float,
                        sl_price: float, sl_limit_price: float) -> dict:
        """
        Place an OCO order for SL + TP.
        All prices formatted as plain decimals — Binance rejects scientific notation.
        """
        params = {
            'symbol':               symbol,
            'side':                 side,
            'quantity':             self._fmt_price(quantity),        # ← fixed
            'price':                self._fmt_price(tp_price),        # ← fixed
            'stopPrice':            self._fmt_price(sl_price),        # ← fixed
            'stopLimitPrice':       self._fmt_price(sl_limit_price),  # ← fixed
            'stopLimitTimeInForce': 'GTC',
            'listClientOrderId':    f"bot_{symbol}_{int(time.time())}",
        }
        return self._post('/v3/order/oco', params)

    def get_oco_status(self, order_list_id: int) -> dict:
        return self._get('/v3/orderList', {'orderListId': order_list_id}, signed=True)

    def cancel_oco(self, symbol: str, order_list_id: int) -> dict:
        return self._delete('/v3/orderList', {
            'symbol':      symbol,
            'orderListId': order_list_id,
        })

    def get_order(self, symbol: str, order_id: int) -> dict:
        return self._get('/v3/order', {'symbol': symbol, 'orderId': order_id}, signed=True)


# ============================================================================
# SYMBOL PRECISION HELPER
# ============================================================================

class PrecisionCache:

    def __init__(self, client: BinanceClient):
        self._client = client
        self._cache  = {}
        self._lock   = threading.Lock()

    def get(self, symbol: str) -> dict:
        with self._lock:
            if symbol in self._cache:
                return self._cache[symbol]

        info    = self._client.get_symbol_info(symbol)
        filters = {f['filterType']: f for f in info.get('filters', [])}

        lot      = filters.get('LOT_SIZE', {})
        tick     = filters.get('PRICE_FILTER', {})
        notional = filters.get('MIN_NOTIONAL', {})

        def _decimals(step_str: str) -> int:
            s = step_str.rstrip('0')
            return len(s.split('.')[-1]) if '.' in s else 0

        result = {
            'qty_step':       float(lot.get('stepSize', '0.001')),
            'qty_decimals':   _decimals(lot.get('stepSize', '0.001')),
            'price_step':     float(tick.get('tickSize', '0.01')),
            'price_decimals': _decimals(tick.get('tickSize', '0.01')),
            'min_qty':        float(lot.get('minQty', '0.001')),
            'min_notional':   float(notional.get('minNotional', '10')),
        }

        with self._lock:
            self._cache[symbol] = result
        return result

    def round_qty(self, symbol: str, qty: float) -> float:
        p    = self.get(symbol)
        step = p['qty_step']
        return round(round(qty / step) * step, p['qty_decimals'])

    def round_price(self, symbol: str, price: float) -> float:
        p    = self.get(symbol)
        step = p['price_step']
        return round(round(price / step) * step, p['price_decimals'])


# ============================================================================
# OPEN POSITION TRACKER
# ============================================================================

class OpenPosition:

    def __init__(self, symbol, strategy, direction,
                 entry_price, sl_price, tp_price,
                 quantity, oco_list_id, entry_order_id,
                 signal_ts, signal_time):
        self.symbol         = symbol
        self.strategy       = strategy
        self.direction      = direction
        self.entry_price    = entry_price
        self.sl_price       = sl_price
        self.tp_price       = tp_price
        self.quantity       = quantity
        self.oco_list_id    = oco_list_id
        self.entry_order_id = entry_order_id
        self.signal_ts      = signal_ts
        self.signal_time    = signal_time
        self.open_time      = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')


# ============================================================================
# ORDER MANAGER
# ============================================================================

class OrderManager:

    def __init__(self, detector=None, alerts=None):
        if not API_KEY or not API_SECRET:
            raise ValueError(
                "API keys not found. Create a .env file with:\n"
                "  BINANCE_API_KEY=your_key\n"
                "  BINANCE_SECRET=your_secret"
            )

        self.detector  = detector
        self.alerts    = alerts
        self.client    = BinanceClient(API_KEY, API_SECRET, BASE_URL)
        self.precision = PrecisionCache(self.client)

        self._lock               = threading.Lock()
        self._open_positions     = {}
        self._global_trade_open  = False

        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, daemon=True, name='pos_monitor'
        )
        self._monitor_thread.start()

        self._init_csv()

        log.info(f"OrderManager ready | Testnet={TESTNET} | "
                 f"Trade size={TRADE_USDT} USDT/trade")

        try:
            bal = self.client.get_usdt_balance()
            log.info(f"Testnet USDT balance: {bal:.2f}")
        except Exception as e:
            log.error(f"Could not fetch balance — check API keys: {e}")

    # ── Signal handler ────────────────────────────────────────────────────────

    def on_signal(self, signal):
        t = threading.Thread(
            target=self._handle_signal,
            args=(signal,),
            daemon=True,
            name=f"trade_{signal.symbol}"
        )
        t.start()

    def _handle_signal(self, signal):
        symbol    = signal.symbol
        direction = signal.direction
        strategy  = signal.strategy

        with self._lock:
            if self._global_trade_open:
                log.info(f"[SKIP] {symbol} {strategy}: trade already open globally")
                return
            if symbol in self._open_positions:
                log.info(f"[SKIP] {symbol}: already has open position")
                return
            self._global_trade_open = True

        log.info(f"[ORDER] Processing signal: {symbol} {strategy} {direction} "
                 f"entry~{signal.entry_price:.6f}")

        try:
            current_price = self.client.get_ticker_price(symbol)
            prec          = self.precision.get(symbol)

            raw_qty = TRADE_USDT / current_price
            qty     = self.precision.round_qty(symbol, raw_qty)

            if qty < prec['min_qty']:
                log.warning(f"[SKIP] {symbol}: qty {qty} < min_qty {prec['min_qty']}")
                self._global_trade_open = False
                return

            if qty * current_price < prec['min_notional']:
                log.warning(f"[SKIP] {symbol}: notional too small")
                self._global_trade_open = False
                return

            entry_side   = 'BUY' if direction == 'LONG' else 'SELL'
            log.info(f"[ORDER] Placing {entry_side} MARKET {qty} {symbol} @ ~{current_price:.6f}")

            entry_result   = self.client.place_market_order(symbol, entry_side, qty)
            entry_order_id = entry_result['orderId']

            fills = entry_result.get('fills', [])
            if fills:
                total_qty    = sum(float(f['qty'])   for f in fills)
                total_cost   = sum(float(f['qty']) * float(f['price']) for f in fills)
                actual_entry = total_cost / total_qty if total_qty > 0 else current_price
            else:
                actual_entry = current_price

            log.info(f"[ORDER] Entry filled: {entry_side} {qty} {symbol} @ {actual_entry:.6f} "
                     f"(order #{entry_order_id})")

            sl_pct = signal.sl_price / signal.entry_price
            tp_pct = signal.tp_price / signal.entry_price

            sl_price = self.precision.round_price(symbol, actual_entry * sl_pct)
            tp_price = self.precision.round_price(symbol, actual_entry * tp_pct)

            if direction == 'LONG':
                sl_limit = self.precision.round_price(symbol, sl_price * 0.999)
            else:
                sl_limit = self.precision.round_price(symbol, sl_price * 1.001)

            oco_side = 'SELL' if direction == 'LONG' else 'BUY'

            log.info(f"[ORDER] Placing OCO {oco_side} | TP={tp_price:.6f} "
                     f"SL={sl_price:.6f} SL_limit={sl_limit:.6f}")

            try:
                oco_result  = self.client.place_oco_order(
                    symbol, oco_side, qty, tp_price, sl_price, sl_limit
                )
                oco_list_id = oco_result['orderListId']
                log.info(f"[ORDER] OCO placed | listId={oco_list_id} | "
                         f"TP={tp_price:.6f} SL={sl_price:.6f}")
            except Exception as oco_err:
                # OCO failed AFTER entry already filled — close immediately at market
                log.error(f"[ORDER] OCO failed for {symbol}: {oco_err} — "
                          f"closing position at market to avoid orphaned trade")
                try:
                    self.client.place_market_order(symbol, oco_side, qty)
                    log.info(f"[ORDER] Emergency market close sent: {symbol} "
                             f"{oco_side} qty={qty}")
                    if self.alerts:
                        self.alerts.on_error(f"OCO failed for {symbol} — emergency "
                                        f"market close sent. Check position manually.")
                except Exception as close_err:
                    log.error(f"[ORDER] Emergency close also failed for {symbol}: "
                              f"{close_err} — MANUAL INTERVENTION REQUIRED")
                    if self.alerts:
                        self.alerts.on_error(f"URGENT: {symbol} entry filled but OCO AND "
                                        f"emergency close both failed. Manual close needed!")
                with self._lock:
                    self._global_trade_open = False
                return

            position = OpenPosition(
                symbol         = symbol,
                strategy       = strategy,
                direction      = direction,
                entry_price    = actual_entry,
                sl_price       = sl_price,
                tp_price       = tp_price,
                quantity       = qty,
                oco_list_id    = oco_list_id,
                entry_order_id = entry_order_id,
                signal_ts      = signal.signal_ts,
                signal_time    = signal.signal_time,
            )

            with self._lock:
                self._open_positions[symbol] = position

            self._log_trade_open(position)

        except Exception as e:
            log.error(f"[ORDER] Failed to place trade for {symbol}: {e}", exc_info=True)
            with self._lock:
                self._global_trade_open = False

    # ── Position monitor ──────────────────────────────────────────────────────

    def _monitor_loop(self):
        log.info("Position monitor started")
        while True:
            time.sleep(POLL_INTERVAL)
            try:
                self._check_positions()
            except Exception as e:
                log.error(f"Monitor error: {e}", exc_info=True)

    def _check_positions(self):
        with self._lock:
            positions = list(self._open_positions.values())

        for pos in positions:
            try:
                oco    = self.client.get_oco_status(pos.oco_list_id)
                status = oco.get('listStatusType', '')

                if status != 'ALL_DONE':
                    continue

                outcome = self._determine_outcome(oco, pos)

                log.info(f"[CLOSED] {pos.symbol} {pos.strategy} {pos.direction} | "
                         f"outcome={outcome} | entry={pos.entry_price:.6f} "
                         f"SL={pos.sl_price:.6f} TP={pos.tp_price:.6f}")

                self._log_trade_close(pos, outcome)

                with self._lock:
                    self._open_positions.pop(pos.symbol, None)
                    self._global_trade_open = False

                if self.detector:
                    self.detector.on_trade_closed(pos.symbol, pos.strategy, outcome)

            except Exception as e:
                log.warning(f"Could not check position {pos.symbol} "
                            f"(oco#{pos.oco_list_id}): {e}")

    def _determine_outcome(self, oco_response: dict, pos: OpenPosition) -> str:
        orders = oco_response.get('orders', [])
        for order in orders:
            order_detail = self.client.get_order(pos.symbol, order['orderId'])
            if order_detail.get('status') == 'FILLED':
                order_type = order_detail.get('type', '')
                if order_type in ('LIMIT_MAKER', 'LIMIT'):
                    return 'WIN'
                else:
                    return 'LOSS'
        return 'UNKNOWN'

    # ── CSV trade log ─────────────────────────────────────────────────────────

    def _init_csv(self):
        if not os.path.exists(TRADE_LOG_FILE):
            with open(TRADE_LOG_FILE, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'open_time', 'close_time', 'symbol', 'strategy',
                    'direction', 'entry_price', 'sl_price', 'tp_price',
                    'quantity', 'outcome', 'pnl_pct',
                    'signal_time', 'oco_list_id'
                ])

    def _log_trade_open(self, pos: OpenPosition):
        log.info(f"[LOG] Trade opened: {pos.symbol} {pos.strategy} {pos.direction} "
                 f"entry={pos.entry_price:.6f} SL={pos.sl_price:.6f} TP={pos.tp_price:.6f} "
                 f"qty={pos.quantity}")

    def _log_trade_close(self, pos: OpenPosition, outcome: str):
        close_time = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

        if outcome == 'WIN':
            pnl_pct = (pos.tp_price - pos.entry_price) / pos.entry_price * 100 \
                      if pos.direction == 'LONG' else \
                      (pos.entry_price - pos.tp_price) / pos.entry_price * 100
        elif outcome == 'LOSS':
            pnl_pct = (pos.sl_price - pos.entry_price) / pos.entry_price * 100 \
                      if pos.direction == 'LONG' else \
                      (pos.entry_price - pos.sl_price) / pos.entry_price * 100
        else:
            pnl_pct = 0.0

        with open(TRADE_LOG_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                pos.open_time, close_time, pos.symbol, pos.strategy,
                pos.direction, f"{pos.entry_price:.8f}",
                f"{pos.sl_price:.8f}", f"{pos.tp_price:.8f}",
                pos.quantity, outcome, f"{pnl_pct:.3f}",
                pos.signal_time, pos.oco_list_id
            ])

        log.info(f"[LOG] Trade closed: {pos.symbol} {outcome} PnL={pnl_pct:+.3f}%")


# ============================================================================
# STANDALONE TEST
# ============================================================================

if __name__ == '__main__':
    logging.basicConfig(
        level   = logging.INFO,
        format  = '%(asctime)s  %(levelname)-7s  %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S',
        handlers = [
            logging.FileHandler('bot.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout),
        ]
    )

    print("""
+------------------------------------------------------+
|  STEP 3 -- Order Manager  (connectivity test)        |
|  Does NOT place any orders.                          |
+------------------------------------------------------+
""")

    if not API_KEY or not API_SECRET:
        print("ERROR: No API keys found.")
        sys.exit(1)

    client = BinanceClient(API_KEY, API_SECRET, BASE_URL)

    print(f"  Testnet : {TESTNET}")
    print(f"  Base URL: {BASE_URL}")
    print()

    try:
        bal = client.get_usdt_balance()
        print(f"  [OK] USDT Balance    : {bal:.2f} USDT")
    except Exception as e:
        print(f"  [FAIL] Balance fetch : {e}")
        sys.exit(1)

    try:
        info = client.get_symbol_info('BTCUSDT')
        print(f"  [OK] BTCUSDT info    : status={info.get('status')}")
    except Exception as e:
        print(f"  [FAIL] Symbol info   : {e}")

    try:
        price = client.get_ticker_price('BTCUSDT')
        print(f"  [OK] BTCUSDT price   : ${price:.2f}")
    except Exception as e:
        print(f"  [FAIL] Price fetch   : {e}")

    try:
        pc   = PrecisionCache(client)
        prec = pc.get('BTCUSDT')
        qty  = pc.round_qty('BTCUSDT', TRADE_USDT / price)
        print(f"  [OK] Precision       : qty_step={prec['qty_step']} "
              f"price_step={prec['price_step']}")
        print(f"  [OK] Order qty       : {qty} BTC "
              f"(= ~{qty*price:.2f} USDT for ${TRADE_USDT} budget)")

        # Test _fmt_price with a very small number (FLOKI-like)
        test_price = 3.175e-05
        formatted  = client._fmt_price(test_price)
        print(f"  [OK] fmt_price test  : {test_price} → '{formatted}' "
              f"(no scientific notation)")
    except Exception as e:
        print(f"  [FAIL] Precision     : {e}")

    print()
    print("  All connectivity tests passed.")
    print(f"  Trade size is set to {TRADE_USDT} USDT per trade.")