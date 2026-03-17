"""
STEP 3 OF 4 — Order Manager
=============================
Receives SignalEvents from Step 2 (SignalDetector).
Places trades on Binance Testnet via REST API.
Monitors open positions and reports outcomes back to Step 2.

What this file does:
  - Reads API keys from a .env file (never hardcoded)
  - Receives SignalEvent from detector.on_signal
  - Places a MARKET order for entry
  - Places an OCO order for SL + TP immediately after entry
  - Monitors the OCO order in a background thread
  - When OCO fills (SL or TP hit), calls detector.on_trade_closed()
  - Enforces one trade at a time globally (across all symbols + strategies)
  - Logs every order action to bot.log and trade_log.csv

Setup:
  1. Create a file called  .env  in the same folder as this script
  2. Add these two lines (use YOUR keys, not these):
       BINANCE_API_KEY=your_api_key_here
       BINANCE_SECRET=your_secret_key_here
  3. pip install python-binance python-dotenv

Usage (integrated — this is how main.py will call it):
  from step3_order_manager import OrderManager
  manager = OrderManager(detector)   # detector = SignalDetector instance
  detector.on_signal = manager.on_signal
  # then start the engine normally

Dependencies:
  pip install python-binance python-dotenv
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

# Fix Windows console encoding
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except AttributeError:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

log = logging.getLogger('order_manager')

# ============================================================================
# CONFIGURATION
# ============================================================================

load_dotenv()   # reads .env file in current directory

API_KEY    = os.getenv('BINANCE_API_KEY', '')
API_SECRET = os.getenv('BINANCE_API_SECRET', os.getenv('BINANCE_SECRET', ''))

TESTNET         = os.getenv('TESTNET', 'true').lower() == 'true'
BASE_URL        = "https://testnet.binance.vision/api" if TESTNET else "https://api.binance.com/api"

# Trade sizing — fixed USDT per trade
TRADE_USDT      = 20.0      # spend this much USDT per trade on testnet
                             # (keep small on testnet — default balance is 1000 USDT)

# How often to poll OCO order status (seconds)
POLL_INTERVAL   = 15

# Trade log CSV file
TRADE_LOG_FILE  = 'trade_log.csv'

# ============================================================================
# BINANCE REST CLIENT — thin wrapper, no external SDK needed beyond requests
# ============================================================================

class BinanceClient:
    """
    Minimal signed REST client for Binance (Testnet or Live).
    Only the endpoints we need: account info, order placement, order query.
    """

    def __init__(self, api_key: str, api_secret: str, base_url: str):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.base_url   = base_url
        self.session    = requests.Session()
        self.session.headers.update({'X-MBX-APIKEY': api_key})

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

    # ── Public endpoints ──────────────────────────────────────────────────────

    def get_symbol_info(self, symbol: str) -> dict:
        """Returns symbol trading rules (tick size, lot size, min notional)."""
        info = self._get('/v3/exchangeInfo', {'symbol': symbol})
        for s in info.get('symbols', []):
            if s['symbol'] == symbol:
                return s
        return {}

    def get_ticker_price(self, symbol: str) -> float:
        data = self._get('/v3/ticker/price', {'symbol': symbol})
        return float(data['price'])

    # ── Account ───────────────────────────────────────────────────────────────

    def get_account(self) -> dict:
        return self._get('/v3/account', {}, signed=True)

    def get_usdt_balance(self) -> float:
        account = self.get_account()
        for b in account.get('balances', []):
            if b['asset'] == 'USDT':
                return float(b['free'])
        return 0.0

    # ── Orders ────────────────────────────────────────────────────────────────

    def place_market_order(self, symbol: str, side: str, quantity: float) -> dict:
        """
        Place a MARKET order.
        side: 'BUY' or 'SELL'
        quantity: asset quantity (not USDT amount)
        """
        params = {
            'symbol':   symbol,
            'side':     side,
            'type':     'MARKET',
            'quantity': quantity,
        }
        return self._post('/v3/order', params)

    def place_oco_order(self, symbol: str, side: str,
                        quantity: float, tp_price: float,
                        sl_price: float, sl_limit_price: float) -> dict:
        """
        Place an OCO (One-Cancels-the-Other) order for SL + TP.

        For a LONG position:
          side = SELL
          price (limit) = TP price
          stopPrice     = SL trigger
          stopLimitPrice = SL limit (slightly below stopPrice to ensure fill)

        For a SHORT position:
          side = BUY
          price (limit) = TP price  (below entry)
          stopPrice     = SL trigger (above entry)
          stopLimitPrice = slightly above stopPrice
        """
        params = {
            'symbol':            symbol,
            'side':              side,
            'quantity':          quantity,
            'price':             tp_price,           # limit leg (TP)
            'stopPrice':         sl_price,           # stop trigger
            'stopLimitPrice':    sl_limit_price,     # stop limit price
            'stopLimitTimeInForce': 'GTC',
            'listClientOrderId': f"bot_{symbol}_{int(time.time())}",
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
    """
    Caches lot size and tick size per symbol so we round quantities correctly.
    Binance rejects orders with wrong decimal precision.
    """

    def __init__(self, client: BinanceClient):
        self._client = client
        self._cache  = {}
        self._lock   = threading.Lock()

    def get(self, symbol: str) -> dict:
        with self._lock:
            if symbol in self._cache:
                return self._cache[symbol]

        info     = self._client.get_symbol_info(symbol)
        filters  = {f['filterType']: f for f in info.get('filters', [])}

        lot  = filters.get('LOT_SIZE', {})
        tick = filters.get('PRICE_FILTER', {})
        notional = filters.get('MIN_NOTIONAL', {})

        def _decimals(step_str: str) -> int:
            s = step_str.rstrip('0')
            return len(s.split('.')[-1]) if '.' in s else 0

        result = {
            'qty_step':     float(lot.get('stepSize', '0.001')),
            'qty_decimals': _decimals(lot.get('stepSize', '0.001')),
            'price_step':   float(tick.get('tickSize', '0.01')),
            'price_decimals': _decimals(tick.get('tickSize', '0.01')),
            'min_qty':      float(lot.get('minQty', '0.001')),
            'min_notional': float(notional.get('minNotional', '10')),
        }

        with self._lock:
            self._cache[symbol] = result

        return result

    def round_qty(self, symbol: str, qty: float) -> float:
        p = self.get(symbol)
        step = p['qty_step']
        return round(round(qty / step) * step, p['qty_decimals'])

    def round_price(self, symbol: str, price: float) -> float:
        p = self.get(symbol)
        step = p['price_step']
        return round(round(price / step) * step, p['price_decimals'])


# ============================================================================
# OPEN POSITION TRACKER
# ============================================================================

class OpenPosition:
    """Represents one live trade being monitored."""

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
    """
    Receives SignalEvents, places orders, monitors positions.

    Usage:
        manager = OrderManager(detector)
        detector.on_signal = manager.on_signal
    """

    def __init__(self, detector=None):
        if not API_KEY or not API_SECRET:
            raise ValueError(
                "API keys not found. Create a .env file with:\n"
                "  BINANCE_API_KEY=your_key\n"
                "  BINANCE_SECRET=your_secret"
            )

        self.detector  = detector
        self.client    = BinanceClient(API_KEY, API_SECRET, BASE_URL)
        self.precision = PrecisionCache(self.client)

        # Global trade gate — one trade at a time across all symbols/strategies
        self._lock          = threading.Lock()
        self._open_positions = {}   # symbol -> OpenPosition
        self._global_trade_open = False

        # Start position monitor thread
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, daemon=True, name='pos_monitor'
        )
        self._monitor_thread.start()

        # Ensure CSV log exists with header
        self._init_csv()

        log.info(f"OrderManager ready | Testnet={TESTNET} | "
                 f"Trade size={TRADE_USDT} USDT/trade")

        # Verify connectivity and balance
        try:
            bal = self.client.get_usdt_balance()
            log.info(f"Testnet USDT balance: {bal:.2f}")
        except Exception as e:
            log.error(f"Could not fetch balance — check API keys: {e}")

    # ==========================================================================
    # SIGNAL HANDLER — called by SignalDetector
    # ==========================================================================

    def on_signal(self, signal):
        """
        Entry point. Called from the WebSocket thread.
        Dispatches to a new thread so we don't block candle processing.
        """
        t = threading.Thread(
            target=self._handle_signal,
            args=(signal,),
            daemon=True,
            name=f"trade_{signal.symbol}"
        )
        t.start()

    def _handle_signal(self, signal):
        """Runs in its own thread. Places entry + OCO orders."""

        symbol    = signal.symbol
        direction = signal.direction
        strategy  = signal.strategy

        # ── Global one-trade-at-a-time gate ──────────────────────────────────
        with self._lock:
            if self._global_trade_open:
                log.info(f"[SKIP] {symbol} {strategy}: trade already open globally")
                return
            if symbol in self._open_positions:
                log.info(f"[SKIP] {symbol}: already has open position")
                return
            self._global_trade_open = True   # reserve the slot

        log.info(f"[ORDER] Processing signal: {symbol} {strategy} {direction} "
                 f"entry~{signal.entry_price:.4f}")

        try:
            # ── Step A: Get current price for quantity calculation ────────────
            current_price = self.client.get_ticker_price(symbol)
            prec          = self.precision.get(symbol)

            # Calculate quantity from USDT budget
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

            # ── Step B: Place MARKET entry order ─────────────────────────────
            entry_side = 'BUY' if direction == 'LONG' else 'SELL'
            log.info(f"[ORDER] Placing {entry_side} MARKET {qty} {symbol} @ ~{current_price:.4f}")

            entry_result = self.client.place_market_order(symbol, entry_side, qty)
            entry_order_id = entry_result['orderId']

            # Use fills to get actual average entry price
            fills = entry_result.get('fills', [])
            if fills:
                total_qty  = sum(float(f['qty'])   for f in fills)
                total_cost = sum(float(f['qty']) * float(f['price']) for f in fills)
                actual_entry = total_cost / total_qty if total_qty > 0 else current_price
            else:
                actual_entry = current_price

            log.info(f"[ORDER] Entry filled: {entry_side} {qty} {symbol} @ {actual_entry:.4f} "
                     f"(order #{entry_order_id})")

            # ── Step C: Recalculate SL/TP from actual entry ───────────────────
            sl_pct = signal.sl_price / signal.entry_price   # ratio from signal
            tp_pct = signal.tp_price / signal.entry_price

            sl_raw = actual_entry * sl_pct
            tp_raw = actual_entry * tp_pct

            sl_price = self.precision.round_price(symbol, sl_raw)
            tp_price = self.precision.round_price(symbol, tp_raw)

            # SL limit price: slightly worse than stop trigger to ensure fill
            # For SHORT (stop is above entry): sl_limit = sl * 1.001
            # For LONG  (stop is below entry): sl_limit = sl * 0.999
            if direction == 'LONG':
                sl_limit = self.precision.round_price(symbol, sl_price * 0.999)
            else:
                sl_limit = self.precision.round_price(symbol, sl_price * 1.001)

            # ── Step D: Place OCO order ───────────────────────────────────────
            oco_side = 'SELL' if direction == 'LONG' else 'BUY'

            log.info(f"[ORDER] Placing OCO {oco_side} | TP={tp_price:.4f} SL={sl_price:.4f} "
                     f"SL_limit={sl_limit:.4f}")

            oco_result    = self.client.place_oco_order(
                symbol, oco_side, qty, tp_price, sl_price, sl_limit
            )
            oco_list_id   = oco_result['orderListId']

            log.info(f"[ORDER] OCO placed | listId={oco_list_id} | "
                     f"TP={tp_price:.4f} SL={sl_price:.4f}")

            # ── Step E: Register open position ────────────────────────────────
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

            # Log to CSV
            self._log_trade_open(position)

        except Exception as e:
            log.error(f"[ORDER] Failed to place trade for {symbol}: {e}", exc_info=True)
            with self._lock:
                self._global_trade_open = False

    # ==========================================================================
    # POSITION MONITOR — background thread, polls OCO status
    # ==========================================================================

    def _monitor_loop(self):
        """Polls all open OCO orders every POLL_INTERVAL seconds."""
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
                oco = self.client.get_oco_status(pos.oco_list_id)
                status = oco.get('listStatusType', '')   # ALL_DONE or EXEC_STARTED

                if status != 'ALL_DONE':
                    continue   # still open

                # OCO is done — determine which leg filled (SL or TP)
                outcome = self._determine_outcome(oco, pos)

                log.info(f"[CLOSED] {pos.symbol} {pos.strategy} {pos.direction} | "
                         f"outcome={outcome} | entry={pos.entry_price:.4f} "
                         f"SL={pos.sl_price:.4f} TP={pos.tp_price:.4f}")

                # Log to CSV
                self._log_trade_close(pos, outcome)

                # Remove from open positions and release gate
                with self._lock:
                    self._open_positions.pop(pos.symbol, None)
                    self._global_trade_open = False

                # Notify detector so it updates S2 loss counter and reopens gates
                if self.detector:
                    self.detector.on_trade_closed(pos.symbol, pos.strategy, outcome)

            except Exception as e:
                log.warning(f"Could not check position {pos.symbol} "
                            f"(oco#{pos.oco_list_id}): {e}")

    def _determine_outcome(self, oco_response: dict, pos: OpenPosition) -> str:
        """
        Look at which OCO leg filled to determine WIN (TP) or LOSS (SL).
        """
        orders = oco_response.get('orders', [])
        for order in orders:
            order_detail = self.client.get_order(pos.symbol, order['orderId'])
            if order_detail.get('status') == 'FILLED':
                filled_price = float(order_detail.get('price', 0))
                order_type   = order_detail.get('type', '')

                # LIMIT leg = TP, STOP_LOSS_LIMIT leg = SL
                if order_type == 'LIMIT_MAKER' or order_type == 'LIMIT':
                    return 'WIN'
                else:
                    return 'LOSS'
        return 'UNKNOWN'

    # ==========================================================================
    # CSV TRADE LOG
    # ==========================================================================

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
                 f"entry={pos.entry_price:.4f} SL={pos.sl_price:.4f} TP={pos.tp_price:.4f} "
                 f"qty={pos.quantity}")

    def _log_trade_close(self, pos: OpenPosition, outcome: str):
        close_time = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

        if outcome == 'WIN':
            if pos.direction == 'LONG':
                pnl_pct = (pos.tp_price - pos.entry_price) / pos.entry_price * 100
            else:
                pnl_pct = (pos.entry_price - pos.tp_price) / pos.entry_price * 100
        elif outcome == 'LOSS':
            if pos.direction == 'LONG':
                pnl_pct = (pos.sl_price - pos.entry_price) / pos.entry_price * 100
            else:
                pnl_pct = (pos.entry_price - pos.sl_price) / pos.entry_price * 100
        else:
            pnl_pct = 0.0

        with open(TRADE_LOG_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                pos.open_time, close_time, pos.symbol, pos.strategy,
                pos.direction, f"{pos.entry_price:.6f}",
                f"{pos.sl_price:.6f}", f"{pos.tp_price:.6f}",
                pos.quantity, outcome, f"{pnl_pct:.3f}",
                pos.signal_time, pos.oco_list_id
            ])

        log.info(f"[LOG] Trade closed: {pos.symbol} {outcome} PnL={pnl_pct:+.3f}%")


# ============================================================================
# STANDALONE TEST — verifies API connectivity without placing real orders
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
|                                                      |
|  Tests API key validity and account balance.         |
|  Does NOT place any orders.                          |
+------------------------------------------------------+
""")

    if not API_KEY or not API_SECRET:
        print("ERROR: No API keys found.")
        print("Create a .env file in this folder with:")
        print("  BINANCE_API_KEY=your_key_here")
        print("  BINANCE_SECRET=your_secret_here")
        sys.exit(1)

    client = BinanceClient(API_KEY, API_SECRET, BASE_URL)

    print(f"  Testnet : {TESTNET}")
    print(f"  Base URL: {BASE_URL}")
    print()

    # Test 1: Account balance
    try:
        bal = client.get_usdt_balance()
        print(f"  [OK] USDT Balance    : {bal:.2f} USDT")
    except Exception as e:
        print(f"  [FAIL] Balance fetch : {e}")
        sys.exit(1)

    # Test 2: Symbol info
    try:
        info = client.get_symbol_info('BTCUSDT')
        print(f"  [OK] BTCUSDT info    : status={info.get('status')}")
    except Exception as e:
        print(f"  [FAIL] Symbol info   : {e}")

    # Test 3: Current price
    try:
        price = client.get_ticker_price('BTCUSDT')
        print(f"  [OK] BTCUSDT price   : ${price:.2f}")
    except Exception as e:
        print(f"  [FAIL] Price fetch   : {e}")

    # Test 4: Precision cache
    try:
        pc   = PrecisionCache(client)
        prec = pc.get('BTCUSDT')
        qty  = pc.round_qty('BTCUSDT', TRADE_USDT / price)
        print(f"  [OK] Precision       : qty_step={prec['qty_step']} "
              f"price_step={prec['price_step']}")
        print(f"  [OK] Order qty       : {qty} BTC "
              f"(= ~{qty*price:.2f} USDT for ${TRADE_USDT} budget)")
    except Exception as e:
        print(f"  [FAIL] Precision     : {e}")

    print()
    print("  All connectivity tests passed.")
    print(f"  Trade size is set to {TRADE_USDT} USDT per trade.")
    print()
    print("  To change trade size, edit TRADE_USDT in step3_order_manager.py")
    print("  Run main.py to start the full bot.")