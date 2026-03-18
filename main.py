"""
MAIN — Full Bot Entry Point
============================
"""
import sys, io, logging, os, time, hmac, hashlib, requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS

load_dotenv()

# ── Flask proxy app ──────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)   # ← AFTER app is created, not before

BINANCE_BASE = 'https://testnet.binance.vision/api'
API_KEY    = os.environ.get('BINANCE_API_KEY', '')
API_SECRET = os.environ.get('BINANCE_API_SECRET', '')

def binance_signed(path, params={}):
    p = dict(params)
    p['timestamp'] = int(time.time() * 1000)
    qs  = '&'.join(f'{k}={v}' for k, v in p.items())
    sig = hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f'{BINANCE_BASE}{path}?{qs}&signature={sig}'
    return requests.get(url, headers={'X-MBX-APIKEY': API_KEY}).json()

@app.route('/proxy/v3/account')
def proxy_v3_account():
    return jsonify(binance_signed('/v3/account'))

@app.route('/proxy/v3/openOrders')
def proxy_v3_open_orders():
    return jsonify(binance_signed('/v3/openOrders'))

@app.route('/proxy/v3/allOrders')
def proxy_v3_all_orders():
    sym = request.args.get('symbol', '')
    return jsonify(binance_signed('/v3/allOrders', {'symbol': sym, 'limit': 100}))

# ── NO @app.after_request cors function — flask_cors handles it ──────────────

# ── Windows UTF-8 fix ────────────────────────────────────────────────────────
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except AttributeError:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level    = logging.INFO,
    format   = '%(asctime)s  %(levelname)-7s  %(name)-16s  %(message)s',
    datefmt  = '%Y-%m-%d %H:%M:%S',
    handlers = [
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger('main')

from step1_candle_engine   import CandleEngine, SYMBOLS, TESTNET
from step2_signal_detector import SignalDetector, SignalEvent
from step3_order_manager   import OrderManager
from step4_telegram        import AlertManager

detector = engine = manager = alerts = None

def candle_callback(symbol, candle, indicators):
    candle_list = engine.store.get_list(symbol)
    detector.set_candle_list(symbol, candle_list)
    detector.on_candle_close(symbol, candle, indicators)

def on_signal_with_alert(signal):
    if alerts:  alerts.on_signal(signal)
    if manager: manager.on_signal(signal)

def main():
    global detector, engine, manager, alerts

    print(f"""
+------------------------------------------------------+
|  DUAL STRATEGY BOT  --  Full Run                     |
|  Strategy 1 : EMA 9/26 Cross + 6 Filters             |
|  Strategy 2 : MA44 Bounce (SHORT only)               |
|  Symbols    : {len(SYMBOLS)} coins                              |
|  Timeframe  : 15m candles                            |
|  Mode       : {'TESTNET (paper money)' if TESTNET else 'LIVE'}                    |
|  Started    : {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}              |
+------------------------------------------------------+
""")

    alerts   = AlertManager()
    detector = SignalDetector()

    try:
        manager = OrderManager(detector=detector)
    except ValueError as e:
        log.error(str(e)); sys.exit(1)

    _orig_open = manager._log_trade_open
    def _patched_open(pos):
        _orig_open(pos)
        if alerts:
            alerts.on_trade_opened(pos.symbol, pos.strategy, pos.direction,
                                   pos.entry_price, pos.sl_price, pos.tp_price, pos.quantity)
    manager._log_trade_open = _patched_open

    _orig_close = manager._log_trade_close
    def _patched_close(pos, outcome):
        _orig_close(pos, outcome)
        if alerts:
            exit_price = pos.tp_price if outcome == 'WIN' else pos.sl_price
            alerts.on_trade_closed(pos.symbol, pos.strategy, pos.direction,
                                   pos.entry_price, exit_price, outcome)
    manager._log_trade_close = _patched_close

    detector.on_signal = on_signal_with_alert
    engine = CandleEngine(SYMBOLS, callback=candle_callback)
    alerts.send_startup(len(SYMBOLS), TESTNET)

    log.info("All components ready. Starting candle engine...")

    try:
        engine.start()
    except KeyboardInterrupt:
        log.info("Bot stopped by user.")
        if alerts: alerts.on_error("Bot stopped by user (KeyboardInterrupt)")

if __name__ == '__main__':
    import threading
    t = threading.Thread(target=main, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
