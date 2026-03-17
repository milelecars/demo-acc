"""
MAIN — Full Bot Entry Point (Steps 1-4)
========================================
Wires together:
  Step 1 — CandleEngine     (WebSocket + indicators)
  Step 2 — SignalDetector   (all strategy filters)
  Step 3 — OrderManager     (Binance Testnet orders)
  Step 4 — AlertManager     (Telegram notifications)

Run:
  python main.py

.env file must contain:
  BINANCE_API_KEY=...
  BINANCE_API_SECRET=...
  TESTNET=true
  INTERVAL=15m
  TELEGRAM_TOKEN=...
  TELEGRAM_CHAT_ID=...
"""

import sys
import io
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# Windows UTF-8 fix
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except AttributeError:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Logging setup
logging.basicConfig(
    level   = logging.INFO,
    format  = '%(asctime)s  %(levelname)-7s  %(name)-16s  %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S',
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

detector = None
engine   = None
manager  = None
alerts   = None


def candle_callback(symbol: str, candle: dict, indicators: dict):
    candle_list = engine.store.get_list(symbol)
    detector.set_candle_list(symbol, candle_list)
    detector.on_candle_close(symbol, candle, indicators)


def on_signal_with_alert(signal: SignalEvent):
    if alerts:
        alerts.on_signal(signal)
    if manager:
        manager.on_signal(signal)


def main():
    global detector, engine, manager, alerts

    print(f"""
+------------------------------------------------------+
|  DUAL STRATEGY BOT  --  Full Run                     |
|                                                      |
|  Strategy 1 : EMA 9/26 Cross + 6 Filters             |
|  Strategy 2 : MA44 Bounce (SHORT only)               |
|  Symbols    : {len(SYMBOLS)} coins                              |
|  Timeframe  : 15m candles                            |
|  Mode       : {'TESTNET (paper money)' if TESTNET else 'LIVE'}                    |
|  Started    : {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}              |
+------------------------------------------------------+
""")

    # Step 4: Telegram
    alerts = AlertManager()

    # Step 2: Signal detector
    detector = SignalDetector()

    # Step 3: Order manager
    try:
        manager = OrderManager(detector=detector)
    except ValueError as e:
        log.error(str(e))
        sys.exit(1)

    # Patch order manager to send Telegram on trade open
    _orig_open = manager._log_trade_open
    def _patched_open(pos):
        _orig_open(pos)
        if alerts:
            alerts.on_trade_opened(
                pos.symbol, pos.strategy, pos.direction,
                pos.entry_price, pos.sl_price, pos.tp_price, pos.quantity
            )
    manager._log_trade_open = _patched_open

    # Patch order manager to send Telegram on trade close
    _orig_close = manager._log_trade_close
    def _patched_close(pos, outcome):
        _orig_close(pos, outcome)
        if alerts:
            exit_price = pos.tp_price if outcome == 'WIN' else pos.sl_price
            alerts.on_trade_closed(
                pos.symbol, pos.strategy, pos.direction,
                pos.entry_price, exit_price, outcome
            )
    manager._log_trade_close = _patched_close

    # Wire signals: detector -> alert + order
    detector.on_signal = on_signal_with_alert

    # Step 1: Candle engine
    engine = CandleEngine(SYMBOLS, callback=candle_callback)

    # Startup Telegram alert
    alerts.send_startup(len(SYMBOLS), TESTNET)

    log.info("All components ready. Starting candle engine...")
    log.info(f"Watching {len(SYMBOLS)} symbols | Signals appear here and on Telegram")

    try:
        engine.start()
    except KeyboardInterrupt:
        log.info("Bot stopped by user.")
        if alerts:
            alerts.on_error("Bot stopped by user (KeyboardInterrupt)")


if __name__ == '__main__':
    main()