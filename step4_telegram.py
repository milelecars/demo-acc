"""
STEP 4 OF 4 — Telegram Alerts + Daily P&L Summary
===================================================
Sends Telegram messages for:
  - Every signal detected (with entry, SL, TP)
  - Every trade closed (WIN or LOSS, with P&L)
  - Daily summary at 00:00 UTC (total trades, win rate, P&L)
  - Bot startup confirmation
  - Any critical errors

Setup:
  Add to your .env file:
    TELEGRAM_TOKEN=your_bot_token_here
    TELEGRAM_CHAT_ID=your_chat_id_here

Dependencies:
  pip install requests python-dotenv  (already installed)
"""

import os
import sys
import io
import csv
import time
import logging
import threading
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

# Windows UTF-8 fix
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except AttributeError:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

load_dotenv()

log = logging.getLogger('telegram')

# ============================================================================
# CONFIGURATION
# ============================================================================

TELEGRAM_TOKEN   = os.getenv('TELEGRAM_TOKEN',   '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
TRADE_LOG_FILE   = 'trade_log.csv'

# Daily summary time: 00:00 UTC
DAILY_SUMMARY_HOUR   = 0
DAILY_SUMMARY_MINUTE = 0

# ============================================================================
# TELEGRAM SENDER
# ============================================================================

class TelegramBot:
    """
    Thin wrapper around the Telegram sendMessage API.
    All sends are non-blocking (dispatched to a thread).
    Failed sends are logged but never crash the bot.
    """

    def __init__(self):
        self.token   = TELEGRAM_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.base    = f"https://api.telegram.org/bot{self.token}"
        self._queue  = []
        self._lock   = threading.Lock()

        if not self.token or not self.chat_id:
            log.warning("Telegram not configured — add TELEGRAM_TOKEN and "
                        "TELEGRAM_CHAT_ID to your .env file")
            self.enabled = False
        else:
            self.enabled = True
            log.info("Telegram alerts enabled")

    def send(self, text: str):
        """Send a message. Non-blocking — fires and forgets."""
        if not self.enabled:
            return
        t = threading.Thread(target=self._send_sync, args=(text,), daemon=True)
        t.start()

    def _send_sync(self, text: str):
        try:
            resp = requests.post(
                f"{self.base}/sendMessage",
                json={
                    'chat_id':    self.chat_id,
                    'text':       text,
                    'parse_mode': 'HTML',
                },
                timeout=10
            )
            if resp.status_code != 200:
                log.warning(f"Telegram send failed: {resp.status_code} {resp.text[:100]}")
        except Exception as e:
            log.warning(f"Telegram error: {e}")

    def test(self) -> bool:
        """Send a test message. Returns True if successful."""
        if not self.enabled:
            return False
        try:
            resp = requests.post(
                f"{self.base}/sendMessage",
                json={
                    'chat_id':    self.chat_id,
                    'text':       '✅ Bot connected successfully!',
                    'parse_mode': 'HTML',
                },
                timeout=10
            )
            return resp.status_code == 200
        except Exception as e:
            log.error(f"Telegram test failed: {e}")
            return False


# ============================================================================
# ALERT FORMATTER
# ============================================================================

class AlertManager:
    """
    Formats and sends all bot alerts via Telegram.
    Plug into the bot by setting callbacks on detector and order manager.

    Usage:
        alerts = AlertManager()
        alerts.send_startup(len(SYMBOLS))

        # Wire into detector signal:
        original_on_signal = manager.on_signal
        def signal_with_alert(sig):
            alerts.on_signal(sig)
            original_on_signal(sig)
        detector.on_signal = signal_with_alert

        # Wire into order manager close:
        alerts.set_order_manager(manager)
    """

    def __init__(self):
        self.bot = TelegramBot()
        self._daily_thread = threading.Thread(
            target=self._daily_summary_loop,
            daemon=True,
            name='daily_summary'
        )
        self._daily_thread.start()

    # ── Startup ───────────────────────────────────────────────────────────────

    def send_startup(self, symbol_count: int, testnet: bool = True):
        mode = "TESTNET (paper money)" if testnet else "LIVE"
        msg = (
            f"<b>Bot Started</b>\n"
            f"Mode: {mode}\n"
            f"Symbols: {symbol_count}\n"
            f"Strategies: EMA Cross + MA44 Bounce\n"
            f"Time: {_now()}"
        )
        self.bot.send(msg)
        log.info("Startup alert sent")

    # ── Signal detected ───────────────────────────────────────────────────────

    def on_signal(self, signal):
        """Called when a new signal is detected (before order is placed)."""
        direction_emoji = "LONG" if signal.direction == 'LONG' else "SHORT"

        if signal.strategy == 'S1_EMA_CROSS':
            strategy_label = "EMA 9/26 Cross"
        else:
            strategy_label = "MA44 Bounce"

        sl_pct = abs(signal.sl_price - signal.entry_price) / signal.entry_price * 100
        tp_pct = abs(signal.tp_price - signal.entry_price) / signal.entry_price * 100

        msg = (
            f"<b>Signal: {direction_emoji} {signal.symbol}</b>\n"
            f"Strategy : {strategy_label}\n"
            f"Entry    : {signal.entry_price:.4f}\n"
            f"SL       : {signal.sl_price:.4f}  (-{sl_pct:.2f}%)\n"
            f"TP       : {signal.tp_price:.4f}  (+{tp_pct:.2f}%)\n"
            f"Time     : {signal.signal_time}"
        )
        self.bot.send(msg)
        log.info(f"Signal alert sent: {signal.symbol} {signal.direction}")

    # ── Trade opened ──────────────────────────────────────────────────────────

    def on_trade_opened(self, symbol: str, strategy: str, direction: str,
                        entry: float, sl: float, tp: float, qty: float):
        direction_emoji = "LONG" if direction == 'LONG' else "SHORT"
        sl_pct = abs(sl - entry) / entry * 100
        tp_pct = abs(tp - entry) / entry * 100

        msg = (
            f"<b>Trade Opened: {direction_emoji} {symbol}</b>\n"
            f"Strategy : {strategy}\n"
            f"Entry    : {entry:.4f}\n"
            f"Qty      : {qty}\n"
            f"SL       : {sl:.4f}  ({sl_pct:.2f}%)\n"
            f"TP       : {tp:.4f}  ({tp_pct:.2f}%)\n"
            f"Time     : {_now()}"
        )
        self.bot.send(msg)

    # ── Trade closed ──────────────────────────────────────────────────────────

    def on_trade_closed(self, symbol: str, strategy: str, direction: str,
                        entry: float, exit_price: float, outcome: str):
        if outcome == 'WIN':
            emoji  = "WIN"
            header = f"<b>Trade WIN: {symbol}</b>"
        elif outcome == 'LOSS':
            emoji  = "LOSS"
            header = f"<b>Trade LOSS: {symbol}</b>"
        else:
            emoji  = "??"
            header = f"<b>Trade Closed: {symbol}</b>"

        if direction == 'LONG':
            pnl_pct = (exit_price - entry) / entry * 100
        else:
            pnl_pct = (entry - exit_price) / entry * 100

        pnl_str = f"{pnl_pct:+.2f}%"

        msg = (
            f"{header}\n"
            f"Strategy : {strategy}\n"
            f"Direction: {direction}\n"
            f"Entry    : {entry:.4f}\n"
            f"Exit     : {exit_price:.4f}\n"
            f"P&L      : {pnl_str}\n"
            f"Time     : {_now()}"
        )
        self.bot.send(msg)
        log.info(f"Close alert sent: {symbol} {outcome} {pnl_str}")

    # ── Error alert ───────────────────────────────────────────────────────────

    def on_error(self, message: str):
        msg = f"<b>Bot Error</b>\n{message}\nTime: {_now()}"
        self.bot.send(msg)

    # ── Daily summary ─────────────────────────────────────────────────────────

    def _daily_summary_loop(self):
        """Runs forever, sends a summary at 00:00 UTC each day."""
        log.info("Daily summary thread started")
        while True:
            now = datetime.now(tz=timezone.utc)
            # Seconds until next 00:00 UTC
            next_midnight = now.replace(
                hour=DAILY_SUMMARY_HOUR,
                minute=DAILY_SUMMARY_MINUTE,
                second=5,
                microsecond=0
            )
            if next_midnight <= now:
                # Already past midnight today — aim for tomorrow
                from datetime import timedelta
                next_midnight += timedelta(days=1)

            wait_sec = (next_midnight - now).total_seconds()
            log.info(f"Daily summary scheduled in {wait_sec/3600:.1f}h "
                     f"({next_midnight.strftime('%Y-%m-%d %H:%M UTC')})")
            time.sleep(wait_sec)

            try:
                self._send_daily_summary()
            except Exception as e:
                log.error(f"Daily summary error: {e}", exc_info=True)

    def _send_daily_summary(self):
        """Reads trade_log.csv and summarises today's trades."""
        today = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d')
        # Yesterday's date (summary runs at 00:00 so we summarise the day that just ended)
        from datetime import timedelta
        yesterday = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')

        trades = []
        try:
            with open(TRADE_LOG_FILE, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('close_time', '').startswith(yesterday):
                        trades.append(row)
        except FileNotFoundError:
            log.info("No trade log found yet — skipping daily summary")
            return

        if not trades:
            msg = (
                f"<b>Daily Summary — {yesterday}</b>\n"
                f"No trades closed yesterday."
            )
            self.bot.send(msg)
            return

        total   = len(trades)
        wins    = sum(1 for t in trades if t['outcome'] == 'WIN')
        losses  = sum(1 for t in trades if t['outcome'] == 'LOSS')
        wr      = wins / total * 100 if total > 0 else 0

        try:
            total_pnl = sum(float(t['pnl_pct']) for t in trades)
        except (ValueError, KeyError):
            total_pnl = 0.0

        # Strategy breakdown
        s1_trades = [t for t in trades if 'EMA' in t.get('strategy', '')]
        s2_trades = [t for t in trades if 'MA44' in t.get('strategy', '')]

        s1_w = sum(1 for t in s1_trades if t['outcome'] == 'WIN')
        s2_w = sum(1 for t in s2_trades if t['outcome'] == 'WIN')

        msg = (
            f"<b>Daily Summary — {yesterday}</b>\n"
            f"\n"
            f"Total trades : {total}\n"
            f"Wins         : {wins}\n"
            f"Losses       : {losses}\n"
            f"Win rate     : {wr:.1f}%\n"
            f"Total P&L    : {total_pnl:+.2f}%\n"
            f"\n"
            f"EMA Cross  : {len(s1_trades)} trades  W:{s1_w} L:{len(s1_trades)-s1_w}\n"
            f"MA44 Bounce: {len(s2_trades)} trades  W:{s2_w} L:{len(s2_trades)-s2_w}"
        )
        self.bot.send(msg)
        log.info(f"Daily summary sent: {total} trades, WR={wr:.1f}%, PnL={total_pnl:+.2f}%")


# ============================================================================
# HELPER
# ============================================================================

def _now() -> str:
    return datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')


# ============================================================================
# STANDALONE TEST
# ============================================================================

if __name__ == '__main__':
    logging.basicConfig(
        level   = logging.INFO,
        format  = '%(asctime)s  %(levelname)-7s  %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler('bot.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout),
        ]
    )

    print("""
+------------------------------------------------------+
|  STEP 4 -- Telegram Alerts  (connectivity test)      |
|                                                      |
|  Sends a test message to your Telegram bot.          |
+------------------------------------------------------+
""")

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("ERROR: Telegram not configured.")
        print("Add to your .env file:")
        print("  TELEGRAM_TOKEN=your_bot_token")
        print("  TELEGRAM_CHAT_ID=your_chat_id")
        sys.exit(1)

    bot = TelegramBot()

    print(f"  Token   : {TELEGRAM_TOKEN[:10]}...")
    print(f"  Chat ID : {TELEGRAM_CHAT_ID}")
    print()

    if bot.test():
        print("  [OK] Test message sent! Check your Telegram.")
    else:
        print("  [FAIL] Could not send message. Check token and chat ID.")
        sys.exit(1)

    # Also send a sample signal alert
    from step2_signal_detector import SignalEvent
    alerts = AlertManager()

    sample = SignalEvent(
        strategy    = 'S1_EMA_CROSS',
        symbol      = 'BTCUSDT',
        direction   = 'SHORT',
        entry_price = 83000.0,
        sl_price    = 83415.0,
        tp_price    = 81170.0,
        signal_ts   = int(time.time() * 1000),
        signal_time = _now(),
        reason      = 'EMA9/26 SHORT cross | ADX=31.2',
        indicators  = {}
    )
    alerts.on_signal(sample)
    time.sleep(1)
    alerts.on_trade_opened('BTCUSDT', 'S1_EMA_CROSS', 'SHORT',
                           83000.0, 83415.0, 81170.0, 0.00024)
    time.sleep(1)
    alerts.on_trade_closed('BTCUSDT', 'S1_EMA_CROSS', 'SHORT',
                           83000.0, 81170.0, 'WIN')
    time.sleep(2)
    print("  [OK] Sample alerts sent. Check your Telegram.")