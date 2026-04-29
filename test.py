"""
Quick 5-hour backtest — runs exact same S1 + S2 signal logic
against recent Binance Futures candles to check if any signals
should have fired in the last 5 hours.
"""

import requests
import time
from datetime import datetime, timezone

# ── Symbols (same as live bot) ────────────────────────────────────────────────
SYMBOLS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","XRPUSDT","SOLUSDT",
    "TRXUSDT","DOGEUSDT","ADAUSDT","BCHUSDT","LINKUSDT",
    "XMRUSDT","XLMUSDT","AVAXUSDT","LTCUSDT","HBARUSDT",
    "SUIUSDT","ZECUSDT","1000SHIBUSDT","TONUSDT","TAOUSDT",
    "DOTUSDT","UNIUSDT","AAVEUSDT","ASTERUSDT","NEARUSDT",
    "1000PEPEUSDT","SKYUSDT","ETCUSDT","ONDOUSDT","WLDUSDT",
    "POLUSDT","ENAUSDT","RENDERUSDT","ATOMUSDT","TRUMPUSDT",
    "KASUSDT","ALGOUSDT","QNTUSDT","APTUSDT","FILUSDT",
    "ZROUSDT","VETUSDT","ARBUSDT","JUPUSDT","1000BONKUSDT",
    "PENGUUSDT","CAKEUSDT","VIRTUALUSDT","FETUSDT","STXUSDT",
    "SEIUSDT","DASHUSDT","ETHFIUSDT","XTZUSDT","CHZUSDT",
    "CRVUSDT","IMXUSDT","TIAUSDT","INJUSDT","SYRUPUSDT",
    "1000FLOKIUSDT","JASMYUSDT","PYTHUSDT","GRTUSDT","IOTAUSDT",
    "OPUSDT","LDOUSDT","SANDUSDT","ENSUSDT","BARDUSDT",
    "1000LUNCUSDT","STRKUSDT","TWTUSDT","RUNEUSDT","HYPEUSDT",
]

# ── Parameters ────────────────────────────────────────────────────────────────
S1_ADX_MIN     = 25.0
S1_SL_PCT      = 0.5
S1_TP_PCT      = 1.5
S2_SL_PCT      = 2.0
S2_TP_PCT      = 6.0
S2_DIST_A_MIN  = 0.0020; S2_DIST_A_MAX = 0.0035
S2_DIST_B_MIN  = 0.0050; S2_DIST_B_MAX = 0.0065
S2_MIN_WICK    = 0.0035; S2_MAX_WICK   = 0.0100
S2_BODY_MIN    = 0.60
S2_SLOPE_MIN   = 0.10
S2_ATR_MAX     = 0.60
MA44_P         = 44
LOOKBACK_BARS  = 300   # warmup + 5h of 15m = 20 bars, need 300 for indicators

REST = "https://fapi.binance.com/fapi/v1/klines"

# ── Indicator helpers ─────────────────────────────────────────────────────────
def ema(closes, p):
    n = len(closes); out = [None]*n
    if n < p: return out
    k = 2/(p+1); out[p-1] = sum(closes[:p])/p
    for i in range(p, n):
        out[i] = closes[i]*k + out[i-1]*(1-k)
    return out

def sma(closes, p, i):
    if i < p-1: return None
    return sum(closes[i-p+1:i+1])/p

def atr_wilder(highs, lows, closes, p):
    n = len(closes); out = [None]*n
    tr = [0.0]*n
    for i in range(1,n):
        tr[i] = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
    out[p] = sum(tr[1:p+1])/p
    for i in range(p+1,n):
        out[i] = (out[i-1]*(p-1)+tr[i])/p
    return out

def adx_wilder(highs, lows, closes, p):
    n = len(closes)
    tr=[0.0]*n; dmp=[0.0]*n; dmn=[0.0]*n
    for i in range(1,n):
        h,l,pc = highs[i],lows[i],closes[i-1]
        tr[i]=max(h-l,abs(h-pc),abs(l-pc))
        up=highs[i]-highs[i-1]; dn=lows[i-1]-lows[i]
        if up>dn and up>0: dmp[i]=up
        if dn>up and dn>0: dmn[i]=dn
    str_=[0.0]*n; sdp=[0.0]*n; sdn=[0.0]*n
    str_[p]=sum(tr[1:p+1]); sdp[p]=sum(dmp[1:p+1]); sdn[p]=sum(dmn[1:p+1])
    for i in range(p+1,n):
        str_[i]=str_[i-1]-str_[i-1]/p+tr[i]
        sdp[i]=sdp[i-1]-sdp[i-1]/p+dmp[i]
        sdn[i]=sdn[i-1]-sdn[i-1]/p+dmn[i]
    dx=[None]*n; dip=[None]*n; din_=[None]*n
    for i in range(p,n):
        if str_[i]==0: continue
        dp=100*sdp[i]/str_[i]; dn2=100*sdn[i]/str_[i]
        dip[i]=dp; din_[i]=dn2
        denom=dp+dn2
        dx[i]=0 if denom==0 else 100*abs(dp-dn2)/denom
    fv=next((i for i in range(n) if dx[i] is not None),None)
    adx_=[None]*n
    if fv is not None:
        se=fv+p
        if se<=n:
            sv=[dx[i] for i in range(fv,se) if dx[i] is not None]
            if len(sv)==p:
                adx_[se-1]=sum(sv)/p
                for i in range(se,n):
                    if dx[i] is not None and adx_[i-1] is not None:
                        adx_[i]=(adx_[i-1]*(p-1)+dx[i])/p
    return adx_, dip, din_

def macd_ind(closes):
    n=len(closes)
    e12=ema(closes,12); e26=ema(closes,26)
    ml=[None]*n
    for i in range(n):
        if e12[i] and e26[i]: ml[i]=e12[i]-e26[i]
    fv=next((i for i,v in enumerate(ml) if v is not None),None)
    sig=[None]*n; hist=[None]*n
    if fv is not None:
        se=fv+9
        if se<=n:
            vals=[ml[i] for i in range(fv,se) if ml[i] is not None]
            if len(vals)==9:
                k=2/10; sig[se-1]=sum(vals)/9
                for i in range(se,n):
                    if ml[i] and sig[i-1]: sig[i]=ml[i]*k+sig[i-1]*(1-k)
                for i in range(n):
                    if ml[i] and sig[i]: hist[i]=ml[i]-sig[i]
    return ml,sig,hist

def monotonic_falling(closes, i, p=44, lb=8):
    if i < p+lb-1: return False
    ma_vals=[sum(closes[i-lb-p+1+k:i-lb+1+k])/p for k in range(lb+1)]
    return all(ma_vals[j]>ma_vals[j+1] for j in range(lb))

# ── Fetch candles ─────────────────────────────────────────────────────────────
def fetch(symbol):
    try:
        r = requests.get(REST, params={'symbol':symbol,'interval':'15m','limit':LOOKBACK_BARS}, timeout=15)
        if r.status_code != 200: return None
        data = r.json()
        if not isinstance(data,list): return None
        return data[:-1]  # exclude open candle
    except: return None

# ── Main scan ─────────────────────────────────────────────────────────────────
now_utc = datetime.now(timezone.utc)
# 5 hours ago in ms
cutoff_ms = int((time.time() - 5*3600) * 1000)

all_signals = []
skipped = []

print(f"Scanning {len(SYMBOLS)} symbols | last 5h | cutoff {datetime.fromtimestamp(cutoff_ms/1000,tz=timezone.utc).strftime('%H:%M UTC')}")
print("="*70)

for sym in SYMBOLS:
    data = fetch(sym)
    if not data or len(data) < 250:
        skipped.append(sym)
        print(f"  SKIP {sym}: not enough data")
        time.sleep(0.05)
        continue

    closes = [float(c[4]) for c in data]
    opens  = [float(c[1]) for c in data]
    highs  = [float(c[2]) for c in data]
    lows   = [float(c[3]) for c in data]
    times  = [int(c[0])   for c in data]
    n      = len(closes)

    e9  = ema(closes,9)
    e26 = ema(closes,26)
    e200= ema(closes,200)
    ml,sig,hist = macd_ind(closes)
    adx_s,dip_s,din_s = adx_wilder(highs,lows,closes,14)
    atr_s = atr_wilder(highs,lows,closes,14)

    sym_signals = []

    # ── S1 scan ───────────────────────────────────────────────────────────────
    used = set()
    for i in range(201, n-1):
        if times[i] < cutoff_ms: continue
        if None in (e9[i],e26[i],e200[i],e9[i-1],e26[i-1],adx_s[i],dip_s[i],din_s[i],ml[i],sig[i],hist[i]): continue
        bull = (e9[i-1]<=e26[i-1]) and (e9[i]>e26[i])
        bear = (e9[i-1]>=e26[i-1]) and (e9[i]<e26[i])
        if not (bull or bear): continue
        if i in used: continue
        direction = 'LONG' if bull else 'SHORT'
        for j in (i, i+1):
            if j>=n: break
            if None in (e9[j],e26[j],e200[j],adx_s[j],dip_s[j],din_s[j],ml[j],sig[j],hist[j]): continue
            c,o = closes[j],opens[j]
            if direction=='LONG':
                if not(c>o and c>e9[j] and c>e26[j]): continue
                if c<=e200[j]: continue
                if not(dip_s[j]>din_s[j]): continue
                if not(ml[j]>sig[j] and hist[j]>0): continue
            else:
                if not(c<o and c<e9[j] and c<e26[j]): continue
                if c>=e200[j]: continue
                if not(din_s[j]>dip_s[j]): continue
                if not(ml[j]<sig[j] and hist[j]<0): continue
            if adx_s[j]<=S1_ADX_MIN: continue
            ts_str = datetime.fromtimestamp(times[j]/1000,tz=timezone.utc).strftime('%H:%M UTC')
            sym_signals.append(f"  S1 {direction} {sym} @ {ts_str}  entry={closes[j]:.5f}  ADX={adx_s[j]:.1f}")
            used.add(i)
            break

    # ── S2 scan ───────────────────────────────────────────────────────────────
    pending = False
    for i in range(MA44_P+20, n):
        if times[i] < cutoff_ms - 15*60*1000: pass  # need setup before cutoff too

        ma44 = sma(closes, MA44_P, i)
        if ma44 is None: continue

        # Step 2 — trigger
        if pending:
            if opens[i] < ma44 and times[i] >= cutoff_ms:
                ts_str = datetime.fromtimestamp(times[i]/1000,tz=timezone.utc).strftime('%H:%M UTC')
                sym_signals.append(f"  S2 SHORT {sym} @ {ts_str}  entry={opens[i]:.5f}  MA44={ma44:.5f}")
            pending = False
            continue

        if times[i] < cutoff_ms - 15*60*1000: continue  # setup must be in window

        # Step 1 — setup
        if closes[i] >= opens[i]: continue  # must be bearish
        if atr_s[i] is None: continue

        ma44_8ago = sma(closes, MA44_P, i-8)
        if ma44_8ago is None: continue
        slope = (ma44-ma44_8ago)/ma44*100 if ma44>0 else 0
        if abs(slope) < S2_SLOPE_MIN: continue
        if slope >= 0: continue
        if not monotonic_falling(closes, i): continue

        ma44_4ago = sma(closes, MA44_P, i-4)
        ma44_8ago2= sma(closes, MA44_P, i-8)
        if ma44_4ago is None: continue
        sr = ma44-ma44_4ago; sp = ma44_4ago-ma44_8ago2
        if not(sr<0 and sp<0 and sr<sp): continue

        body_top = max(opens[i],closes[i])
        body_bot = min(opens[i],closes[i])
        c_size   = highs[i]-lows[i]
        if c_size==0: continue
        body_r   = (body_top-body_bot)/c_size
        wick_pct = c_size/highs[i] if highs[i]>0 else 0
        if body_r < S2_BODY_MIN: continue
        if wick_pct < S2_MIN_WICK or wick_pct > S2_MAX_WICK: continue
        if body_top >= ma44: continue
        dist = (ma44-body_top)/ma44
        if not((S2_DIST_A_MIN<=dist<=S2_DIST_A_MAX) or (S2_DIST_B_MIN<=dist<=S2_DIST_B_MAX)): continue
        atr_pct = atr_s[i]/closes[i]*100 if closes[i]>0 else 99
        if atr_pct >= S2_ATR_MAX: continue

        pending = True  # next candle is trigger

    if sym_signals:
        all_signals.extend(sym_signals)
    else:
        print(f"  {sym}: no signals")
    time.sleep(0.08)

print()
print("="*70)
if all_signals:
    print(f"SIGNALS FOUND ({len(all_signals)}):")
    for s in all_signals:
        print(s)
else:
    print("NO SIGNALS in the last 5 hours across all 75 symbols.")
    print("The strategies genuinely found nothing in this window.")
print("="*70)