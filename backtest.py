"""
MILELE PRIME — Strategy Backtest
Start: 2026-04-23 06:56 UTC (new deployment go-live)
"""
import math, csv, time, requests, sys, io
from datetime import datetime, timezone
from collections import deque

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except AttributeError:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# ── CONFIG ───────────────────────────────────────────────────────────────────
START_TS_MS   = 1777326300000   # 2026-04-27 21:45 UTC = 2026-04-28 01:45 UAE (UTC+4)
REST_BASE     = "https://fapi.binance.com/fapi"
INTERVAL      = "15m"
CANDLE_LIMIT  = 1500
SEED_CANDLES  = 300

EMA_FAST, EMA_SLOW, EMA_TREND = 9, 26, 200
MACD_FAST, MACD_SLOW, MACD_SIG = 12, 26, 9
ADX_PERIOD = 14
ATR_PERIOD = 14
MA44_PERIOD = 44

S1_ADX_MIN = 25.0
S1_SL_PCT  = 0.5
S1_TP_PCT  = 1.5
S1_MARGIN  = 20.0

S2_SL_PCT      = 2.0
S2_TP_PCT      = 6.0
S2_MARGIN      = 16.65
S2_MIN_BODY    = 0.60
S2_DIST_A      = (0.0020, 0.0035)
S2_DIST_B      = (0.0050, 0.0065)
S2_MIN_WICK    = 0.0035
S2_MAX_WICK    = 0.0100
S2_SLOPE_MIN   = 0.10
S2_ATR_MAX     = 0.60
S2_COOLDOWN_S  = 4 * 3600

H4_MA_PERIOD  = 44
H4_SLOPE_BARS = 4

SYMBOLS = [
    "BTCUSDT","ETHUSDT","XRPUSDT","TRXUSDT","ADAUSDT",
    "ZECUSDT","DOTUSDT","VETUSDT","FETUSDT","SEIUSDT",
    "DASHUSDT","SYRUPUSDT","ENSUSDT","BARDUSDT","TWTUSDT",
]

# ── INDICATORS ────────────────────────────────────────────────────────────────
def ema_series(values, period):
    n = len(values); out = [None]*n
    if n < period: return out
    k = 2.0/(period+1)
    out[period-1] = sum(values[:period])/period
    for i in range(period,n):
        out[i] = values[i]*k + out[i-1]*(1-k)
    return out

def compute_indicators(candles):
    if len(candles) < EMA_TREND+10: return None
    closes=[c['c'] for c in candles]; highs=[c['h'] for c in candles]; lows=[c['l'] for c in candles]
    n=len(candles)
    e9_s=ema_series(closes,EMA_FAST); e26_s=ema_series(closes,EMA_SLOW); e200_s=ema_series(closes,EMA_TREND)
    ef=ema_series(closes,MACD_FAST); es=ema_series(closes,MACD_SLOW)
    ml=[ef[i]-es[i] if (ef[i] is not None and es[i] is not None) else None for i in range(n)]
    ml_c=[v if v is not None else 0.0 for v in ml]
    sig_s=ema_series(ml_c,MACD_SIG)
    p=ADX_PERIOD
    tr_r=[0.]*n; dm_p=[0.]*n; dm_n=[0.]*n
    for i in range(1,n):
        h,l,pc=highs[i],lows[i],closes[i-1]
        tr_r[i]=max(h-l,abs(h-pc),abs(l-pc))
        up=highs[i]-highs[i-1]; dn=lows[i-1]-lows[i]
        if up>dn and up>0: dm_p[i]=up
        if dn>up and dn>0: dm_n[i]=dn
    s_tr=[0.]*n; s_dp=[0.]*n; s_dn=[0.]*n
    if n>p:
        s_tr[p]=sum(tr_r[1:p+1]); s_dp[p]=sum(dm_p[1:p+1]); s_dn[p]=sum(dm_n[1:p+1])
        for i in range(p+1,n):
            s_tr[i]=s_tr[i-1]-s_tr[i-1]/p+tr_r[i]
            s_dp[i]=s_dp[i-1]-s_dp[i-1]/p+dm_p[i]
            s_dn[i]=s_dn[i-1]-s_dn[i-1]/p+dm_n[i]
    dx_s=[None]*n; dip_s=[None]*n; din_s=[None]*n
    for i in range(p,n):
        av=s_tr[i]
        if av==0: continue
        dip=100.*s_dp[i]/av; din=100.*s_dn[i]/av
        dip_s[i]=dip; din_s[i]=din
        denom=dip+din
        dx_s[i]=0. if denom==0 else 100.*abs(dip-din)/denom
    first_dx=next((i for i in range(n) if dx_s[i] is not None),None)
    adx_s=[None]*n
    if first_dx is not None:
        se=first_dx+p
        if se<=n:
            sv=[dx_s[i] for i in range(first_dx,se) if dx_s[i] is not None]
            if len(sv)==p:
                adx_s[se-1]=sum(sv)/p
                for i in range(se,n):
                    if dx_s[i] is not None and adx_s[i-1] is not None:
                        adx_s[i]=(adx_s[i-1]*(p-1)+dx_s[i])/p
    tr2=[0.]*n
    for i in range(1,n):
        tr2[i]=max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1]))
    atr_s=[None]*n
    if n>ATR_PERIOD:
        atr_s[ATR_PERIOD]=sum(tr2[1:ATR_PERIOD+1])/ATR_PERIOD
        for i in range(ATR_PERIOD+1,n):
            atr_s[i]=(atr_s[i-1]*(ATR_PERIOD-1)+tr2[i])/ATR_PERIOD
    ma44_val=ma44_slope=ma44_accel=None
    if n>=MA44_PERIOD+8:
        ma44_val  =sum(closes[-MA44_PERIOD:])/MA44_PERIOD
        ma44_4ago =sum(closes[-MA44_PERIOD-4:-4])/MA44_PERIOD
        ma44_8ago =sum(closes[-MA44_PERIOD-8:-8])/MA44_PERIOD
        if ma44_val>0: ma44_slope=(ma44_val-ma44_8ago)/ma44_val*100
        ma44_accel=(ma44_val-ma44_4ago)-(ma44_4ago-ma44_8ago)
    atr=atr_s[-1]
    atr_pct=(atr/closes[-1]*100) if (atr and closes[-1]>0) else None
    return {
        'ema9':e9_s[-1],'ema26':e26_s[-1],'ema200':e200_s[-1],
        'ema9_prev':e9_s[-2] if n>1 else None,'ema26_prev':e26_s[-2] if n>1 else None,
        'macd':ml[-1],'macd_sig':sig_s[-1],
        'macd_hist':(ml[-1]-sig_s[-1]) if (ml[-1] is not None and sig_s[-1] is not None) else None,
        'adx':adx_s[-1],'di_plus':dip_s[-1],'di_minus':din_s[-1],
        'ma44':ma44_val,'ma44_slope_8bar':ma44_slope,'ma44_accel':ma44_accel,
        'atr':atr,'atr_pct':atr_pct,
    }

def check_ma44_monotonic(candles, period=MA44_PERIOD, lookback=8):
    if len(candles)<period+lookback: return False
    closes=[c['c'] for c in candles]; n=len(closes)
    ma_vals=[]
    for offset in range(lookback+1):
        idx=n-1-(lookback-offset)
        if idx<period-1: return False
        ma_vals.append(sum(closes[idx-period+1:idx+1])/period)
    return all(ma_vals[i]>ma_vals[i+1] for i in range(len(ma_vals)-1))

# ── REST HELPERS ──────────────────────────────────────────────────────────────
def fetch_candles_page(symbol, start_ms, limit=CANDLE_LIMIT):
    try:
        r=requests.get(f"{REST_BASE}/v1/klines",params={
            'symbol':symbol,'interval':INTERVAL,'startTime':start_ms,'limit':limit},timeout=15)
        if r.status_code!=200: return []
        return [{'t':int(x[0]),'o':float(x[1]),'h':float(x[2]),'l':float(x[3]),'c':float(x[4])} for x in r.json()]
    except Exception as e:
        print(f"  [WARN] {symbol}: {e}"); return []

def fetch_all_candles(symbol, start_ms):
    result=[]; cur=start_ms; now_ms=int(time.time()*1000)
    while cur<now_ms:
        batch=fetch_candles_page(symbol,cur)
        if not batch: break
        result.extend(batch)
        if len(batch)<CANDLE_LIMIT: break
        cur=batch[-1]['t']+1
        time.sleep(0.05)
    now_15m=(now_ms//(15*60*1000))*(15*60*1000)
    return [c for c in result if c['t']<now_15m]

_h4_cache={}
def fetch_h4_direction(symbol, candle_ts_ms):
    bucket=(candle_ts_ms//(4*3600*1000))*(4*3600*1000)
    key=(symbol,bucket)
    if key in _h4_cache: return _h4_cache[key]
    try:
        r=requests.get(f"{REST_BASE}/v1/klines",params={
            'symbol':symbol,'interval':'4h','endTime':candle_ts_ms,
            'limit':H4_MA_PERIOD+H4_SLOPE_BARS+5},timeout=10)
        result=None
        if r.status_code==200:
            data=r.json()
            if len(data)>=H4_MA_PERIOD+H4_SLOPE_BARS:
                c4=[float(x[4]) for x in data]
                ma_now=sum(c4[-H4_MA_PERIOD:])/H4_MA_PERIOD
                ma_prev=sum(c4[-H4_MA_PERIOD-H4_SLOPE_BARS:-H4_SLOPE_BARS])/H4_MA_PERIOD
                result=ma_now>ma_prev
    except: result=None
    _h4_cache[key]=result
    return result

# ── SIMULATOR ─────────────────────────────────────────────────────────────────
def simulate_outcome(direction, entry, sl, tp, forward_candles):
    for i,c in enumerate(forward_candles):
        if direction=='LONG':
            if c['l']<=sl: return('LOSS',sl,i+1,c['t'])
            if c['h']>=tp: return('WIN',tp,i+1,c['t'])
        else:
            if c['h']>=sl: return('LOSS',sl,i+1,c['t'])
            if c['l']<=tp: return('WIN',tp,i+1,c['t'])
    return('OPEN',None,len(forward_candles),None)

def fmt_ts(ms):
    return datetime.fromtimestamp(ms/1000,tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

# ── BACKTEST ENGINE ───────────────────────────────────────────────────────────
def backtest_symbol(symbol, all_candles):
    signals=[]
    warm=[c for c in all_candles if c['t']<START_TS_MS]
    live=[c for c in all_candles if c['t']>=START_TS_MS]
    if len(warm)<EMA_TREND+10 or not live: return signals
    buf=deque(warm[-500:],maxlen=500)

    s1_pending_dir=None; s1_pending_ts=None; s1_open=False; s1_close_after=None
    s2_pending=False; s2_setup_ts=None; s2_setup_snap={}
    s2_open=False; s2_close_after=None; s2_last_sig_sec=-S2_COOLDOWN_S

    for idx,candle in enumerate(live):
        buf.append(candle)
        buf_list=list(buf)
        ind=compute_indicators(buf_list)
        if ind is None: continue
        required=['ema9','ema26','ema200','ema9_prev','ema26_prev','adx','di_plus','di_minus',
                  'macd','macd_sig','macd_hist','ma44','ma44_slope_8bar','ma44_accel','atr_pct']
        if any(ind.get(k) is None for k in required): continue

        ts=candle['t']; c_close=candle['c']; c_open=candle['o']
        c_high=candle['h']; c_low=candle['l']; now_sec=ts/1000.0

        if s1_open and s1_close_after is not None and idx>s1_close_after:
            s1_open=False; s1_close_after=None
        if s2_open and s2_close_after is not None and idx>s2_close_after:
            s2_open=False; s2_close_after=None

        # ── S1 ────────────────────────────────────────────────────────────────
        if not s1_open:
            e9=ind['ema9']; e9p=ind['ema9_prev']; e26=ind['ema26']; e26p=ind['ema26_prev']
            e200=ind['ema200']; adx=ind['adx']; dip=ind['di_plus']; din=ind['di_minus']
            mac=ind['macd']; macs=ind['macd_sig']; mach=ind['macd_hist']
            bull_x=(e9p<=e26p)and(e9>e26); bear_x=(e9p>=e26p)and(e9<e26)
            if bull_x or bear_x:
                s1_pending_dir='LONG' if bull_x else 'SHORT'; s1_pending_ts=ts
            if s1_pending_dir is not None:
                d=s1_pending_dir
                if ts>s1_pending_ts+2*15*60*1000:
                    s1_pending_dir=None; s1_pending_ts=None
                else:
                    f2=(c_close>c_open and c_close>e9 and c_close>e26) if d=='LONG' else (c_close<c_open and c_close<e9 and c_close<e26)
                    if f2 and d=='LONG'  and c_close<=e200: f2=False
                    if f2 and d=='SHORT' and c_close>=e200: f2=False
                    if f2 and adx<=S1_ADX_MIN: f2=False
                    if f2 and d=='LONG'  and not(dip>din): f2=False
                    if f2 and d=='SHORT' and not(din>dip): f2=False
                    if f2 and d=='LONG'  and not(mac>macs and mach>0): f2=False
                    if f2 and d=='SHORT' and not(mac<macs and mach<0): f2=False
                    if f2:
                        if idx+1>=len(live):
                            s1_pending_dir=None; s1_pending_ts=None; continue
                        next_c=live[idx+1]; entry=next_c['o']
                        sl_raw=entry*(1-S1_SL_PCT/100) if d=='LONG' else entry*(1+S1_SL_PCT/100)
                        tp_raw=entry*(1+S1_TP_PCT/100) if d=='LONG' else entry*(1-S1_TP_PCT/100)
                        pd=len(str(entry).rstrip('0').split('.')[-1]) if '.' in str(entry) else 0
                        sl=round(sl_raw,pd); tp=round(tp_raw,pd); notional=S1_MARGIN*50
                        candle_label='N' if ts==s1_pending_ts else 'N+1'
                        forward=live[idx+2:]
                        outcome,exit_p,bars,exit_ts=simulate_outcome(d,entry,sl,tp,forward)
                        pnl_pct=pnl=None
                        if exit_p is not None:
                            mult=1 if d=='LONG' else -1
                            pnl_pct=round(mult*(exit_p-entry)/entry*100,3)
                            pnl=round(notional*pnl_pct/100,2)
                        signals.append({'strategy':'S1_EMA_CROSS','symbol':symbol,'direction':d,
                            'signal_time':fmt_ts(next_c['t']),'entry':round(entry,8),
                            'sl':round(sl,8),'tp':round(tp,8),'margin':S1_MARGIN,'notional':notional,
                            'candle':candle_label,'adx':round(adx,1),'outcome':outcome,
                            'exit_price':round(exit_p,8) if exit_p else '','exit_time':fmt_ts(exit_ts) if exit_ts else '',
                            'bars_held':bars,'pnl_pct':pnl_pct,'pnl_usdt':pnl,
                            's2_zone':'','s2_dist':'','s2_slope':'','s2_h4':'','s2_atr':''})
                        s1_pending_dir=None; s1_pending_ts=None
                        s1_open=True; s1_close_after=idx+1+bars

        # ── S2 ────────────────────────────────────────────────────────────────
        if s2_pending:
            ma44=ind['ma44']; triggered=False
            if c_open<ma44 and not s2_open:
                if (now_sec-s2_last_sig_sec)>=S2_COOLDOWN_S:
                    if idx+1<len(live):
                        entry=live[idx+1]['o']
                        sl_raw=entry*(1+S2_SL_PCT/100); tp_raw=entry*(1-S2_TP_PCT/100)
                        pd=len(str(entry).rstrip('0').split('.')[-1]) if '.' in str(entry) else 0
                        sl=round(sl_raw,pd); tp=round(tp_raw,pd); notional=S2_MARGIN*15
                        snap=s2_setup_snap
                        forward=live[idx+1:]
                        outcome,exit_p,bars,exit_ts=simulate_outcome('SHORT',entry,sl,tp,forward)
                        pnl_pct=pnl=None
                        if exit_p is not None:
                            pnl_pct=round((entry-exit_p)/entry*100,3)
                            pnl=round(notional*pnl_pct/100,2)
                        signals.append({'strategy':'S2_MA44_BOUNCE','symbol':symbol,'direction':'SHORT',
                            'signal_time':fmt_ts(live[idx+1]['t']),'entry':round(entry,8),
                            'sl':round(sl,8),'tp':round(tp,8),'margin':S2_MARGIN,'notional':notional,
                            'candle':'TRIGGER','adx':round(ind.get('adx') or 0,1),'outcome':outcome,
                            'exit_price':round(exit_p,8) if exit_p else '','exit_time':fmt_ts(exit_ts) if exit_ts else '',
                            'bars_held':bars,'pnl_pct':pnl_pct,'pnl_usdt':pnl,
                            's2_zone':snap.get('zone',''),'s2_dist':round(snap.get('dist_pct',0),3),
                            's2_slope':round(snap.get('slope_8bar',0),3),'s2_h4':snap.get('h4_dir',''),
                            's2_atr':round(snap.get('atr_pct',0),3)})
                        s2_last_sig_sec=now_sec; s2_open=True; s2_close_after=idx+1+bars; triggered=True
            s2_pending=False; s2_setup_ts=None; s2_setup_snap={}
            if triggered: continue

        if s2_open: continue
        if c_close>=c_open: continue

        slope_8bar=ind['ma44_slope_8bar']; ma44_accel=ind['ma44_accel']
        atr_pct=ind['atr_pct']; ma44=ind['ma44']
        if abs(slope_8bar)<S2_SLOPE_MIN: continue
        if not check_ma44_monotonic(buf_list): continue
        if slope_8bar>=0 or ma44_accel>=0: continue
        body_top=max(c_open,c_close); body_bottom=min(c_open,c_close)
        candle_rng=c_high-c_low; body_size=body_top-body_bottom
        if candle_rng==0: continue
        wick_pct=candle_rng/c_high if c_high>0 else 0
        body_ratio=body_size/candle_rng
        if body_ratio<S2_MIN_BODY: continue
        if wick_pct<S2_MIN_WICK or wick_pct>S2_MAX_WICK: continue
        if body_top>=ma44: continue
        dist_pct=(ma44-body_top)/ma44
        in_a=S2_DIST_A[0]<=dist_pct<=S2_DIST_A[1]; in_b=S2_DIST_B[0]<=dist_pct<=S2_DIST_B[1]
        if not(in_a or in_b): continue
        if atr_pct>=S2_ATR_MAX: continue
        h4_rising=fetch_h4_direction(symbol,ts)
        if h4_rising is True: continue
        s2_pending=True; s2_setup_ts=ts
        s2_setup_snap={'zone':'A' if in_a else 'B','dist_pct':dist_pct*100,
            'slope_8bar':slope_8bar,'ma44_accel':ma44_accel,'atr_pct':atr_pct,
            'h4_dir':'FALLING' if h4_rising is False else 'UNKNOWN'}

    return signals

# ── STATS + OUTPUT ────────────────────────────────────────────────────────────
def stats(sigs):
    closed=[s for s in sigs if s['outcome']!='OPEN']
    wins=[s for s in closed if s['outcome']=='WIN']
    losses=[s for s in closed if s['outcome']=='LOSS']
    opens=[s for s in sigs if s['outcome']=='OPEN']
    pnls=[s['pnl_usdt'] for s in closed if s['pnl_usdt'] is not None]
    gross=sum(pnls) if pnls else 0
    avg_w=(sum(s['pnl_usdt'] for s in wins if s['pnl_usdt'])/len(wins)) if wins else 0
    avg_l=(sum(s['pnl_usdt'] for s in losses if s['pnl_usdt'])/len(losses)) if losses else 0
    wr=len(wins)/len(closed)*100 if closed else 0
    return {'total':len(sigs),'closed':len(closed),'wins':len(wins),'losses':len(losses),
            'open':len(opens),'win_rate':wr,'gross_pnl':gross,'avg_win':avg_w,'avg_loss':avg_l}

def main():
    print("="*70)
    print("  MILELE PRIME — STRATEGY BACKTEST")
    print(f"  Start  : {fmt_ts(START_TS_MS)}")
    print(f"  End    : {fmt_ts(int(time.time()*1000))}")
    print(f"  Symbols: {len(SYMBOLS)}")
    print("="*70)

    all_signals=[]; warm_start=START_TS_MS-(SEED_CANDLES*15*60*1000)

    for i,sym in enumerate(SYMBOLS):
        print(f"  [{i+1:2d}/{len(SYMBOLS)}] {sym:<18}",end='',flush=True)
        candles=fetch_all_candles(sym,warm_start)
        live_n=sum(1 for c in candles if c['t']>=START_TS_MS)
        if len(candles)<EMA_TREND+50 or live_n==0:
            print(f"  SKIP ({len(candles)} candles)"); continue
        sigs=backtest_symbol(sym,candles)
        wins=sum(1 for s in sigs if s['outcome']=='WIN')
        losses=sum(1 for s in sigs if s['outcome']=='LOSS')
        opens=sum(1 for s in sigs if s['outcome']=='OPEN')
        pnl=sum(s['pnl_usdt'] for s in sigs if s['pnl_usdt'] is not None)
        print(f"  {live_n} candles | {len(sigs):3d} signals ({wins}W/{losses}L/{opens}O) PnL ${pnl:+.2f}")
        all_signals.extend(sigs)
        time.sleep(0.08)

    fieldnames=['strategy','symbol','direction','signal_time','candle','entry','sl','tp',
                'margin','notional','adx','outcome','exit_price','exit_time','bars_held',
                'pnl_pct','pnl_usdt','s2_zone','s2_dist','s2_slope','s2_h4','s2_atr']
    with open('backtest_report.csv','w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=fieldnames,extrasaction='ignore')
        w.writeheader()
        for s in sorted(all_signals,key=lambda x:x['signal_time']): w.writerow(s)

    ov=stats(all_signals)
    s1=stats([s for s in all_signals if s['strategy']=='S1_EMA_CROSS'])
    s2=stats([s for s in all_signals if s['strategy']=='S2_MA44_BOUNCE'])

    lines=["="*80,"  MILELE PRIME — BACKTEST REPORT",
        f"  Period  : {fmt_ts(START_TS_MS)}  →  {fmt_ts(int(time.time()*1000))}",
        f"  Symbols : {len(SYMBOLS)}","="*80,"",
        "  OVERALL",
        f"    Total signals  : {ov['total']}",
        f"    Closed         : {ov['closed']}   ({ov['wins']}W / {ov['losses']}L)",
        f"    Still open     : {ov['open']}",
        f"    Win rate       : {ov['win_rate']:.1f}%",
        f"    Gross P&L      : ${ov['gross_pnl']:+,.2f}","",
        "  S1  EMA 9/26 Cross  ($20 × 50x = $1,000 notional, SL 0.5% / TP 1.5%)",
        f"    Signals        : {s1['total']}",
        f"    Closed         : {s1['closed']}   ({s1['wins']}W / {s1['losses']}L)",
        f"    Win rate       : {s1['win_rate']:.1f}%",
        f"    Gross P&L      : ${s1['gross_pnl']:+,.2f}",
        f"    Avg win        : ${s1['avg_win']:+.2f}",
        f"    Avg loss       : ${s1['avg_loss']:+.2f}","",
        "  S2  MA44 Bounce SHORT  ($16.65 × 15x = $249.75 notional, SL 2.0% / TP 6.0%)",
        f"    Signals        : {s2['total']}",
        f"    Closed         : {s2['closed']}   ({s2['wins']}W / {s2['losses']}L)",
        f"    Win rate       : {s2['win_rate']:.1f}%",
        f"    Gross P&L      : ${s2['gross_pnl']:+,.2f}",
        f"    Avg win        : ${s2['avg_win']:+.2f}",
        f"    Avg loss       : ${s2['avg_loss']:+.2f}"]

    if all_signals:
        top10=sorted([s for s in all_signals if s['pnl_usdt'] is not None],key=lambda x:x['pnl_usdt'],reverse=True)[:10]
        bot10=sorted([s for s in all_signals if s['pnl_usdt'] is not None],key=lambda x:x['pnl_usdt'])[:10]
        lines+=["","  TOP 10 BY P&L"]
        for s in top10:
            lines.append(f"    {s['symbol']:<16} {s['strategy']:<16} {s['direction']:<6} {s['outcome']:<5}  ${s['pnl_usdt']:>+9.2f}  {s['signal_time']}")
        lines+=["","  WORST 10 BY P&L"]
        for s in bot10:
            lines.append(f"    {s['symbol']:<16} {s['strategy']:<16} {s['direction']:<6} {s['outcome']:<5}  ${s['pnl_usdt']:>+9.2f}  {s['signal_time']}")

    lines+=["","="*80,"  PER-SYMBOL BREAKDOWN","="*80]
    for sym in sorted(set(s['symbol'] for s in all_signals)):
        st=stats([s for s in all_signals if s['symbol']==sym])
        lines.append(f"  {sym:<16}  {st['total']:3d} signals  {st['wins']}W/{st['losses']}L/{st['open']}O  WR {st['win_rate']:5.1f}%  P&L ${st['gross_pnl']:+,.2f}")

    lines+=["","="*80,"  SIGNAL DETAIL  (chronological)","="*80,""]
    prev_sym=None
    for n,s in enumerate(sorted(all_signals,key=lambda x:x['signal_time']),1):
        if s['symbol']!=prev_sym:
            lines.append(f"  {'─'*76}")
            lines.append(f"  {s['symbol']}")
            lines.append(f"  {'─'*76}")
            prev_sym=s['symbol']
        marker='✓' if s['outcome']=='WIN' else '✗' if s['outcome']=='LOSS' else '○'
        pnl_str=f"${s['pnl_usdt']:+.2f}" if s['pnl_usdt'] is not None else "OPEN"
        pnl_pct_str=f"({s['pnl_pct']:+.3f}%)" if s['pnl_pct'] is not None else ""
        lines.append(f"  #{n:<4}  {marker}  {s['strategy']:<16}  {s['direction']:<6}  {s['signal_time']}")
        lines.append(f"         Entry {s['entry']:<14}  SL {s['sl']:<14}  TP {s['tp']}")
        if s['strategy']=='S1_EMA_CROSS':
            lines.append(f"         ADX={s['adx']}  Candle={s['candle']}  Margin=${s['margin']} × 50x")
        else:
            lines.append(f"         Zone={s['s2_zone']}  Dist={s['s2_dist']}%  Slope={s['s2_slope']}%  H4={s['s2_h4']}  ATR={s['s2_atr']}%")
        if s['outcome']=='OPEN':
            lines.append(f"         Outcome  OPEN  ({s['bars_held']} bars so far)")
        else:
            lines.append(f"         Outcome  {s['outcome']:<5}  Exit {s['exit_price']}  {s['exit_time']}  {s['bars_held']} bars  {pnl_str} {pnl_pct_str}")
        lines.append("")

    lines+=["="*80]
    report="\n".join(lines)
    with open('backtest_summary.txt','w',encoding='utf-8') as f:
        f.write(report)
    print(f"\n  Report → backtest_summary.txt  ({len(all_signals)} signals)")
    print("\n"+report[:4000])

if __name__=='__main__':
    main()