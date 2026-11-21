#!/usr/bin/env python3
"""
orb_live.py — live ORB + S2 ABSORB signals (A+B+C) from Databento MBP-10 -> TradersPost webhook -> Tradeify (Tradovate).

Systems (exactly as backtested; first break per system per day, entries until 11:30 CT, flat 15:00 CT):
  A  OR15, 1m close outside the range, S2 >= 1
  B  OR5,  5m close outside the range, S2 >= 2
  C  OR30, 5m close outside the range, S2 >= 3
  TP = entry + 0.2 x range, SL = opposite side of the range.
  S2 = with-direction aggressive volume traded AT the breakout level in the 120 s before the first print beyond it,
       divided by the average displayed size per level (10 bid + 10 ask levels, per second, 08:30-11:30 CT).
       Backtest used the SAME DAY's average (look-ahead). Live uses the PREVIOUS day's average
       (checked on 69 days of 2026: 98% same keep/skip decisions).
Sizing: max --max MNQ, cut until the full stop fits in --cap of the drawdown room (min 1). One position at a time.

modes:
  --mode paper : signals + would-be orders logged, nothing sent
  --mode alert : paper + sound + macOS notification
  --mode live  : alert + order sent to TradersPost
  --selftest   : replay saved NinjaTrader trades/books (~/cvd-bot/nq_trades, nq_compact) through the same engine
                 and compare with orb_signals_ALL.csv

setup:
  pip3 install databento requests pandas pyarrow
  export DATABENTO_API_KEY=...          (never paste keys in chat or in this file)
  export TP_WEBHOOK='https://webhooks.traderspost.io/trading/webhook/<uuid>/<password>'
usage (start before 08:25 CT = 15:25 Amsterdam, runs until 15:00 CT):
  python3 orb_live.py --mode paper --symbol NQZ6 --ticker MNQZ2026 --room 2000
  python3 orb_live.py --selftest
"""
import argparse, bisect, datetime as dt, json, math, os, subprocess, threading, time
from zoneinfo import ZoneInfo

CT = ZoneInfo("America/Chicago")
SEC, MIN = 1_000_000_000, 60_000_000_000
TICK = 0.25
SYSTEMS = (("A", 15, 1, 1.0), ("B", 5, 5, 2.0), ("C", 30, 5, 3.0))
HOME = os.path.expanduser("~/cvd-bot")
STATE = os.path.join(HOME, "live_state.json")
LOGDIR = os.path.join(HOME, "live_log")

def ct_ns(d, h, m):
    return int(dt.datetime(d.year, d.month, d.day, h, m, tzinfo=CT).timestamp()) * SEC

def hms(ns):
    return dt.datetime.fromtimestamp(ns / 1e9, CT).strftime("%H:%M:%S.%f")[:-3]

# ═════════════════════════ signal engine (pure logic, used live and in selftest) ═════════════════════════
class Engine:
    def __init__(self, day, lvl_avg_prev):
        self.day = day
        self.t_open, self.t_cut = ct_ns(day, 8, 30), ct_ns(day, 11, 30)
        self.lvl_avg_prev = lvl_avg_prev
        self.tt, self.tp, self.tv, self.ts = [], [], [], []      # trades: ns, price ticks, size, side (+1 buy agg, -1 sell agg)
        self.last_px, self.last_dir = None, 1                     # for tick rule when side unknown
        self.min_close = {}                                       # minute index -> last trade price
        self.next_min = 0                                         # next minute index to finalize
        self.orhl = {}                                            # orm -> (H, L)
        self.done = {s[0]: False for s in SYSTEMS}
        self.book_mean, self.book_sum, self.book_n = None, 0.0, 0
        self.next_sec = self.t_open

    # call for every record BEFORE on_trade/on_book of that record
    def clock(self, ts):
        out = []
        # per-second book sampling 08:30-11:30 (state at each whole second)
        while self.next_sec <= ts and self.next_sec < self.t_cut:
            if self.book_mean is not None:
                self.book_sum += self.book_mean; self.book_n += 1
            self.next_sec += SEC
        # finalize finished minutes
        while ts >= self.t_open + (self.next_min + 1) * MIN and self.next_min < 180:
            out += self._minute_done(self.next_min)
            self.next_min += 1
        return out

    def on_book(self, sizes20_mean):
        self.book_mean = sizes20_mean

    def on_trade(self, ts, px, sz, side):
        if side == 0:                                             # tick rule
            if self.last_px is not None and px != self.last_px: self.last_dir = 1 if px > self.last_px else -1
            side = self.last_dir
        elif self.last_px is not None and px != self.last_px:
            self.last_dir = 1 if px > self.last_px else -1
        self.last_px = px
        self.tt.append(ts); self.tp.append(px); self.tv.append(sz); self.ts.append(side)
        if ts >= self.t_open:
            self.min_close[(ts - self.t_open) // MIN] = px

    def today_lvl_avg(self):
        return self.book_sum / self.book_n if self.book_n else None

    def _win(self, a, b):
        return bisect.bisect_left(self.tt, a), bisect.bisect_left(self.tt, b)

    def _minute_done(self, mi):
        sig = []
        for orm in (5, 15, 30):                                   # opening range complete
            if mi == orm - 1:
                a, b = self._win(self.t_open, self.t_open + orm * MIN)
                if b - a >= 50:
                    self.orhl[orm] = (max(self.tp[a:b]), min(self.tp[a:b]))
        c = self.min_close.get(mi)
        for name, orm, etf, th in SYSTEMS:
            if self.done[name] or orm not in self.orhl or mi < orm: continue
            if etf == 5 and (mi + 1) % 5: continue
            if c is None: continue
            H, L = self.orhl[orm]
            if H - L <= 0: self.done[name] = True; continue
            if c > H: dr = 1
            elif c < L: dr = -1
            else: continue
            self.done[name] = True                                # first break only, kept or not
            td = self.t_open + (mi + 1) * MIN
            lvl, oth = (H, L) if dr == 1 else (L, H)
            bar0 = td - (60 if etf == 1 else 300) * SEC
            a, b = self._win(bar0, td)
            t_first = bar0
            for k in range(a, b):
                if (self.tp[k] - lvl) * dr > 0: t_first = self.tt[k]; break
            a2, b2 = self._win(t_first - 120 * SEC, t_first)
            num = sum(self.tv[k] for k in range(a2, b2) if self.tp[k] == lvl and self.ts[k] == dr)
            s2 = num / self.lvl_avg_prev if self.lvl_avg_prev else 0.0
            sig.append(dict(sys=name, orm=orm, etf=etf, dir=dr, td=td, H=H * TICK, L=L * TICK, R=(H - L) * TICK,
                            close=c * TICK, lvl=lvl * TICK, stop=oth * TICK, s2_num=num, s2=s2, th=th, keep=s2 >= th))
        return sig

# ═════════════════════════ order side: sizing, TradersPost, position tracking ═════════════════════════
class Trader:
    def __init__(self, a, log):
        self.a, self.log = a, log
        self.room, self.dp = a.room, 0.0
        self.pos = None                                           # dict while a position is open
        self.http = None
        if a.mode == "live":
            import requests
            self.url = os.environ.get("TP_WEBHOOK", "")
            if not self.url.startswith("https://webhooks.traderspost.io/"):
                raise SystemExit("set TP_WEBHOOK to your TradersPost webhook URL")
            self.http = requests.Session()
            threading.Thread(target=self._keepwarm, daemon=True).start()

    def _keepwarm(self):                                          # keep the TLS connection open so the order goes out instantly
        while True:
            try: self.http.head("https://webhooks.traderspost.io/", timeout=3)
            except Exception: pass
            time.sleep(25)

    def _alert(self, title, msg):
        if self.a.mode == "paper": return
        try:
            subprocess.Popen(["afplay", "/System/Library/Sounds/Glass.aiff"])
            subprocess.Popen(["osascript", "-e", f'display notification "{msg}" with title "{title}" sound name "Hero"'])
        except FileNotFoundError: pass

    def _post(self, payload, tag, t_dec):
        rec = dict(ev="send", tag=tag, payload=payload, t_decide=t_dec)
        if self.a.mode != "live":
            rec["sent"] = False; self.log(rec); return
        def go():
            t0 = time.time_ns()
            try:
                r = self.http.post(self.url, json=payload, timeout=5)
                rec.update(sent=True, status=r.status_code, body=r.text[:300])
            except Exception as ex:
                rec.update(sent=False, error=str(ex))
            rec.update(t_post=t0, ms_decide_to_post=(t0 - t_dec) / 1e6, ms_http=(time.time_ns() - t0) / 1e6)
            self.log(rec)
        threading.Thread(target=go, daemon=True).start()

    def signal(self, s, replay, t_dec, ts_last):
        a = self.a
        base = dict(ev="signal", **s, t_bar=hms(s["td"]), lag_ms_feed=(t_dec - ts_last) / 1e6, replay=replay)
        if not s["keep"]:
            self.log({**base, "action": f"skip: S2 {s['s2']:.2f} < {s['th']}"}); return
        if self.pos is not None:
            self.log({**base, "action": "skip: position already open (one at a time)"}); return
        dr = s["dir"]
        e = s["close"] + 0.5 * dr                                 # backtest entry = next print + 0.5 pt
        tp = e + 0.2 * s["R"] * dr
        tp = math.ceil(tp / TICK) * TICK if dr == 1 else math.floor(tp / TICK) * TICK
        stopd = abs(e - s["stop"]) + 0.25
        room = self.room + self.dp
        k = a.max
        while k > 1 and stopd * 2 * k > a.cap * room: k -= 1
        if room <= 0:
            self.log({**base, "action": "skip: no drawdown room"}); return
        payload = {"ticker": a.ticker, "action": "buy" if dr == 1 else "sell", "quantity": k,
                   "takeProfit": {"limitPrice": round(tp, 2)}, "stopLoss": {"type": "stop", "stopPrice": round(s["stop"], 2)}}
        txt = f"{s['sys']} {'LONG' if dr == 1 else 'SHORT'} {k} MNQ | TP {tp:.2f} | SL {s['stop']:.2f} | S2 {s['s2']:.2f}"
        if replay:
            self.log({**base, "action": "MISSED (replayed history, not sent)", "would_send": payload}); return
        self.pos = dict(dir=dr, k=k, tp=tp, sl=s["stop"], e=None, sys=s["sys"])
        self.log({**base, "action": "ENTER", "order": txt})
        print(f"\n>>> {hms(s['td'])} CT  {txt}\n", flush=True)
        self._alert("ORB " + s["sys"], txt)
        self._post(payload, "entry", t_dec)

    def on_trade(self, ts, px):                                   # track our own position from the feed (approximate)
        p = self.pos
        if p is None: return
        if p["e"] is None: p["e"] = px + 0.5 * p["dir"]; return
        dr = p["dir"]
        if (px - p["sl"]) * dr <= 0: self._close(p["sl"] - 0.25 * dr, "SL")
        elif (px - p["tp"]) * dr >= 0: self._close(p["tp"], "TP")

    def _close(self, px, why):
        p = self.pos
        pnl = (px - p["e"]) * p["dir"] * 2 * p["k"] - p["k"]
        self.dp += pnl; self.pos = None
        self.log(dict(ev="exit", why=why, sys=p["sys"], px=px, pnl_usd=round(pnl, 2), day_pnl=round(self.dp, 2)))
        print(f"<<< {why} {p['sys']} {pnl:+.0f}$  day {self.dp:+.0f}$", flush=True)

    def flat(self, px, t_dec):
        if self.pos is None: return
        self._post({"ticker": self.a.ticker, "action": "exit"}, "flat15", t_dec)
        self._alert("ORB", "15:00 CT flat")
        self._close(px, "15:00 flat")

# ═════════════════════════ live loop ═════════════════════════
def ch(x):
    v = getattr(x, "value", x)
    return chr(v) if isinstance(v, int) else str(v)

def live(a):
    import databento as db
    st = json.load(open(STATE)) if os.path.exists(STATE) else {}
    day = dt.datetime.now(CT).date()
    lvl_prev = a.lvl_avg or st.get("lvl_avg")
    if not lvl_prev: raise SystemExit("no previous-day level average yet: pass --lvl-avg (2026 median 4.41) on the first day")
    if a.room is None: a.room = st.get("room", 2000.0)
    os.makedirs(LOGDIR, exist_ok=True)
    lf = open(os.path.join(LOGDIR, f"{day}.jsonl"), "a")
    def log(r):
        r = {"t": time.time_ns(), **r}; lf.write(json.dumps(r, default=str) + "\n"); lf.flush()
        if r.get("ev") in ("signal",): print(f"[{hms(r['td'])}] {r['sys']} dir {r['dir']:+d} S2 {r['s2']:.2f} -> {r['action']}", flush=True)
    eng, tr = Engine(day, lvl_prev), Trader(a, log)
    t_flat, t_save = ct_ns(day, 15, 0), False
    print(f"{day} | mode {a.mode} | {a.symbol} -> {a.ticker} | prev level avg {lvl_prev:.2f} | room ${a.room:.0f} | max {a.max} MNQ", flush=True)
    cl = db.Live(key=os.environ["DATABENTO_API_KEY"], reconnect_policy="reconnect")
    t820 = dt.datetime(day.year, day.month, day.day, 8, 20, tzinfo=CT)
    kw = dict(start=t820.isoformat()) if dt.datetime.now(CT) > t820 else {}   # started late: replay from 08:20 CT (no orders on replayed bars)
    cl.subscribe(dataset="GLBX.MDP3", schema="mbp-10", stype_in="raw_symbol", symbols=[a.symbol], **kw)
    U = 9223372036854775807
    n = 0
    for r in cl:
        if not hasattr(r, "levels"): continue
        ts = r.ts_event
        now = time.time_ns()
        replay = now - r.ts_recv > 3 * SEC
        for s in eng.clock(ts):
            tr.signal(s, replay, now, ts)
        lv = r.levels
        eng.on_book(sum(l.bid_sz + l.ask_sz for l in lv) / 20.0)
        if ch(r.action) == "T" and r.price != U:
            sd = ch(r.side)
            px = int(round(r.price / 1e9 / TICK))
            eng.on_trade(ts, px, r.size, 1 if sd == "B" else -1 if sd == "A" else 0)
            tr.on_trade(ts, px * TICK)
        if not t_save and ts >= eng.t_cut:
            t_save = True
            st.update(lvl_avg=eng.today_lvl_avg(), lvl_avg_date=str(day)); json.dump(st, open(STATE, "w"))
            log(dict(ev="lvl_avg_saved", value=st["lvl_avg"]))
        if ts >= t_flat:
            if eng.last_px is not None: tr.flat(eng.last_px * TICK, now)
            st.update(room_end_of_day_hint=round(tr.room + tr.dp, 2)); json.dump(st, open(STATE, "w"))
            print(f"15:00 CT done. day P&L (feed-estimated) {tr.dp:+.0f}$", flush=True); time.sleep(2); break
        n += 1
        if n % 200000 == 0: print(f"  {hms(ts)} CT  records {n:,}  last {eng.last_px * TICK if eng.last_px else 0:.2f}", flush=True)

# ═════════════════════════ selftest on saved NinjaTrader data ═════════════════════════
def selftest(a):
    import glob, numpy as np, pandas as pd
    E = pd.read_csv(os.path.expanduser(a.events), parse_dates=["date"]).drop_duplicates(["date", "orm", "etf"])
    files = sorted(glob.glob(os.path.join(os.path.expanduser(a.trades), "NQ_*_trades.parquet")))
    cols = [f"{s}s{i}" for i in range(10) for s in "ba"]
    rows, prev = [], None
    for f in files:
        ds = os.path.basename(f)[3:11]; day = dt.datetime.strptime(ds, "%Y%m%d").date()
        T = pd.read_parquet(f)
        cf = os.path.join(os.path.expanduser(a.compact), f"NQ_{ds}.parquet")
        if not os.path.exists(cf): continue
        Cb = pd.read_parquet(cf); Cb = Cb[Cb[cols].notna().any(axis=1)]
        bt, bm = (Cb.t.values.astype(np.int64) * SEC), Cb[cols].fillna(0).values.mean(1)
        t0 = ct_ns(day, 8, 25)
        full = bm[(bt >= ct_ns(day, 8, 30)) & (bt < ct_ns(day, 11, 30))].mean()
        eng = Engine(day, 1.0)                                    # one pass; S2 normalised afterwards
        tt = t0 + T.ms.values.astype(np.int64) * 1_000_000
        px, sz, sd = T.px.values.tolist(), T.sz.values.tolist(), T.side.values.tolist()
        got = []
        for i, t in enumerate(tt.tolist()):
            got += eng.clock(t)
            eng.on_trade(t, px[i], sz[i], sd[i])
        got += eng.clock(ct_ns(day, 11, 31))
        for norm, la in (("same-day", full), ("prev-day", prev)):
            if la is None: continue
            for s in got:
                s2 = s["s2_num"] / la
                rows.append(dict(date=pd.Timestamp(day), norm=norm, **{**s, "s2": s2, "keep": s2 >= s["th"]}))
        prev = full
    L = pd.DataFrame(rows)
    if L.empty: raise SystemExit("no days found")
    M = L.merge(E, on=["date", "orm", "etf"], how="left", suffixes=("", "_bt"))
    for norm, g in M.groupby("norm"):
        bt_keep = g.S2_ABSORB >= g.th
        print(f"{norm:9s}: {len(g)} signals on {g.date.nunique()} days | in backtest {g.dir_bt.notna().mean()*100:5.1f}% | "
              f"same direction {(g.dir == g.dir_bt).mean()*100:5.1f}% | same entry minute {(g.td == g.td_bt).mean()*100:5.1f}% | "
              f"same keep/skip {(g.keep == bt_keep).mean()*100:5.1f}% | kept {int(g.keep.sum())} vs backtest {int(bt_keep.sum())}")
        k = g[g.keep]
        print(f"           kept trades backtest result: WR {(k.pts > 0).mean()*100:5.1f}%  avg {k.pts.mean():+5.1f} pt  sum {k.pts.sum():+7.1f} pt")
    bad = M[(M.norm == "same-day") & ((M.dir != M.dir_bt) | (M.td != M.td_bt) | (M.keep != (M.S2_ABSORB >= M.th)))]
    if len(bad): print("\nmismatches (same-day):\n" + bad[["date", "sys", "dir", "dir_bt", "s2", "S2_ABSORB"]].to_string(index=False))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=("paper", "alert", "live"), default="paper")
    ap.add_argument("--symbol", default="NQZ6", help="Databento raw symbol of the NQ front month")
    ap.add_argument("--ticker", default="MNQZ2026", help="TradersPost ticker for MNQ front month")
    ap.add_argument("--room", type=float, default=None, help="$ above your drawdown limit right now")
    ap.add_argument("--max", type=int, default=12)
    ap.add_argument("--cap", type=float, default=0.6)
    ap.add_argument("--lvl-avg", type=float, default=None)
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--events", default="~/cvd-bot/orb_signals_ALL.csv")
    ap.add_argument("--trades", default="~/cvd-bot/nq_trades")
    ap.add_argument("--compact", default="~/cvd-bot/nq_compact")
    a = ap.parse_args()
    selftest(a) if a.selftest else live(a)
