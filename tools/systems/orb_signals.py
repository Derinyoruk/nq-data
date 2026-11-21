#!/usr/bin/env python3
"""
orb_signals.py — 12 order-flow / order-book signals x 3 logical thresholds on ORB breakouts, computed from the FULL
NinjaTrader replay stream (every L2 add/update/remove + every trade), not snapshots. Runs on all .nrd days, parallel.
Needs nrd_compact.py in the same folder (decoder + book rebuild).

ORB events: OR 5/15/30 min from 08:30 CT, entry on first 1m or 5m close outside OR (before 11:30), TP 0.2 x OR,
SL = opposite OR side, flat 15:00, simulated on trade prints (+0.5pt entry slip, 0.25pt stop slip, exits -0.5pt).
Orientation: everything relative to the trade direction ('with' = breakout side aggressor, 'opp' = resting liquidity in front).

PRE-ENTRY FILTERS (known at the decision bar close)
 S1  PULL     resting liquidity at the breakout level that vanished WITHOUT trading (share of its peak, 60s)    -> SKIP if >= t
 S2  ABSORB   with-direction aggression absorbed AT the level before it broke (x avg level size)                -> KEEP if >= t
 S3  ICEBERG  volume traded at the level / peak displayed size there (>1 = hidden size refilling, 60s)          -> SKIP if >= t
 S4  STACK    own-side top-5 depth / opposing top-5 depth at entry (support behind)                              -> KEEP if >= t
 S5  WALL     biggest opposing level between entry and target (x avg level size)                                 -> SKIP if >= t
 S6  DIVERG   5-min delta into the break (with - against)/volume (delta divergence)                              -> SKIP if <  t
 S7  BURST    with-direction aggressive volume in the break bar (x avg 1-min volume)                             -> KEEP if >= t
 S8  BIGLOT   share of break-bar with-direction volume from orders >= 10 lots                                    -> KEEP if >= t
 S9  TRAPPED  counter-direction aggression of last 5 min now underwater at entry (fuel, x avg 1-min volume)      -> KEEP if >= t
IN-TRADE EXITS
 S10 RELOAD   opposing 10-level depth at +30s vs at entry (sellers reloading ahead)                              -> EXIT @+30s if >= t
 S11 EXHAUST  with-direction aggression per tick of progress, first 2 min (x avg 1-min vol)                      -> EXIT @+120s if >= t
 S12 RETEST   first retest of the level: counter aggression AT the level in 30s / own resting size at the level  -> EXIT @retest+30s if >= t and price behind level
MAE (mae_nt) = deepest move against the position while open, from the replay's own trade prints.
Each rule judged on FIT days (first 60%) and TEST days (last 40%) separately. Events + all signal values saved to CSV.
usage: python3 orb_signals.py --nrd-dir ~/Downloads
"""
import argparse, datetime as dt, glob, os, time
from multiprocessing import Pool, cpu_count
import numpy as np, pandas as pd
from numba import njit
from nrd_compact import load, book_snaps, ct_ns

SLIP, SSLIP, TPM = 0.5, 0.25, 0.2
SKIP_DAYS = {"20260703", "20260907"}

@njit(cache=True)
def level_track(ts, sd, op, pos, px, vol, q_side, q_price, q_t0, q_t1):
    """for each query: size changes of resting liquidity at one price on one side within [t0,t1).
    returns dec (size removed), inc (size added), refills (increases while that price is best), maxsize"""
    nq = len(q_side)
    P = np.zeros((2, 12), np.int64); S = np.zeros((2, 12), np.int64)
    dec = np.zeros(nq); inc = np.zeros(nq); ref = np.zeros(nq); mx = np.zeros(nq)
    before = np.zeros(nq)
    tlo = q_t0.min(); thi = q_t1.max()
    for i in range(len(ts)):
        s = sd[i]; p = pos[i]
        if p > 10: continue
        if ts[i] > thi: break
        # only events that touch the queried price itself (ignore levels sliding in/out of the 10-level window)
        active = ts[i] >= tlo and p < 9
        for j in range(nq):
            before[j] = 0.0
            if not active: continue
            if q_side[j] == s and px[i] == q_price[j] and ts[i] >= q_t0[j] and ts[i] < q_t1[j]:
                for q in range(12):
                    if P[s, q] == q_price[j] and S[s, q] > 0: before[j] += S[s, q]
        if op[i] == 0:
            for q in range(11, p, -1): P[s, q] = P[s, q - 1]; S[s, q] = S[s, q - 1]
            P[s, p] = px[i]; S[s, p] = vol[i]
        elif op[i] == 1:
            P[s, p] = px[i]; S[s, p] = vol[i]
        else:
            for q in range(p, 11): P[s, q] = P[s, q + 1]; S[s, q] = S[s, q + 1]
            P[s, 11] = 0; S[s, 11] = 0
        if not active: continue
        for j in range(nq):
            if q_side[j] == s and px[i] == q_price[j] and ts[i] >= q_t0[j] and ts[i] < q_t1[j]:
                after = 0.0
                for q in range(12):
                    if P[s, q] == q_price[j] and S[s, q] > 0: after += S[s, q]
                dlt = after - before[j]
                if dlt < 0: dec[j] -= dlt
                elif dlt > 0:
                    inc[j] += dlt
                    if P[s, 0] == q_price[j]: ref[j] += 1
                if after > mx[j]: mx[j] = after
    return dec, inc, ref, mx

def day_events(f):
    ds = os.path.basename(f)[:8]
    try: d = dt.datetime.strptime(ds, "%Y%m%d").date()
    except ValueError: return ds, "skip", []
    if d.weekday() >= 5 or ds in SKIP_DAYS: return ds, "skip", []
    try:
        tick, (ts1, mdt, px1, v1), (ts2, sd, op, pos, px2, v2) = load(f, d)
    except Exception as ex:
        return ds, f"FAILED {ex}", []
    sd = sd.astype(np.int64); op = op.astype(np.int64); pos = pos.astype(np.int64)
    # ---- trades + aggressor side ----
    idx = np.arange(len(mdt))
    la = np.maximum.accumulate(np.where(mdt == 0, idx, -1)); lb = np.maximum.accumulate(np.where(mdt == 1, idx, -1))
    ist = mdt == 2
    tt, tp, tv = ts1[ist], px1[ist], v1[ist].astype(float)
    if len(tt) < 20000: return ds, "skip (few trades)", []
    ask = np.where(la[ist] >= 0, px1[np.maximum(la[ist], 0)], 0); bid = np.where(lb[ist] >= 0, px1[np.maximum(lb[ist], 0)], 0)
    dpx = np.sign(np.diff(tp, prepend=tp[0])); ii = np.arange(len(dpx))
    tr_rule = dpx[np.maximum.accumulate(np.where(dpx != 0, ii, 0))]
    sg = np.where((ask > 0) & (tp >= ask), 1, np.where((bid > 0) & (tp <= bid), -1, tr_rule))
    # merged orders (same ns, price, side) for lot-size signal
    mo = pd.DataFrame({"t": tt, "p": tp, "s": sg, "v": tv}).groupby(["t", "p", "s"], sort=False, as_index=False).v.sum()
    mt, mp, ms_, mv = mo.t.values, mo.p.values, mo.s.values, mo.v.values
    o_ = np.argsort(mt, kind="stable"); mt, mp, ms_, mv = mt[o_], mp[o_], ms_[o_], mv[o_]
    t_open, t_cut, t_flat = ct_ns(d, 8, 30), ct_ns(d, 11, 30), ct_ns(d, 15, 0)
    NS, SEC = 60_000_000_000, 1_000_000_000
    # ---- 1s book snapshots 08:25 -> 11:45 (state at each second) ----
    qt = np.arange(t_open - 300 * SEC, ct_ns(d, 11, 45) + 1, SEC, dtype=np.int64)
    BP, BS = book_snaps(ts2, sd, op, pos, px2, v2, qt)
    qi = np.searchsorted(ts1, qt, "left") - 1; okq = qi >= 0
    match = float(((BP[okq, 0, 0] == px1[la[np.maximum(qi, 0)]][okq]) & (BP[okq, 1, 0] == px1[lb[np.maximum(qi, 0)]][okq])).mean())
    if match < 0.97: return ds, f"SKIP book match {match*100:.1f}%", []
    rth = (qt >= t_open) & (qt < t_cut)
    lvl_avg = BS[rth].mean()
    vmin = tv[(tt >= t_open) & (tt < t_cut)].sum() / 180.0
    def snap(t): return min(max(int(np.searchsorted(qt, t, "right") - 1), 0), len(qt) - 1)
    mkey = (tt - t_open) // NS
    def close_of_min(mi):
        k = np.searchsorted(mkey, mi, "right") - 1
        return tp[k] if k >= 0 and mkey[k] == mi else None
    def win(a, b): return int(np.searchsorted(tt, a, "left")), int(np.searchsorted(tt, b, "left"))
    rows, queries = [], []
    for orm in (5, 15, 30):
        a, b = win(t_open, t_open + orm * NS)
        if b - a < 50: continue
        H, L = int(tp[a:b].max()), int(tp[a:b].min()); R = H - L
        if R <= 0: continue
        for etf in (1, 5):
            ent = None
            for mi in range(orm, 180):
                if etf == 5 and (mi + 1) % 5: continue
                c = close_of_min(mi)
                if c is None: continue
                if c > H: ent = (mi, 1); break
                if c < L: ent = (mi, -1); break
            if ent is None: continue
            mi, dr = ent
            td = t_open + (mi + 1) * NS
            i0, i1 = win(td, t_flat)
            if i0 >= i1: continue
            lvl, oth = (H, L) if dr == 1 else (L, H)
            e = tp[i0] * tick + SLIP * dr; stop = oth * tick; tgt = e + TPM * R * tick * dr
            hit = None
            for k in range(i0, i1):
                pk = tp[k] * tick
                if (pk - stop) * dr <= 0: hit = (k, stop - SSLIP * dr); break
                if (pk - tgt) * dr >= 0: hit = (k, tgt); break
            kx, xpx = hit if hit else (i1 - 1, tp[i1 - 1] * tick)
            seg = tp[i0:kx + 1] * tick                          # every trade print while the position is open
            mae = max(0.0, ((e - seg.min()) if dr == 1 else (seg.max() - e))) if len(seg) else 0.0
            row = dict(date=d, orm=orm, etf=etf, dir=dr, R=R * tick, R_pct=R * tick / e * 100, e=e,
                       pts=(xpx - e) * dr, loss=int((xpx - e) * dr <= 0), t_exit=tt[kx], td=td, mae_nt=mae)
            xt = (tp - lvl) * dr                                # ticks beyond the level
            wd = sg == dr
            bar0 = td - (60 if etf == 1 else 300) * SEC
            # first with-direction print beyond the level inside the break bar
            ba_, bb_ = win(bar0, td)
            beyond = np.nonzero((xt[ba_:bb_] > 0))[0]
            t_first = tt[ba_ + beyond[0]] if len(beyond) else bar0
            # S1/S3 level queries (opposing side: asks=0 for long, bids=1 for short)
            opp_side = 0 if dr == 1 else 1
            queries.append((len(rows), opp_side, lvl, t_first - 60 * SEC, t_first))
            a1, b1 = win(t_first - 60 * SEC, t_first)
            row["traded_at_lvl"] = tv[a1:b1][(tp[a1:b1] == lvl) & wd[a1:b1]].sum()
            # S2 absorption at level before break (120s)
            a2, b2 = win(t_first - 120 * SEC, t_first)
            row["S2_ABSORB"] = tv[a2:b2][(tp[a2:b2] == lvl) & wd[a2:b2]].sum() / lvl_avg
            # S4 stack / S5 wall at entry
            k_td = snap(td)
            own_i, opp_i = (1, 0) if dr == 1 else (0, 1)
            row["S4_STACK"] = BS[k_td, own_i, :5].sum() / max(BS[k_td, opp_i, :5].sum(), 1)
            tgt_t = int(round(TPM * R)) + 1
            ahead = BP[k_td, opp_i] * 1; ahead_ticks = (ahead - tp[i0]) * dr
            m_ = (ahead_ticks >= 0) & (ahead_ticks <= tgt_t) & (BS[k_td, opp_i] > 0)
            row["S5_WALL"] = BS[k_td, opp_i][m_].max() / lvl_avg if m_.any() else 0.0
            # S6 5-min delta into break
            a6, b6 = win(td - 300 * SEC, td)
            vv = tv[a6:b6].sum()
            row["S6_DIVERG"] = (tv[a6:b6] * sg[a6:b6]).sum() * dr / vv if vv else 0.0
            # S7 burst / S8 big lots in the break bar
            row["S7_BURST"] = tv[ba_:bb_][wd[ba_:bb_]].sum() / vmin * (60 / (td - bar0) * SEC)
            m0, m1 = int(np.searchsorted(mt, bar0)), int(np.searchsorted(mt, td))
            wv = mv[m0:m1][ms_[m0:m1] == dr]
            row["S8_BIGLOT"] = wv[wv >= 10].sum() / wv.sum() if wv.sum() else 0.0
            # S9 trapped counter traders (last 5 min), underwater at entry
            ce = tp[max(i0 - 1, 0)]
            cm = ~wd[a6:b6] & ((ce - tp[a6:b6]) * dr > 0)
            row["S9_TRAPPED"] = tv[a6:b6][cm].sum() / vmin
            # S10 reload ahead: opposing 10-lvl depth at +30s / at entry
            k30 = snap(td + 30 * SEC)
            row["S10_RELOAD"] = BS[k30, opp_i].sum() / max(BS[k_td, opp_i].sum(), 1)
            row["alive30"] = int(tt[kx] >= td + 30 * SEC)
            j30 = int(np.searchsorted(tt, td + 30 * SEC)) - 1
            row["px30"] = (tp[j30] * tick - e) * dr - SLIP
            # S11 exhaustion first 2 min
            a11, b11 = win(td, td + 120 * SEC)
            fav = ((tp[a11:b11] - tp[i0]) * dr).max() if b11 > a11 else 0
            row["S11_EXHAUST"] = tv[a11:b11][wd[a11:b11]].sum() / vmin / (max(fav, 0) + 1)
            row["alive120"] = int(tt[kx] >= td + 120 * SEC)
            row["px120"] = (tp[max(b11 - 1, 0)] * tick - e) * dr - SLIP
            # S12 retest defence
            post = np.nonzero(xt[i0:kx + 1] <= 0)[0]
            row["S12_RETEST"] = np.nan; row["rt_exit"] = np.nan
            if len(post):
                kr = i0 + post[0]; trt = tt[kr]
                if trt + 30 * SEC < ct_ns(d, 11, 45) and tt[kx] >= trt + 30 * SEC:
                    ar, br = win(trt, trt + 30 * SEC)
                    counter = tv[ar:br][(~wd[ar:br]) & (np.abs(xt[ar:br]) <= 1)].sum()
                    ks = snap(trt)
                    pr_own = BP[ks, own_i]; own_at = BS[ks, own_i][(np.abs((pr_own - lvl) * dr) <= 1) & (BS[ks, own_i] > 0)].sum()
                    row["S12_RETEST"] = counter / max(own_at, 1)
                    behind = xt[br - 1] <= 0 if br > ar else False
                    row["rt_behind"] = int(behind)
                    row["rt_exit"] = (tp[max(br - 1, 0)] * tick - e) * dr - SLIP
            rows.append(row)
    if queries:
        q = np.array(queries, dtype=np.int64)
        dec, inc, ref, mx = level_track(ts2, sd, op, pos, px2, v2, q[:, 1], q[:, 2], q[:, 3], q[:, 4])
        for j, (ri, *_r) in enumerate(queries):
            r = rows[ri]
            r["lvl_dec"], r["lvl_inc"], r["lvl_max"] = dec[j], inc[j], mx[j]
            r["lvl_refills"] = ref[j]
            # share of the PEAK resting size at the level that vanished without being traded (cancelled / pulled)
            r["S1_PULL"] = min(1.0, max(0.0, 1 - r["traded_at_lvl"] / mx[j])) if mx[j] > 0 else 0.0
            # traded at the level / peak displayed size there (>1 = hidden size / refills = iceberg defending)
            r["S3_ICEBERG"] = r["traded_at_lvl"] / mx[j] if mx[j] > 0 else 0.0
    return ds, f"ok  {len(rows)} events  book match {match*100:.1f}%", rows

RULES = [  # name, column, kind, direction, thresholds
    ("S1 PULL (skip if >=)", "S1_PULL", "filter", "skip_ge", (0.5, 0.7, 0.9)),
    ("S2 ABSORB (keep if >=)", "S2_ABSORB", "filter", "keep_ge", (1, 3, 6)),
    ("S3 ICEBERG (skip if >=)", "S3_ICEBERG", "filter", "skip_ge", (1.0, 2.0, 4.0)),
    ("S4 STACK (keep if >=)", "S4_STACK", "filter", "keep_ge", (1.0, 1.5, 2.0)),
    ("S5 WALL (skip if >=)", "S5_WALL", "filter", "skip_ge", (3, 5, 8)),
    ("S6 DIVERG (skip if <)", "S6_DIVERG", "filter", "skip_lt", (0.0, -0.05, -0.10)),
    ("S7 BURST (keep if >=)", "S7_BURST", "filter", "keep_ge", (0.5, 1.0, 1.5)),
    ("S8 BIGLOT (keep if >=)", "S8_BIGLOT", "filter", "keep_ge", (0.05, 0.15, 0.30)),
    ("S9 TRAPPED (keep if >=)", "S9_TRAPPED", "filter", "keep_ge", (0.25, 0.5, 1.0)),
    ("S10 RELOAD (exit@30s if >=)", "S10_RELOAD", "exit30", "exit_ge", (1.2, 1.4, 1.6)),
    ("S11 EXHAUST (exit@2m if >=)", "S11_EXHAUST", "exit120", "exit_ge", (0.1, 0.25, 0.5)),
    ("S12 RETEST (exit if >= & behind)", "S12_RETEST", "exitrt", "exit_ge", (1, 2, 4)),
]

def stt(p):
    p = np.asarray(p, float)
    if len(p) == 0: return "  0 tr" + " " * 33
    w = p > 0; gl = -p[~w].sum()
    return f"{len(p):3d} tr WR {w.mean()*100:5.1f}% PF {min(p[w].sum()/gl, 99) if gl > 0 else 99:5.2f} exp {p.mean():+6.1f}"

def apply(s, col, kind, how, th):
    v = s[col]
    if kind == "filter":
        keep = (v < th) if how == "skip_ge" else (v >= th) if how == "keep_ge" else (v >= th)
        return s.pts[keep.fillna(True) if how != "keep_ge" else keep.fillna(False)].values
    if kind == "exit30":
        trig = (s.alive30 == 1) & (v >= th); return np.where(trig, s.px30, s.pts)
    if kind == "exit120":
        trig = (s.alive120 == 1) & (v >= th); return np.where(trig, s.px120, s.pts)
    trig = (v >= th) & (s.get("rt_behind", 0) == 1); return np.where(trig.fillna(False), s.rt_exit, s.pts)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nrd-dir", action="append", required=True)
    ap.add_argument("--workers", type=int, default=max(1, cpu_count() - 1))
    ap.add_argument("--out", default="orb_signals_events.csv")
    a = ap.parse_args()
    files = sorted({f for dd in a.nrd_dir for f in glob.glob(os.path.join(os.path.expanduser(dd), "**", "*.nrd"), recursive=True)})
    best = {}
    for f in files:
        k = os.path.basename(f)[:8]
        if k not in best or os.path.getsize(f) > os.path.getsize(best[k]): best[k] = f
    files = [best[k] for k in sorted(best)]
    print(f"{len(files)} files, {a.workers} workers", flush=True)
    T0 = time.time(); EV = []
    with Pool(a.workers) as pool:
        for i, (ds, msg, rows) in enumerate(pool.imap_unordered(day_events, files), 1):
            if msg != "skip": print(f"[{time.time()-T0:5.0f}s] {i:3d}/{len(files)} {ds}: {msg}", flush=True)
            EV += rows
    E = pd.DataFrame(EV).sort_values(["date", "orm", "etf"]).reset_index(drop=True)
    E.to_csv(a.out, index=False)
    days = sorted(E.date.unique()); cut = days[int(len(days) * 0.6)]
    E["fit"] = E.date < cut
    print(f"\n{len(days)} days, {len(E)} events, {E.loss.sum()} losers | FIT {days[0]}..{days[int(len(days)*0.6)-1]} | TEST {cut}..{days[-1]} -> {a.out}")
    print("\nsignal distributions (all events): p10 / p50 / p90")
    for _, col, *_r in RULES:
        v = E[col].dropna()
        print(f"  {col:12s} {v.quantile(.1):8.3f} {v.quantile(.5):8.3f} {v.quantile(.9):8.3f}   (n={len(v)})")
    SETS = {"ALL 6 ORB versions": E.orm > 0, "OR15 1m": (E.orm == 15) & (E.etf == 1), "OR5 5m": (E.orm == 5) & (E.etf == 5)}
    for sname, m in SETS.items():
        F, T = E[m & E.fit], E[m & ~E.fit]
        print(f"\n{'═'*118}\n {sname}\n{'═'*118}")
        print(f" {'rule':34s} {'thr':>6s} | {'FIT':38s} | {'TEST':38s}")
        print(f" {'BASE':34s} {'':6s} | {stt(F.pts)} | {stt(T.pts)}")
        bF, bT = F.pts.mean(), T.pts.mean()
        for name, col, kind, how, ths in RULES:
            for th in ths:
                pf, pt = apply(F, col, kind, how, th), apply(T, col, kind, how, th)
                good = len(pf) and len(pt) and pf.mean() > bF and pt.mean() > bT
                rem = ""
                if kind == "filter":
                    nk = len(pt); nr = len(T) - nk
                    rem = f"| removed {nr:3d}: exp {(T.pts.sum() - pt.sum()) / nr:+6.1f}" if nr else "| removed   0"
                print(f" {name:34s} {th:6g} | {stt(pf)} | {stt(pt)} {rem} {'<< better on BOTH' if good else ''}")
    print(f"\ndone in {time.time()-T0:.0f}s")
