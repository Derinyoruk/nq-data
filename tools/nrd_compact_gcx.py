#!/usr/bin/env python3
"""
nrd_compact.py — shrink NinjaTrader 8 replay days (.nrd) to a compact per-SECOND dataset (~1 MB/day) for upload.
Parallel over all CPU cores. Skips days whose rebuilt book doesn't match NT's own L1 bid/ask (>= 97% required).

Per day -> NQ_YYYYMMDD.parquet, one row per second, 08:25:00 -> 15:00:00 CT:
  t (epoch s), o h l c (trade prices), buy_v sell_v (aggressor volume), n (trades)
  08:25 -> 11:45 only (book): bb ba (best bid/ask), bs0..bs9 / as0..as9 (size at bid/ask level 0..9),
                              bo1..bo9 / ao1..ao9 (level distance from best, in ticks)
Book = state at the END of each second. Aggressor: trade >= prevailing ask -> buy, <= bid -> sell, else tick rule.
At the end everything is zipped into ~/cvd-bot/nq_compact.zip.
usage: python3 nrd_compact.py --nrd-dir "~/Downloads/NQ 2026 Continuous 2" [--nrd-dir another_folder ...]
"""
import argparse, datetime as dt, glob, os, struct, sys, time, zipfile
from multiprocessing import Pool, cpu_count
from zoneinfo import ZoneInfo
import numpy as np, pandas as pd
from numba import njit

CT = ZoneInfo("America/Chicago")
EPOCH_TICKS = 621355968000000000
HDR = struct.Struct("<diddddd i qq q"); N_SLOTS = 44
OUT = os.path.expanduser("~/cvd-bot/nq_compact")

@njit(cache=True)
def _be(buf, j, n):
    v = np.int64(0)
    for q in range(n): v = (v << 8) | np.int64(buf[j + q])
    return v

@njit(cache=True)
def decode(buf, cur, l2cur, t, t_from, t_stop, cap1, cap2):
    VS = np.array([0, 1, 1, 1, 1, 2, 4, -1]); VM = np.array([1, 1, 100, 500, 1000, 1, 1, 1]); TS = np.array([0, 1, 2, 4])
    a_ts = np.empty(cap1, np.int64); a_mdt = np.empty(cap1, np.int8); a_px = np.empty(cap1, np.int64); a_v = np.empty(cap1, np.int64)
    b_ts = np.empty(cap2, np.int64); b_sd = np.empty(cap2, np.int8); b_op = np.empty(cap2, np.int8)
    b_pos = np.empty(cap2, np.int32); b_px = np.empty(cap2, np.int64); b_v = np.empty(cap2, np.int64)
    n = len(buf); i = 0; k1 = 0; k2 = 0; err = 0
    while i < n:
        if i + 3 > n: err = 1; break
        m1 = np.int64(buf[i]); m2 = np.int64(buf[i + 1]); info = np.int64(buf[i + 2]); j = i + 3
        tscode = m1 & 3
        if m1 & 4:
            if tscode == 0: t += _be(buf, j, 8); j += 8
            else: sz = TS[tscode]; t += _be(buf, j, sz) * 10_000_000; j += sz
        elif tscode > 0:
            sz = TS[tscode]; t += _be(buf, j, sz); j += sz
        if t > t_stop: break
        pbits = m1 & 0x18; vc = (m1 >> 5) & 7
        if info & 0xC0 == 0xC0:
            mdt = m2 >> 5; nib = m2 & 0x1F
            if nib == 0x1F: mdt += 8; nib = 0
            c = cur[mdt]
            if nib: c += (nib - 15) if nib <= 14 else (nib - 14)
            elif pbits == 0x08: c -= 15
            elif pbits == 0x10: c += np.int64(buf[j]) - 0x80; j += 1
            elif pbits == 0x18: c += _be(buf, j, 4) - 0x80000000; j += 4
            cur[mdt] = c
            if VS[vc] < 0: err = 2; break
            vol = _be(buf, j, VS[vc]) * VM[vc] if VS[vc] else 0; j += VS[vc]
            if t >= t_from:
                if k1 >= cap1: err = 3; break
                a_ts[k1] = (t - EPOCH_TICKS) * 100; a_mdt[k1] = mdt; a_px[k1] = c; a_v[k1] = vol; k1 += 1
        else:
            sd = 1 if (m2 & 0x80) else 0
            c = l2cur[sd]
            if pbits == 0x08: c += np.int64(buf[j]) - 0x80; j += 1
            elif pbits == 0x10: c += _be(buf, j, 2) - 0x8000; j += 2
            elif pbits == 0x18: c += _be(buf, j, 4) - 0x80000000; j += 4
            l2cur[sd] = c
            if VS[vc] < 0: err = 2; break
            vol = _be(buf, j, VS[vc]) * VM[vc] if VS[vc] else 0; j += VS[vc]
            if t >= t_from - 3_000_000_000:
                if k2 >= cap2: err = 3; break
                b_ts[k2] = (t - EPOCH_TICKS) * 100; b_sd[k2] = sd
                b_op[k2] = 2 if (info & 0x80) else (1 if (info & 0x40) else 0)
                b_pos[k2] = info & 0x3F; b_px[k2] = c; b_v[k2] = vol; k2 += 1
        i = j
    return err, i, a_ts[:k1], a_mdt[:k1], a_px[:k1], a_v[:k1], b_ts[:k2], b_sd[:k2], b_op[:k2], b_pos[:k2], b_px[:k2], b_v[:k2]

@njit(cache=True)
def book_snaps(ts, sd, op, pos, px, vol, qt):
    P = np.zeros((2, 12), np.int64); S = np.zeros((2, 12), np.int64)
    outp = np.zeros((len(qt), 2, 10), np.int64); outs = np.zeros((len(qt), 2, 10), np.int64); j = 0; n = len(ts)
    for i in range(n + 1):
        while j < len(qt) and (i == n or ts[i] >= qt[j]):
            for s in range(2):
                for q in range(10):
                    outp[j, s, q] = P[s, q]; outs[j, s, q] = S[s, q]
            j += 1
        if i == n: break
        s = sd[i]; p = pos[i]
        if p > 10: continue
        if op[i] == 0:
            for q in range(11, p, -1): P[s, q] = P[s, q - 1]; S[s, q] = S[s, q - 1]
            P[s, p] = px[i]; S[s, p] = vol[i]
        elif op[i] == 1:
            P[s, p] = px[i]; S[s, p] = vol[i]
        else:
            for q in range(p, 11): P[s, q] = P[s, q + 1]; S[s, q] = S[s, q + 1]
            P[s, 11] = 0; S[s, 11] = 0
    return outp, outs

def ct_ns(d, h, m):
    return int(dt.datetime(d.year, d.month, d.day, h, m, tzinfo=CT).timestamp() * 1e9)

def load(path, d):
    raw = open(path, "rb").read()
    slots = []
    for s in range(N_SLOTS):
        (last, cnt, pmax, pmin, pfirst, _one, ticksz, _flag, t0, t1, volsum) = HDR.unpack_from(raw, s * 80)
        slots.append(dict(count=cnt, first=pfirst, tick=ticksz, t0=t0))
    tick = next(s["tick"] for s in slots if s["count"])
    cur = np.array([round(slots[k]["first"] / tick) for k in range(10)], np.int64)
    l2cur = np.array([round(slots[10]["first"] / tick), round(slots[11]["first"] / tick)], np.int64)
    buf = np.frombuffer(raw, np.uint8)[N_SLOTS * 80:]
    t_from = ct_ns(d, 7, 0) // 100 + EPOCH_TICKS; t_stop = ct_ns(d, 15, 1) // 100 + EPOCH_TICKS
    cap1 = int(sum(slots[k]["count"] for k in range(10))) + 100000
    cap2 = int(slots[10]["count"] + slots[11]["count"]) + 100000
    r = decode(buf, cur, l2cur, min(s["t0"] for s in slots if s["count"]), t_from, t_stop, cap1, cap2)
    if r[0]: raise RuntimeError(f"decode error {r[0]} at offset {r[1]}")
    return tick, r[2:6], r[6:12]

def build(d, tick, L1, L2):
    """returns (DataFrame per second, book match fraction)"""
    ts1, mdt, px1, v1 = L1; ts2, sd, op, pos, px2, v2 = L2      # prices in TICKS (int)
    idx = np.arange(len(mdt))
    la = np.maximum.accumulate(np.where(mdt == 0, idx, -1)); lb = np.maximum.accumulate(np.where(mdt == 1, idx, -1))
    ist = mdt == 2
    tt, tp, tv = ts1[ist], px1[ist], v1[ist]
    if len(tt) < 5000: return None, 0.0
    ask_at = np.where(la[ist] >= 0, px1[np.maximum(la[ist], 0)], 10**12)
    bid_at = np.where(lb[ist] >= 0, px1[np.maximum(lb[ist], 0)], -10**12)
    dpx = np.sign(np.diff(tp, prepend=tp[0]))
    nz = np.where(dpx != 0, idx[:len(dpx)], 0); nz = np.maximum.accumulate(nz); tick_rule = dpx[nz]
    sg = np.where(tp >= ask_at, 1, np.where(tp <= bid_at, -1, tick_rule))
    t_a, t_b, t_e = ct_ns(d, 8, 25), ct_ns(d, 11, 45), ct_ns(d, 15, 0)
    secs = np.arange(t_a // 10**9, t_e // 10**9, dtype=np.int64)
    # per-second trades
    k = tt // 10**9 - secs[0]; ok = (k >= 0) & (k < len(secs)); k, p_, v_, s_ = k[ok], tp[ok], tv[ok], sg[ok]
    df = pd.DataFrame({"k": k, "p": p_, "bv": np.where(s_ > 0, v_, 0), "sv": np.where(s_ < 0, v_, 0)})
    g = df.groupby("k")
    out = pd.DataFrame(index=np.arange(len(secs)))
    out["t"] = secs
    out["o"] = g.p.first(); out["h"] = g.p.max(); out["l"] = g.p.min(); out["c"] = g.p.last()
    out["buy_v"] = g.bv.sum(); out["sell_v"] = g.sv.sum(); out["n"] = g.size()
    out[["buy_v", "sell_v", "n"]] = out[["buy_v", "sell_v", "n"]].fillna(0).astype(np.int32)
    # book at end of each second 08:25 -> 11:45
    nb = int((t_b - t_a) // 10**9)
    qt = (secs[:nb] + 1) * 10**9
    P, S = book_snaps(ts2, sd.astype(np.int64), op.astype(np.int64), pos.astype(np.int64), px2, v2, qt)
    qi = np.searchsorted(ts1, qt, "left") - 1; okq = qi >= 0
    l1a = px1[la[np.maximum(qi, 0)]]; l1b = px1[lb[np.maximum(qi, 0)]]
    match = float(((P[okq, 0, 0] == l1a[okq]) & (P[okq, 1, 0] == l1b[okq])).mean())
    bk = {"bb": P[:, 1, 0], "ba": P[:, 0, 0]}
    for q in range(10):
        bk[f"bs{q}"] = S[:, 1, q].clip(0, 32767).astype(np.int16); bk[f"as{q}"] = S[:, 0, q].clip(0, 32767).astype(np.int16)
        if q:
            bk[f"bo{q}"] = (P[:, 1, 0] - P[:, 1, q]).clip(0, 255).astype(np.uint8)
            bk[f"ao{q}"] = (P[:, 0, q] - P[:, 0, 0]).clip(0, 255).astype(np.uint8)
    bk = pd.DataFrame(bk)
    for c in bk.columns: out.loc[: nb - 1, c] = bk[c].values
    for c in ("o", "h", "l", "c", "bb", "ba"): out[c] = (out[c] * tick).astype(np.float64)
    return out, match

def work(f):
    try:
        d = dt.datetime.strptime(os.path.basename(f)[:8], "%Y%m%d").date()
    except ValueError:
        return f, "skip (name)", None
    if d.weekday() >= 5: return f, "skip (weekend)", None
    dst = os.path.join(OUT, f"NQ_{d:%Y%m%d}.parquet")
    if os.path.exists(dst): return f, "already done", dst
    try:
        tick, L1, L2 = load(f, d)
        df, match = build(d, tick, L1, L2)
    except Exception as ex:
        return f, f"FAILED {ex}", None
    if df is None: return f, "skip (no trades)", None
    if match < 0.97: return f, f"SKIP book match {match*100:.1f}%", None
    df.to_parquet(dst, compression="zstd", index=False)
    return f, f"ok  match {match*100:.1f}%  {os.path.getsize(dst)/1e6:.2f} MB", dst

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nrd-dir", action="append", required=True)
    ap.add_argument("--workers", type=int, default=max(1, cpu_count() - 1))
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    files = sorted({f for dd in a.nrd_dir for f in glob.glob(os.path.join(os.path.expanduser(dd), "**", "*.nrd"), recursive=True)})
    # one file per date (if duplicated across folders, keep the largest = front contract)
    best = {}
    for f in files:
        key = os.path.basename(f)[:8]
        if key not in best or os.path.getsize(f) > os.path.getsize(best[key]): best[key] = f
    files = [best[k] for k in sorted(best)]
    print(f"{len(files)} days to process with {a.workers} workers -> {OUT}", flush=True)
    T0 = time.time()
    load(files[0], dt.datetime.strptime(os.path.basename(files[0])[:8], "%Y%m%d").date())   # compile numba once
    done = []
    with Pool(a.workers) as pool:
        for i, (f, msg, dst) in enumerate(pool.imap_unordered(work, files), 1):
            print(f"[{time.time()-T0:5.0f}s] {i:3d}/{len(files)} {os.path.basename(f)}: {msg}", flush=True)
            if dst: done.append(dst)
    zp = os.path.expanduser("~/cvd-bot/nq_compact.zip")
    with zipfile.ZipFile(zp, "w", zipfile.ZIP_STORED) as z:
        for p in sorted(done): z.write(p, os.path.basename(p))
    print(f"\n{len(done)} days -> {zp}  ({os.path.getsize(zp)/1e6:.0f} MB)  in {time.time()-T0:.0f}s")
