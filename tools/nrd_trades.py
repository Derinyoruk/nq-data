#!/usr/bin/env python3
"""
nrd_trades.py — add-on to nrd_compact.py: save EVERY trade 08:25-11:45 CT per day (for level-specific order flow:
absorption vs pull at the breakout level, retests, trapped traders, exhaustion).
Needs nrd_compact.py in the same folder (reuses its decoder). Parallel over all cores. ~1 MB/day.

Per day -> ~/cvd-bot/nq_trades/NQ_YYYYMMDD_trades.parquet
  ms   : milliseconds since 08:25:00 CT (int32)
  px   : trade price in ticks (int32)   (price = px * 0.25)
  sz   : size (int16)
  side : +1 buyer aggressor, -1 seller aggressor (int8)  (trade >= prevailing ask / <= prevailing bid, else tick rule)
  Fills with the same millisecond, price and side are merged (NT prints each fill separately; this approximates
  one aggressive order per row).
Zipped to ~/cvd-bot/nq_trades.zip
usage: python3 nrd_trades.py --nrd-dir ~/Downloads
"""
import argparse, datetime as dt, glob, os, time, zipfile
from multiprocessing import Pool, cpu_count
import numpy as np, pandas as pd
from nrd_compact import load, ct_ns

OUT = os.path.expanduser("~/cvd-bot/nq_trades")
SKIP = {"20260703", "20260907"}          # half days

def work(f):
    ds = os.path.basename(f)[:8]
    try: d = dt.datetime.strptime(ds, "%Y%m%d").date()
    except ValueError: return f, "skip (name)", None
    if d.weekday() >= 5 or ds in SKIP: return f, "skip", None
    dst = os.path.join(OUT, f"NQ_{ds}_trades.parquet")
    if os.path.exists(dst): return f, "already done", dst
    if not os.path.exists(os.path.expanduser(f"~/cvd-bot/nq_compact/NQ_{ds}.parquet")):
        return f, "skip (no compact file = failed book check)", None
    try:
        tick, (ts1, mdt, px1, v1), _ = load(f, d)
    except Exception as ex:
        return f, f"FAILED {ex}", None
    idx = np.arange(len(mdt))
    la = np.maximum.accumulate(np.where(mdt == 0, idx, -1)); lb = np.maximum.accumulate(np.where(mdt == 1, idx, -1))
    ist = mdt == 2
    tt, tp, tv = ts1[ist], px1[ist], v1[ist]
    ask = np.where(la[ist] >= 0, px1[np.maximum(la[ist], 0)], 0); bid = np.where(lb[ist] >= 0, px1[np.maximum(lb[ist], 0)], 0)
    dpx = np.sign(np.diff(tp, prepend=tp[0])); ii = np.arange(len(dpx))
    nz = np.maximum.accumulate(np.where(dpx != 0, ii, 0)); tick_rule = dpx[nz]
    side = np.where((ask > 0) & (tp >= ask), 1, np.where((bid > 0) & (tp <= bid), -1, tick_rule))
    a, b = ct_ns(d, 8, 25), ct_ns(d, 11, 45)
    k = (tt >= a) & (tt < b)
    if k.sum() < 1000: return f, "skip (no trades)", None
    out = pd.DataFrame({"ms": ((tt[k] - a) // 1_000_000).astype(np.int32), "px": tp[k].astype(np.int32),
                        "sz": tv[k].astype(np.int32), "side": side[k].astype(np.int8)})
    out = out.groupby(["ms", "px", "side"], sort=False, as_index=False).sz.sum()
    out["sz"] = out.sz.clip(0, 32767).astype(np.int16)
    out = out[["ms", "px", "sz", "side"]]
    out.to_parquet(dst, compression="zstd", index=False)
    return f, f"ok  {k.sum():,} trades  {os.path.getsize(dst)/1e6:.2f} MB", dst

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nrd-dir", action="append", required=True)
    ap.add_argument("--workers", type=int, default=max(1, cpu_count() - 1))
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    files = sorted({f for dd in a.nrd_dir for f in glob.glob(os.path.join(os.path.expanduser(dd), "**", "*.nrd"), recursive=True)})
    best = {}
    for f in files:
        key = os.path.basename(f)[:8]
        if key not in best or os.path.getsize(f) > os.path.getsize(best[key]): best[key] = f
    files = [best[k] for k in sorted(best)]
    print(f"{len(files)} files, {a.workers} workers -> {OUT}", flush=True)
    T0 = time.time(); done = []
    with Pool(a.workers) as pool:
        for i, (f, msg, dst) in enumerate(pool.imap_unordered(work, files), 1):
            print(f"[{time.time()-T0:5.0f}s] {i:3d}/{len(files)} {os.path.basename(f)}: {msg}", flush=True)
            if dst: done.append(dst)
    zp = os.path.expanduser("~/cvd-bot/nq_trades.zip")
    with zipfile.ZipFile(zp, "w", zipfile.ZIP_STORED) as z:
        for p in sorted(done): z.write(p, os.path.basename(p))
    print(f"\n{len(done)} days -> {zp}  ({os.path.getsize(zp)/1e6:.0f} MB)  in {time.time()-T0:.0f}s")
