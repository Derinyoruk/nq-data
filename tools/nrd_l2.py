#!/usr/bin/env python3
"""
nrd_l2.py - export the FULL event-level order book from NinjaTrader .nrd files (no 1-second sampling).
Needs nrd_compact.py in the same folder (reuses its decoder: load, ct_ns, book_snaps).

Per day, window 08:25 -> 11:45 CT (depth events kept from 07:55 so the book can be rebuilt from empty):
  ~/cvd-bot/nq_l2/NQ_YYYYMMDD_l2.parquet   every depth event:
        ns   int64  nanoseconds since 08:25 CT (negative = before 08:25, warm-up)
        sd   int8   1 = bid, 0 = ask
        op   int8   0 = insert, 1 = update, 2 = delete
        pos  int8   book level (0 = best)
        px   int32  price in ticks
        v    int32  size
  ~/cvd-bot/nq_l2/NQ_YYYYMMDD_tr.parquet   every trade:
        ns, px (ticks), v, sd (+1 buyer aggressor: price >= prevailing L1 ask, -1 seller: <= bid, else tick rule)
Day is skipped if the rebuilt book matches NT's own L1 < 97% (same rule as nrd_compact).

usage:
  test one day first:   python3 nrd_l2.py --nrd-dir "~/folder/with/one/nrd" --max-days 1
  from 7z archives:     python3 nrd_l2.py --archive "~/Downloads/NQ 2025 Continuous.7z.001" --every 3
                        (--every 3 = keep every 3rd trading day; --months 202503,202504 = only those months)
Finished days are skipped, so it can be stopped and restarted.
"""
import argparse, datetime as dt, glob, os, shutil, subprocess, tempfile, time
from multiprocessing import Pool, cpu_count
import numpy as np, pandas as pd
from nrd_compact import load, ct_ns, book_snaps

OUT = os.path.expanduser("~/cvd-bot/nq_l2")
SKIP = {"20260703", "20260907"}          # half days


def day_of(f):
    try:
        return dt.datetime.strptime(os.path.basename(f)[:8], "%Y%m%d").date()
    except ValueError:
        return None


def work(f):
    d = day_of(f)
    if d is None: return f, "skip (name)"
    if d.weekday() >= 5 or f"{d:%Y%m%d}" in SKIP: return f, "skip (weekend/half day)"
    p2 = os.path.join(OUT, f"NQ_{d:%Y%m%d}_l2.parquet"); p1 = os.path.join(OUT, f"NQ_{d:%Y%m%d}_tr.parquet")
    if os.path.exists(p2) and os.path.exists(p1): return f, "already done"
    try:
        tick, L1, L2 = load(f, d)
    except Exception as ex:
        return f, f"FAILED {ex}"
    ts1, mdt, px1, v1 = L1; ts2, sd, op, pos, px2, v2 = L2
    if (mdt == 2).sum() < 5000: return f, "skip (no trades)"
    t0 = ct_ns(d, 8, 25); t_end = ct_ns(d, 11, 45)
    # sanity: rebuild book at the end of each second 08:25-11:45 and compare with NT's own L1
    qt = np.arange(t0 + 10**9, t_end + 1, 10**9, dtype=np.int64)
    P, S = book_snaps(ts2, sd.astype(np.int64), op.astype(np.int64), pos.astype(np.int64), px2, v2, qt)
    idx = np.arange(len(mdt))
    la = np.maximum.accumulate(np.where(mdt == 0, idx, -1)); lb = np.maximum.accumulate(np.where(mdt == 1, idx, -1))
    qi = np.searchsorted(ts1, qt, "left") - 1; ok = (qi >= 0) & (la[np.maximum(qi, 0)] >= 0) & (lb[np.maximum(qi, 0)] >= 0)
    l1a = px1[la[np.maximum(qi, 0)]]; l1b = px1[lb[np.maximum(qi, 0)]]
    match = float(((P[ok, 0, 0] == l1a[ok]) & (P[ok, 1, 0] == l1b[ok])).mean()) if ok.any() else 0.0
    if match < 0.97: return f, f"SKIP book match {match*100:.1f}%"
    k2 = ts2 < t_end
    df2 = pd.DataFrame({"ns": ts2[k2] - t0, "sd": sd[k2].astype(np.int8), "op": op[k2].astype(np.int8),
                        "pos": pos[k2].astype(np.int8), "px": px2[k2].astype(np.int32), "v": v2[k2].clip(0, 2**31 - 1).astype(np.int32)})
    ist = mdt == 2
    ask_at = np.where(la[ist] >= 0, px1[np.maximum(la[ist], 0)], 10**12)
    bid_at = np.where(lb[ist] >= 0, px1[np.maximum(lb[ist], 0)], -10**12)
    tp = px1[ist]; ti = np.arange(len(tp))
    dpx = np.sign(np.diff(tp, prepend=tp[0])); nz = np.maximum.accumulate(np.where(dpx != 0, ti, 0)); tick_rule = dpx[nz]
    sg = np.where(tp >= ask_at, 1, np.where(tp <= bid_at, -1, tick_rule))
    tt = ts1[ist]; k1 = (tt >= t0 - 300 * 10**9) & (tt < t_end)
    df1 = pd.DataFrame({"ns": tt[k1] - t0, "px": tp[k1].astype(np.int32),
                        "v": v1[ist][k1].clip(0, 2**31 - 1).astype(np.int32), "sd": sg[k1].astype(np.int8)})
    df2.to_parquet(p2, compression="zstd", index=False); df1.to_parquet(p1, compression="zstd", index=False)
    mb = (os.path.getsize(p2) + os.path.getsize(p1)) / 1e6
    return f, (f"ok  match {match*100:.1f}%  depth events {len(df2):,}  "
               f"trades {len(df1):,}  {mb:.1f} MB")


def select(files, every, months, max_days):
    best = {}
    for f in files:
        key = os.path.basename(f)[:8]
        if not key.isdigit(): continue
        if months and key[:6] not in months: continue
        if key not in best or os.path.getsize(f) > os.path.getsize(best[key]): best[key] = f
    keys = [k for k in sorted(best) if dt.datetime.strptime(k, "%Y%m%d").weekday() < 5 and k not in SKIP]
    keys = keys[::every]
    if max_days: keys = keys[:max_days]
    return [best[k] for k in keys]


def run_files(files, workers, T0):
    if not files: return
    if workers <= 1 or len(files) == 1:
        for i, f in enumerate(files, 1):
            print(f"[{time.time()-T0:6.0f}s] {i:3d}/{len(files)} {os.path.basename(f)}: {work(f)[1]}", flush=True)
        return
    with Pool(workers) as pool:
        for i, (f, msg) in enumerate(pool.imap_unordered(work, files), 1):
            print(f"[{time.time()-T0:6.0f}s] {i:3d}/{len(files)} {os.path.basename(f)}: {msg}", flush=True)


def sevenzip():
    for b in ("7zz", "7z", "7za"):
        if shutil.which(b): return b
    raise SystemExit("no 7-Zip found: brew install sevenzip")


def archive_months(archive, z):
    lst = subprocess.run([z, "l", "-ba", "-slt", archive], capture_output=True, text=True).stdout
    names = [l.split(" = ", 1)[1] for l in lst.splitlines() if l.startswith("Path = ") and l.lower().endswith(".nrd")]
    return sorted({os.path.basename(n)[:6] for n in names if os.path.basename(n)[:6].isdigit()})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nrd-dir", action="append", default=[])
    ap.add_argument("--archive", action="append", default=[])
    ap.add_argument("--every", type=int, default=1, help="keep every Nth trading day")
    ap.add_argument("--months", default="", help="comma list YYYYMM, empty = all")
    ap.add_argument("--max-days", type=int, default=0)
    ap.add_argument("--workers", type=int, default=max(1, min(4, cpu_count() - 1)))
    a = ap.parse_args()
    months = {m.strip() for m in a.months.split(",") if m.strip()}
    os.makedirs(OUT, exist_ok=True)
    T0 = time.time()
    for dd in a.nrd_dir:
        fs = glob.glob(os.path.join(os.path.expanduser(dd), "**", "*.nrd"), recursive=True)
        run_files(select(fs, a.every, months, a.max_days), a.workers, T0)
    if a.archive:
        z = sevenzip()
        for arc in a.archive:
            arc = os.path.expanduser(arc)
            ms = [m for m in archive_months(arc, z) if not months or m in months]
            print(f"{os.path.basename(arc)}: months {ms}", flush=True)
            for m in ms:
                mark = os.path.join(OUT, f".done_{os.path.basename(arc)[:20]}_{m}_e{a.every}")
                if os.path.exists(mark):
                    print(f"  {m}: done earlier, skip", flush=True); continue
                tmp = tempfile.mkdtemp(prefix=f"nrd_{m}_", dir=os.path.dirname(arc))
                print(f"  {m}: extracting ...", flush=True)
                r = subprocess.run([z, "x", arc, f"-o{tmp}", f"-ir!{m}*.nrd", "-y"], capture_output=True, text=True)
                if r.returncode != 0: print(r.stderr[-500:], flush=True)
                got = glob.glob(os.path.join(tmp, "**", "*.nrd"), recursive=True)
                print(f"  {m}: {len(got)} .nrd files", flush=True)
                run_files(select(got, a.every, set(), a.max_days), a.workers, T0)
                shutil.rmtree(tmp, ignore_errors=True)
                if got and not a.max_days: open(mark, "w").close()
    fs = glob.glob(os.path.join(OUT, "NQ_*_l2.parquet"))
    tot = sum(os.path.getsize(p) for p in glob.glob(os.path.join(OUT, "NQ_*.parquet"))) / 1e6
    print(f"\n{len(fs)} days in {OUT}  ({tot:.0f} MB total, {time.time()-T0:.0f}s)")
