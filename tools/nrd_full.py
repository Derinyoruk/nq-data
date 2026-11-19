#!/usr/bin/env python3
"""
nrd_full.py - like nrd_compact.py + nrd_trades.py, but the BOOK and the TAPE run 08:25 -> 15:00 CT (not 11:45).
Needs nrd_compact.py in the same folder (reuses its decoder: load, ct_ns, book_snaps).

Per day:
  ~/cvd-bot/nq_full/NQ_YYYYMMDD.parquet          same columns as nq_compact, book filled to 15:00
  ~/cvd-bot/nq_full_trades/NQ_YYYYMMDD_trades.parquet   same columns as nq_trades (ms since 08:25 CT), to 15:00
Same rules as before: aggressor = trade >= prevailing L1 ask -> buy, <= bid -> sell, else tick rule
(CME check: identical to CME's aggressor flag); day skipped if the rebuilt book matches NT's own L1 < 97%.

Sources (any mix):
  --nrd-dir FOLDER            .nrd files already extracted (searched recursively)
  --archive FILE.7z.001       multi-part 7z archive; extracted ONE MONTH AT A TIME into a temp folder,
                              processed, then the temp folder is deleted (needs 7zz / 7z / 7za on PATH:
                              brew install sevenzip)
usage:
  python3 nrd_full.py --archive "~/Downloads/NQ 2025 Continuous.7z.001" --archive "~/Downloads/NQ 2024 Continuous.7z.001"
  python3 nrd_full.py --nrd-dir "~/Downloads/NQ 2026 Continuous 2"
Already finished days are skipped, so it can be stopped and restarted.
"""
import argparse, datetime as dt, glob, os, shutil, subprocess, tempfile, time
from multiprocessing import Pool, cpu_count
import numpy as np, pandas as pd
from nrd_compact import load, ct_ns, book_snaps

OUT_C = os.path.expanduser("~/cvd-bot/nq_full")
OUT_T = os.path.expanduser("~/cvd-bot/nq_full_trades")
SKIP = {"20260703", "20260907"}          # half days


def sides(ts1, mdt, px1):
    idx = np.arange(len(mdt))
    la = np.maximum.accumulate(np.where(mdt == 0, idx, -1)); lb = np.maximum.accumulate(np.where(mdt == 1, idx, -1))
    ist = mdt == 2
    tp = px1[ist]
    ask = np.where(la[ist] >= 0, px1[np.maximum(la[ist], 0)], 0); bid = np.where(lb[ist] >= 0, px1[np.maximum(lb[ist], 0)], 0)
    dpx = np.sign(np.diff(tp, prepend=tp[0])); ii = np.arange(len(dpx))
    nz = np.maximum.accumulate(np.where(dpx != 0, ii, 0)); tick_rule = dpx[nz]
    sd = np.where((ask > 0) & (tp >= ask), 1, np.where((bid > 0) & (tp <= bid), -1, tick_rule))
    return ist, la, lb, sd


def build_full(d, tick, L1, L2):
    ts1, mdt, px1, v1 = L1; ts2, sd2, op, pos, px2, v2 = L2
    ist, la, lb, sg = sides(ts1, mdt, px1)
    tt, tp, tv = ts1[ist], px1[ist], v1[ist]
    if len(tt) < 5000: return None, None, 0.0
    t_a, t_e = ct_ns(d, 8, 25), ct_ns(d, 15, 0)
    secs = np.arange(t_a // 10**9, t_e // 10**9, dtype=np.int64)
    k = tt // 10**9 - secs[0]; ok = (k >= 0) & (k < len(secs)); k, p_, v_, s_ = k[ok], tp[ok], tv[ok], sg[ok]
    df = pd.DataFrame({"k": k, "p": p_, "bv": np.where(s_ > 0, v_, 0), "sv": np.where(s_ < 0, v_, 0)})
    g = df.groupby("k")
    out = pd.DataFrame(index=np.arange(len(secs)))
    out["t"] = secs
    out["o"] = g.p.first(); out["h"] = g.p.max(); out["l"] = g.p.min(); out["c"] = g.p.last()
    out["buy_v"] = g.bv.sum(); out["sell_v"] = g.sv.sum(); out["n"] = g.size()
    out[["buy_v", "sell_v", "n"]] = out[["buy_v", "sell_v", "n"]].fillna(0).astype(np.int32)
    nb = len(secs)                                           # book for every second to 15:00
    qt = (secs[:nb] + 1) * 10**9
    P, S = book_snaps(ts2, sd2.astype(np.int64), op.astype(np.int64), pos.astype(np.int64), px2, v2, qt)
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
    # tape
    kk = (tt >= t_a) & (tt < t_e)
    tr = pd.DataFrame({"ms": ((tt[kk] - t_a) // 1_000_000).astype(np.int32), "px": tp[kk].astype(np.int32),
                       "sz": tv[kk].astype(np.int32), "side": sg[kk].astype(np.int8)})
    tr = tr.groupby(["ms", "px", "side"], sort=False, as_index=False).sz.sum()
    tr["sz"] = tr.sz.clip(0, 32767).astype(np.int16)
    return out, tr[["ms", "px", "sz", "side"]], match


def work(f):
    ds = os.path.basename(f)[:8]
    try: d = dt.datetime.strptime(ds, "%Y%m%d").date()
    except ValueError: return f, "skip (name)"
    if d.weekday() >= 5 or ds in SKIP: return f, "skip"
    dc = os.path.join(OUT_C, f"NQ_{ds}.parquet"); dtr = os.path.join(OUT_T, f"NQ_{ds}_trades.parquet")
    if os.path.exists(dc) and os.path.exists(dtr): return f, "already done"
    try:
        tick, L1, L2 = load(f, d)
        df, tr, match = build_full(d, tick, L1, L2)
    except Exception as ex:
        return f, f"FAILED {ex}"
    if df is None: return f, "skip (no trades)"
    if match < 0.97: return f, f"SKIP book match {match*100:.1f}%"
    df.to_parquet(dc, compression="zstd", index=False); tr.to_parquet(dtr, compression="zstd", index=False)
    return f, f"ok  match {match*100:.1f}%  {len(tr):,} prints"


def run_files(files, workers, T0):
    best = {}
    for f in files:
        key = os.path.basename(f)[:8]
        if key not in best or os.path.getsize(f) > os.path.getsize(best[key]): best[key] = f
    files = [best[k] for k in sorted(best)]
    if not files: return
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
    ap.add_argument("--workers", type=int, default=max(1, cpu_count() - 1))
    a = ap.parse_args()
    os.makedirs(OUT_C, exist_ok=True); os.makedirs(OUT_T, exist_ok=True)
    T0 = time.time()
    for dd in a.nrd_dir:
        run_files(glob.glob(os.path.join(os.path.expanduser(dd), "**", "*.nrd"), recursive=True), a.workers, T0)
    if a.archive:
        z = sevenzip()
        for arc in a.archive:
            arc = os.path.expanduser(arc)
            months = archive_months(arc, z)
            print(f"{os.path.basename(arc)}: months {months}", flush=True)
            for m in months:
                mark = os.path.join(OUT_C, f".done_{os.path.basename(arc)[:20]}_{m}")
                if os.path.exists(mark):
                    print(f"  {m}: done earlier, skip extract", flush=True); continue
                tmp = tempfile.mkdtemp(prefix=f"nrd_{m}_", dir=os.path.dirname(arc))
                print(f"  {m}: extracting ...", flush=True)
                r = subprocess.run([z, "x", arc, f"-o{tmp}", f"-ir!{m}*.nrd", "-y"], capture_output=True, text=True)
                if r.returncode != 0: print(r.stderr[-500:], flush=True)
                got = glob.glob(os.path.join(tmp, "**", "*.nrd"), recursive=True)
                print(f"  {m}: {len(got)} .nrd files", flush=True)
                run_files(got, a.workers, T0)
                shutil.rmtree(tmp, ignore_errors=True)
                if got: open(mark, "w").close()
    n = len(glob.glob(os.path.join(OUT_C, "NQ_*.parquet")))
    print(f"\n{n} full days in {OUT_C}  ({time.time()-T0:.0f}s)")
