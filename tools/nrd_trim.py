#!/usr/bin/env python3
"""nrd_trim.py — standalone: cut ONE NinjaTrader 8 replay day (.nrd) down to a small window (default 08:40-09:20 CT)
and write it as one small Parquet (L1 + L2 in one table) for upload / format check.
Decoder copied from nrd_to_parquet.py (same .nrd format rules).
usage: python3 nrd_trim.py /path/to/20260805.nrd [--start 08:40] [--end 09:20]"""
import argparse, datetime as dt, os, struct, sys
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo
import numpy as np, pyarrow as pa, pyarrow.compute as pc, pyarrow.parquet as pq

EPOCH_TICKS = 621355968000000000
HDR = struct.Struct("<diddddd i qq q")
N_SLOTS = 44
TSSIZE = (0, 1, 2, 4)
VOLMAP = {0: (0, 1), 1: (1, 1), 2: (1, 100), 3: (1, 500), 4: (1, 1000), 5: (2, 1), 6: (4, 1)}

class FormatError(RuntimeError): pass

def parse_headers(head):
    slots = []
    for s in range(N_SLOTS):
        (last, cnt, pmax, pmin, pfirst, _one, ticksz, _flag, t0, t1, volsum) = HDR.unpack_from(head, s * 80)
        slots.append(dict(count=cnt, first=pfirst, tick=ticksz, t0=t0, t1=t1))
    return slots

def decode(buf, slots):
    tick = next((s["tick"] for s in slots if s["count"]), None)
    if tick is None: raise FormatError("empty file")
    A = {k: [] for k in ("ts", "mdt", "px", "vol")}
    B = {k: [] for k in ("ts", "side", "op", "pos", "px", "vol")}
    cur = [round(slots[k]["first"] / tick) for k in range(10)]
    l2cur = [round(slots[10]["first"] / tick), round(slots[11]["first"] / tick)]
    t = min(s["t0"] for s in slots if s["count"])
    n = len(buf); i = 0
    while i < n:
        if i + 3 > n: raise FormatError(f"truncated record header at offset {i}")
        m1 = buf[i]; m2 = buf[i + 1]; info = buf[i + 2]; j = i + 3
        tscode = m1 & 3
        if m1 & 4:
            if tscode == 0: t += int.from_bytes(buf[j:j + 8], "big"); j += 8
            else:
                sz = TSSIZE[tscode]; t += int.from_bytes(buf[j:j + sz], "big") * 10_000_000; j += sz
        elif tscode == 1: t += buf[j]; j += 1
        elif tscode == 2: t += (buf[j] << 8) | buf[j + 1]; j += 2
        elif tscode == 3: t += int.from_bytes(buf[j:j + 4], "big"); j += 4
        pbits = m1 & 0x18
        if info & 0xC0 == 0xC0:                                   # L1
            mdt = m2 >> 5; nib = m2 & 0x1F
            if nib == 0x1F: mdt += 8; nib = 0
            c = cur[mdt]
            if nib: c += nib - 15 if nib <= 14 else nib - 14
            elif pbits == 0x08: c -= 15
            elif pbits == 0x10: c += buf[j] - 0x80; j += 1
            elif pbits == 0x18: c += int.from_bytes(buf[j:j + 4], "big") - 0x80000000; j += 4
            cur[mdt] = c
            vc = (m1 >> 5) & 7
            if vc not in VOLMAP: raise FormatError(f"unknown volume code {vc} at {i}")
            sz, mult = VOLMAP[vc]; vol = int.from_bytes(buf[j:j + sz], "big") * mult if sz else 0; j += sz
            A["ts"].append((t - EPOCH_TICKS) * 100); A["mdt"].append(mdt); A["px"].append(c * tick); A["vol"].append(vol)
        else:                                                     # L2
            side = 1 if (m2 & 0x80) else 0
            c = l2cur[side]
            if pbits == 0x08: c += buf[j] - 0x80; j += 1
            elif pbits == 0x10: c += int.from_bytes(buf[j:j + 2], "big") - 0x8000; j += 2
            elif pbits == 0x18: c += int.from_bytes(buf[j:j + 4], "big") - 0x80000000; j += 4
            l2cur[side] = c
            vc = (m1 >> 5) & 7
            if vc not in VOLMAP: raise FormatError(f"unknown L2 volume code {vc} at {i}")
            sz, mult = VOLMAP[vc]; vol = int.from_bytes(buf[j:j + sz], "big") * mult if sz else 0; j += sz
            B["ts"].append((t - EPOCH_TICKS) * 100); B["side"].append(side)
            B["op"].append(2 if (info & 0x80) else (1 if (info & 0x40) else 0))
            B["pos"].append(info & 0x3F); B["px"].append(c * tick); B["vol"].append(vol)
        i = j
    if i != n: raise FormatError(f"stream not fully consumed: stopped at {i} of {n}")
    ndp = max(0, -Decimal(str(tick)).as_tuple().exponent)
    L1 = (np.array(A["ts"], np.int64), np.array(A["mdt"], np.int8), np.round(np.array(A["px"]), ndp), np.array(A["vol"], np.int64))
    L2 = (np.array(B["ts"], np.int64), np.array(B["side"], np.int8), np.array(B["op"], np.int8),
          np.array(B["pos"], np.int32), np.round(np.array(B["px"]), ndp), np.array(B["vol"], np.int64))
    return L1, L2

ap = argparse.ArgumentParser()
ap.add_argument("nrd")
ap.add_argument("--start", default="08:40")
ap.add_argument("--end", default="09:20")
args = ap.parse_args()
p = Path(os.path.expanduser(args.nrd))
raw = p.read_bytes()
print(f"file {p.name}: {len(raw)/1e6:.1f} MB, decoding (pure Python, can take a few minutes) ...", flush=True)
(ts1, mdt, px1, v1), (ts2, side, op, pos, px2, v2) = decode(raw[N_SLOTS * 80:], parse_headers(raw[:N_SLOTS * 80]))
day = dt.datetime.strptime(p.stem, "%Y%m%d").date()
CT = ZoneInfo("America/Chicago")
def ns(hhmm):
    h, m = map(int, hhmm.split(":"))
    return int(dt.datetime(day.year, day.month, day.day, h, m, tzinfo=CT).timestamp() * 1e9)
a, b = ns(args.start), ns(args.end)
print(f"full day: L1 {len(ts1):,} events, L2 {len(ts2):,} events, "
      f"{dt.datetime.fromtimestamp(ts1.min()/1e9, CT)} -> {dt.datetime.fromtimestamp(ts1.max()/1e9, CT)} CT")
k1 = (ts1 >= a) & (ts1 < b)
w2 = (ts2 >= a - 120 * 10**9) & (ts2 < b)                 # +2 min warm-up so the book can be rebuilt
n1, n2 = int(k1.sum()), int(w2.sum())
t = pa.table({
    "ts":    pa.array(np.r_[ts1[k1], ts2[w2]], pa.int64()),
    "lvl":   pa.array(np.r_[np.ones(n1, np.int8), np.full(n2, 2, np.int8)]),
    "type":  pa.array(np.r_[mdt[k1], side[w2]].astype(np.int8)),   # L1: 0 ask 1 bid 2 last ... | L2: 0 ask 1 bid
    "op":    pa.array(np.r_[np.full(n1, -1, np.int8), op[w2]]),     # L2: 0 add 1 update 2 remove
    "pos":   pa.array(np.r_[np.full(n1, -1, np.int32), pos[w2]]),
    "price": pa.array(np.r_[px1[k1], px2[w2]]),
    "vol":   pa.array(np.r_[v1[k1], v2[w2]].astype(np.int64)),
})
t = t.take(pc.sort_indices(t, sort_keys=[("ts", "ascending")]))
out = Path.home() / "cvd-bot" / f"TRIM_{p.stem}.parquet"
pq.write_table(t, out, compression="zstd")
print(f"window {args.start}-{args.end} CT: L1 {n1:,}, L2 {n2:,} (incl. 2 min warm-up) -> {out}  {os.path.getsize(out)/1e6:.1f} MB")
print("L2 positions seen:", sorted(set(pos[w2].tolist()))[:20], "| L1 types present:", sorted(set(mdt[k1].tolist())))
