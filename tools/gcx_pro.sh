#!/bin/bash
# gcx_pro.sh v2 - re-decode GC replay archives with BOTH the loader and the decoder starting at 07:00/07:15 CT
# (covers the 07:20 CT COMEX open) into ~/cvd-bot/gcx_full + gcx_full_trades (ms since 07:15), push to nq-data/gcx<year>/.
# Uses patched copies nrd_compact_gcx.py + gcx_nrd_full.py; the normal NQ/ES decoders are not changed.
set -u
LINK="https://mega.nz/folder/uJIiUB6Z#38a5fd3inUMqrsGLMshVqw"; DL="$HOME/Downloads"; BOT="$HOME/cvd-bot"; REPO="$HOME/nq-data-push"
export PATH="/Applications/MEGAcmd.app/Contents/MacOS:$PATH"
cd "$BOT" || exit 1
grep -q "t_from = ct_ns(d, 8, 0)" nrd_compact.py || { echo "loader line not found - paste me: grep -n t_from nrd_compact.py"; exit 1; }
grep -q "t_a, t_e = ct_ns(d, 8, 25), ct_ns(d, 15, 0)" nrd_full.py || { echo "decoder window line not found - paste me: grep -n ct_ns nrd_full.py"; exit 1; }
grep -q "from nrd_compact import" nrd_full.py || { echo "import line not found - paste me: grep -n import nrd_full.py"; exit 1; }
sed -e "s#t_from = ct_ns(d, 8, 0)#t_from = ct_ns(d, 7, 0)#" nrd_compact.py > nrd_compact_gcx.py
sed -e "s#t_a, t_e = ct_ns(d, 8, 25), ct_ns(d, 15, 0)#t_a, t_e = ct_ns(d, 7, 15), ct_ns(d, 13, 30)#" \
    -e "s#from nrd_compact import#from nrd_compact_gcx import#" \
    -e "s#nq_full#gcx_full#g" -e "s#NQ_#GC_#g" nrd_full.py > gcx_nrd_full.py
grep -q "ct_ns(d, 7, 0)" nrd_compact_gcx.py && grep -q "ct_ns(d, 7, 15)" gcx_nrd_full.py && grep -q "from nrd_compact_gcx import" gcx_nrd_full.py || { echo "patch failed"; exit 1; }
mega-whoami < /dev/null || { echo "Not logged in: mega-login <your Pro email>, then re-run"; exit 1; }
[ -s nq_all_handles.txt ] || python3 mega_list26.py "$LINK" > nq_all_handles.txt
for YR in 2024 2025 2026; do
  echo; echo "########## GC $YR (07:15-13:30 CT)"
  awk -F'\t' -v s="GC $YR " '$3 ~ /Replay Data/ { n=$3; sub(/.*\//,"",n); if (index(n,s)==1) print }' nq_all_handles.txt > job_handles.txt
  [ -s job_handles.txt ] || { echo "  no GC $YR archive - skipping"; continue; }
  rm -rf gcx_full gcx_full_trades; : > "log_gcx${YR}.txt"
  for base in $(cut -f3 job_handles.txt | sed -E 's#.*/##; s/\.[0-9]{3}$//; s/ /%20/g' | sort -u); do
    arc="${base//%20/ }"; echo "  --- $arc (disk free $(df -g ~ | tail -1 | awk '{print $4}') GB)"
    grep -F "$arc" job_handles.txt | while IFS=$'\t' read -r h s p; do
      f="$DL/$(basename "$p")"
      if [ -f "$f" ] && [ "$(stat -f%z "$f")" = "$s" ]; then echo "  have $(basename "$f")"; continue; fi
      rm -f "$f"; echo "  downloading $(basename "$f") ($((s/1000000)) MB)"
      mega-get "$LINK/file/$h" "$DL/" < /dev/null || { echo "  download failed - re-run"; exit 1; }
    done || exit 1
    first=$(ls "$DL/$arc".001 "$DL/$arc" 2>/dev/null | head -1)
    7zz t "$first" > gcx_test.txt 2>&1; grep -q "Everything is Ok" gcx_test.txt || { echo "  ARCHIVE TEST FAILED"; tail -8 gcx_test.txt; exit 1; }
    [ "$(df -g ~ | tail -1 | awk '{print $4}')" -ge 6 ] || { echo "  STOP: under 6 GB free"; exit 1; }
    caffeinate -i python3 gcx_nrd_full.py --archive "$first" >> "log_gcx${YR}.txt" 2>&1
    rm -f "$DL/$arc" "$DL/$arc".[0-9][0-9][0-9]
  done
  ok=$(grep -c ": ok " "log_gcx${YR}.txt"); echo "  decoded ok: $ok days | skipped: $(grep -c 'SKIP book' log_gcx${YR}.txt)"
  [ "$ok" -gt 0 ] || { echo "  nothing decoded; paste me: tail -20 $BOT/log_gcx${YR}.txt"; continue; }
  chk=$(python3 -c "
import glob,pandas as pd
f=sorted(glob.glob('gcx_full_trades/GC_*_trades.parquet'))[len(glob.glob('gcx_full_trades/GC_*'))//2]
print(int(pd.read_parquet(f,columns=['ms']).ms.min()))")
  echo "  first print of a mid-year day: ${chk} ms after 07:15 (must be < 300000 = before 07:20)"
  [ "$chk" -lt 300000 ] || { echo "  WINDOW STILL WRONG - stopping, paste me this output"; exit 1; }
  cd "$REPO" && git sparse-checkout add "/gcx${YR}/" 2>/dev/null; git pull -q --rebase 2>/dev/null
  mkdir -p "gcx${YR}" && rm -f "gcx${YR}"/*.tar.part_*
  (cd "$BOT" && tar cf - gcx_full gcx_full_trades) | split -b 90m - "gcx${YR}/gcx${YR}.tar.part_"
  git add -A "gcx${YR}" && git commit -q -m "GC $YR decoded 07:15-13:30 CT v2 ($ok days)" && git push -q && echo "  pushed $(ls gcx${YR} | wc -l | tr -d ' ') parts"
  cd "$BOT"; rm -rf gcx_full gcx_full_trades
done
echo; echo "=== ALL DONE"
