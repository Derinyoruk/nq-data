#!/bin/bash
# nq26_pro.sh - NQ Jan-Jun 2026 replay data: download (Pro account), test, decode with nrd_full.py, delete archive,
# push Jan-Jun 2026 days to nq-data/nq26h1/. Re-run safe: parts with the right size are not downloaded again.
set -u
LINK="https://mega.nz/folder/uJIiUB6Z#38a5fd3inUMqrsGLMshVqw"
DL="$HOME/Downloads"; BOT="$HOME/cvd-bot"
export PATH="/Applications/MEGAcmd.app/Contents/MacOS:$PATH"
cd "$BOT" || { echo "no ~/cvd-bot folder"; exit 1; }
[ -f nrd_full.py ] || { echo "nrd_full.py missing in ~/cvd-bot"; exit 1; }
command -v 7zz >/dev/null || { echo "7zz missing: brew install sevenzip"; exit 1; }
command -v mega-get >/dev/null || { echo "MEGAcmd missing"; exit 1; }
python3 -c "import Crypto" 2>/dev/null || pip3 install --quiet pycryptodome
mega-whoami < /dev/null || { echo "Not logged in. Run:  mega-login <your Pro email>   then re-run."; exit 1; }

echo "=== 1. listing the MEGA folder"
python3 mega_list26.py "$LINK" > nq_all_handles.txt || { echo "listing failed"; exit 1; }
python3 - <<'EOF'
import re
keep=[]
for line in open("nq_all_handles.txt"):
    h,s,p=line.rstrip("\n").split("\t"); b=p.split("/")[-1]
    if "Replay Data" in p and re.match(r"NQ 2026 ",b) and not re.search(r"Jul|Aug|Sep|Oct|Nov|Dec",b,re.I): keep.append(line)
open("nq26_handles.txt","w").writelines(keep)
EOF
[ -s nq26_handles.txt ] || { echo "No NQ 2026 replay archive found. Paste me: grep -i nq nq_all_handles.txt | cut -f3"; exit 1; }
echo "archives:"; awk -F'\t' '{printf "  %-60s %.2f GB\n",$3,$2/1e9}' nq26_handles.txt

echo "=== 2. download (skips parts already here)"
while IFS=$'\t' read -r h s p; do
  f="$DL/$(basename "$p")"
  if [ -f "$f" ] && [ "$(stat -f%z "$f")" = "$s" ]; then echo "  have $(basename "$f")"; continue; fi
  rm -f "$f"; echo "  downloading $(basename "$f") ($((s/1000000)) MB)"
  mega-get "$LINK/file/$h" "$DL/" < /dev/null || { echo "  download failed: $(basename "$f") - re-run"; exit 1; }
done < nq26_handles.txt
first="$DL/NQ 2026 Continuous.7z.001"
[ -f "$first" ] || { echo "first part missing"; exit 1; }

echo "=== 3. testing the archive (a few minutes)"
7zz t "$first" > "$BOT/nq26_test.txt" 2>&1
if grep -q "Everything is Ok" "$BOT/nq26_test.txt"; then
  echo "  archive ok: $(grep -E '^Files:' "$BOT/nq26_test.txt")"
else
  echo "  ARCHIVE TEST FAILED - paste me this:"; tail -15 "$BOT/nq26_test.txt"; exit 1
fi
7zz l "$first" | grep -oE "20[0-9]{6}\.nrd" | cut -c1-6 | sort | uniq -c | sed 's/^/  days per month: /'

free=$(df -g ~ | tail -1 | awk '{print $4}')
if [ "$free" -lt 6 ]; then
  echo "STOP: only ${free} GB free; one month unpacks to ~4-5 GB. Biggest folders:"
  du -sh ~/cvd-bot/* ~/Downloads/* 2>/dev/null | sort -h | tail -12; exit 1
fi

echo "=== 4. decoding month by month (free: ${free} GB)"
caffeinate -i python3 nrd_full.py --archive "$first" 2>&1 | tee -a log_nq26.txt | grep -E "months|\.nrd files|full days|FAIL|rror"
for m in 01 02 03 04 05 06; do
  echo "  2026-$m: $(ls nq_full/NQ_2026${m}*.parquet 2>/dev/null | wc -l | tr -d ' ') days book, $(ls nq_full_trades/NQ_2026${m}*_trades.parquet 2>/dev/null | wc -l | tr -d ' ') days tape"
done
n=$(ls nq_full/NQ_20260[1-6]*.parquet 2>/dev/null | wc -l | tr -d ' ')
[ "$n" -gt 100 ] || { echo "only $n days decoded - archive kept; paste me the lines above"; exit 1; }
rm -f "$DL/NQ 2026 Continuous.7z".[0-9][0-9][0-9]; echo "  archive deleted"

echo "=== 5. pack + push to GitHub (Derinyoruk/nq-data, folder nq26h1/)"
REPO=""
for c in "$HOME/nq-data" "$HOME/cvd-bot/nq-data" "$HOME/Downloads/nq-data" "$HOME/Documents/nq-data"; do
  [ -d "$c/.git" ] && REPO="$c" && break
done
if [ -z "$REPO" ]; then
  REPO="$HOME/nq-data-push"
  if [ ! -d "$REPO/.git" ]; then
    git clone --depth 1 --filter=blob:none --no-checkout https://github.com/Derinyoruk/nq-data.git "$REPO" || exit 1
    (cd "$REPO" && git sparse-checkout set --no-cone /nq26h1/ && git checkout) || exit 1
  fi
fi
cd "$REPO" && git pull --quiet --rebase 2>/dev/null; mkdir -p nq26h1 && rm -f nq26h1/nq26h1.tar.part_*
(cd "$BOT" && tar cf - nq_full/NQ_20260[1-6]*.parquet nq_full_trades/NQ_20260[1-6]*_trades.parquet) | split -b 90m - nq26h1/nq26h1.tar.part_
ls nq26h1 | wc -l | xargs echo "  parts:"
git add nq26h1 && git commit -q -m "NQ Jan-Jun 2026 decoded depth+tape" && git push -q && echo "=== ALL DONE: pushed $(ls nq26h1 | wc -l | tr -d ' ') parts" || echo "push failed - paste me the last lines"
