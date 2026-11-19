#!/bin/bash
# nqold_pro.sh - NQ 2019-2023 replay archives -> decode (normal 08:25-15:00 CT window) into ~/cvd-bot/nqold_full +
# nqold_full_trades (separate from nq_full), push each year to nq-data/nqold<year>/, then clear the local copy.
set -u
LINK="https://mega.nz/folder/uJIiUB6Z#38a5fd3inUMqrsGLMshVqw"; DL="$HOME/Downloads"; BOT="$HOME/cvd-bot"; REPO="$HOME/nq-data-push"
export PATH="/Applications/MEGAcmd.app/Contents/MacOS:$PATH"
cd "$BOT" || exit 1
sed -e "s#nq_full#nqold_full#g" nrd_full.py > nqold_nrd_full.py
grep -q "nqold_full" nqold_nrd_full.py || { echo "decoder copy failed"; exit 1; }
mega-whoami < /dev/null || { echo "Not logged in: mega-login <your Pro email>, then re-run"; exit 1; }
[ -s nq_all_handles.txt ] || python3 mega_list26.py "$LINK" > nq_all_handles.txt
echo "NQ replay archives on MEGA:"; awk -F'\t' '$3 ~ /Replay Data/ { n=$3; sub(/.*\//,"",n); if (n ~ /^NQ [0-9]/) print "  "n }' nq_all_handles.txt | sed -E 's/\.[0-9]{3}$//' | sort -u
for YR in 2023 2022 2021 2020 2019; do
  echo; echo "########## NQ $YR"
  awk -F'\t' -v s="NQ $YR " '$3 ~ /Replay Data/ { n=$3; sub(/.*\//,"",n); if (index(n,s)==1) print }' nq_all_handles.txt > job_handles.txt
  [ -s job_handles.txt ] || { echo "  no NQ $YR archive - skipping"; continue; }
  rm -rf nqold_full nqold_full_trades; : > "log_nqold${YR}.txt"
  for base in $(cut -f3 job_handles.txt | sed -E 's#.*/##; s/\.[0-9]{3}$//; s/ /%20/g' | sort -u); do
    arc="${base//%20/ }"; echo "  --- $arc (disk free $(df -g ~ | tail -1 | awk '{print $4}') GB)"
    grep -F "$arc" job_handles.txt | while IFS=$'\t' read -r h s p; do
      f="$DL/$(basename "$p")"
      if [ -f "$f" ] && [ "$(stat -f%z "$f")" = "$s" ]; then echo "  have $(basename "$f")"; continue; fi
      rm -f "$f"; echo "  downloading $(basename "$f") ($((s/1000000)) MB)"
      mega-get "$LINK/file/$h" "$DL/" < /dev/null || { echo "  download failed - re-run"; exit 1; }
    done || exit 1
    first=$(ls "$DL/$arc".001 "$DL/$arc" 2>/dev/null | head -1)
    7zz t "$first" > nqold_test.txt 2>&1; grep -q "Everything is Ok" nqold_test.txt || { echo "  ARCHIVE TEST FAILED"; tail -8 nqold_test.txt; exit 1; }
    [ "$(df -g ~ | tail -1 | awk '{print $4}')" -ge 6 ] || { echo "  STOP: under 6 GB free"; exit 1; }
    caffeinate -i python3 nqold_nrd_full.py --archive "$first" >> "log_nqold${YR}.txt" 2>&1
    rm -f "$DL/$arc" "$DL/$arc".[0-9][0-9][0-9]
  done
  ok=$(grep -c ": ok " "log_nqold${YR}.txt"); echo "  decoded ok: $ok days | skipped: $(grep -c 'SKIP book' log_nqold${YR}.txt)"
  [ "$ok" -gt 0 ] || { echo "  nothing decoded; paste me: tail -20 $BOT/log_nqold${YR}.txt"; continue; }
  cd "$REPO" && git sparse-checkout add "/nqold${YR}/" 2>/dev/null; git pull -q --rebase 2>/dev/null
  mkdir -p "nqold${YR}" && rm -f "nqold${YR}"/*.tar.part_*
  (cd "$BOT" && tar cf - nqold_full nqold_full_trades) | split -b 90m - "nqold${YR}/nqold${YR}.tar.part_"
  git add -A "nqold${YR}" && git commit -q -m "NQ $YR decoded depth+tape ($ok days)" && git push -q && echo "  pushed $(ls nqold${YR} | wc -l | tr -d ' ') parts"
  cd "$BOT"; rm -rf nqold_full nqold_full_trades
done
echo; echo "=== ALL DONE"
