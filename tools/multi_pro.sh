#!/bin/bash
# multi_pro.sh "SYM YEAR" ["SYM YEAR" ...] - for each job: download every NinjaTrader replay archive whose name starts
# with "SYM YEAR " (e.g. "GC 2026 Continuous", "GC 2026 Jul Continuous"), test, decode with a per-symbol copy of
# nrd_full.py, delete the archives, push the decoded days to nq-data/<sym><year>/.
set -u
LINK="https://mega.nz/folder/uJIiUB6Z#38a5fd3inUMqrsGLMshVqw"; DL="$HOME/Downloads"; BOT="$HOME/cvd-bot"; REPO="$HOME/nq-data-push"
export PATH="/Applications/MEGAcmd.app/Contents/MacOS:$PATH"
cd "$BOT" || exit 1
[ $# -gt 0 ] || { echo 'usage: bash multi_pro.sh "GC 2024" "GC 2026" ...'; exit 1; }
mega-whoami < /dev/null || { echo "Not logged in: mega-login <your Pro email>, then re-run"; exit 1; }
[ -s nq_all_handles.txt ] || python3 mega_list26.py "$LINK" > nq_all_handles.txt
for JOB in "$@"; do
  SYM=${JOB% *}; YR=${JOB#* }; low=$(echo "$SYM" | tr 'A-Z' 'a-z')
  echo; echo "########## $SYM $YR"
  awk -F'\t' -v s="$SYM $YR " '$3 ~ /Replay Data/ { n=$3; sub(/.*\//,"",n); if (index(n,s)==1) print }' nq_all_handles.txt > job_handles.txt
  [ -s job_handles.txt ] || { echo "  no archive named '$SYM $YR ...' - skipping"; continue; }
  awk -F'\t' '{n=$3; sub(/.*\//,"",n); printf "  %-45s %.2f GB\n",n,$2/1e9}' job_handles.txt
  sed -e "s#nq_full#${low}_full#g" -e "s#NQ_#${SYM}_#g" nrd_full.py > "${low}_nrd_full.py"
  rm -rf "${low}_full" "${low}_full_trades"; : > "log_${low}${YR}.txt"
  for base in $(cut -f3 job_handles.txt | sed -E 's#.*/##; s/\.[0-9]{3}$//; s/ /%20/g' | sort -u); do
    arc="${base//%20/ }"; echo "  --- $arc"
    free=$(df -g ~ | tail -1 | awk '{print $4}'); echo "  disk free ${free} GB"
    grep -F "$arc" job_handles.txt | while IFS=$'\t' read -r h s p; do
      f="$DL/$(basename "$p")"
      if [ -f "$f" ] && [ "$(stat -f%z "$f")" = "$s" ]; then echo "  have $(basename "$f")"; continue; fi
      rm -f "$f"; echo "  downloading $(basename "$f") ($((s/1000000)) MB)"
      mega-get "$LINK/file/$h" "$DL/" < /dev/null || { echo "  download failed - re-run the script"; exit 1; }
    done || exit 1
    first=$(ls "$DL/$arc".001 "$DL/$arc" 2>/dev/null | head -1)
    7zz t "$first" > "${low}_test.txt" 2>&1
    grep -q "Everything is Ok" "${low}_test.txt" || { echo "  ARCHIVE TEST FAILED:"; tail -8 "${low}_test.txt"; exit 1; }
    echo "  archive ok: $(grep -E '^Files:' ${low}_test.txt)"
    free=$(df -g ~ | tail -1 | awk '{print $4}'); [ "$free" -ge 6 ] || { echo "  STOP: only ${free} GB free"; exit 1; }
    caffeinate -i python3 "${low}_nrd_full.py" --archive "$first" >> "log_${low}${YR}.txt" 2>&1
    rm -f "$DL/$arc" "$DL/$arc".[0-9][0-9][0-9]
  done
  ok=$(grep -c ": ok " "log_${low}${YR}.txt"); sk=$(grep -c "SKIP book" "log_${low}${YR}.txt")
  echo "  decoded ok: $ok days | skipped (book match < 97%): $sk"
  [ "$ok" -gt 0 ] || { echo "  nothing decoded; paste me: tail -20 $BOT/log_${low}${YR}.txt"; continue; }
  cd "$REPO" && git sparse-checkout add "/${low}${YR}/" 2>/dev/null; git pull -q --rebase 2>/dev/null
  mkdir -p "${low}${YR}" && rm -f "${low}${YR}"/*.tar.part_*
  (cd "$BOT" && tar cf - "${low}_full" "${low}_full_trades") | split -b 90m - "${low}${YR}/${low}${YR}.tar.part_"
  git add "${low}${YR}" && git commit -q -m "$SYM $YR decoded depth+tape ($ok days)" && git push -q && echo "  pushed $(ls ${low}${YR} | wc -l | tr -d ' ') parts"
  cd "$BOT"; rm -rf "${low}_full" "${low}_full_trades"
done
echo; echo "=== ALL DONE"
