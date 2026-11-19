#!/bin/bash
# es_pro.sh - download ES archives through the logged-in MEGA Pro account (nested links into the shared folder),
# one year at a time: download -> 7z test -> decode (run_es.sh) -> delete archive; finally upload all ES days to GitHub.
LINK="https://mega.nz/folder/uJIiUB6Z#38a5fd3inUMqrsGLMshVqw"; LOG="$HOME/cvd-bot/log_es_pro.txt"; DL="$HOME/Downloads"
H="$HOME/cvd-bot/es_handles.txt"
cut -f1 "$H" | uniq > /tmp/es_years.txt
while IFS= read -r P; do
  echo "=== $P  $(date)" | tee -a "$LOG"
  awk -F'\t' -v p="$P" '$1==p {print $2"\t"$3}' "$H" > /tmp/es_parts.txt
  while IFS=$'\t' read -r name h; do
    tries=0
    until [ -s "$DL/$name" ]; do
      mega-get "$LINK/file/$h" "$DL/" < /dev/null >> "$LOG" 2>&1
      if [ ! -s "$DL/$name" ]; then tries=$((tries+1)); echo "  $name failed (try $tries) - retry in 10 min" | tee -a "$LOG"; sleep 600; fi
    done
    echo "  ok $name ($(du -h "$DL/$name" | cut -f1))" | tee -a "$LOG"
  done < /tmp/es_parts.txt
  A="$DL/$P.7z.001"; [ -f "$A" ] || A="$DL/$P.7z"
  if 7zz t "$A" > /dev/null 2>&1; then
    bash ~/cvd-bot/run_es.sh "$A" < /dev/null >> "$LOG" 2>&1
    echo "  decoded -> ES days now: $(ls ~/cvd-bot/es_full/ES_*.parquet | wc -l)   free disk: $(df -h ~ | tail -1 | awk '{print $4}')" | tee -a "$LOG"
    rm -f "$DL/$P.7z"*
  else
    echo "  ARCHIVE TEST FAILED: $A (kept for checking)" | tee -a "$LOG"
  fi
done < /tmp/es_years.txt
cd ~/nq_upload && mkdir -p es && rm -f es/es_all.tar.part_*
tar -cf - -C ~/cvd-bot es_full es_full_trades | split -b 90m - es/es_all.tar.part_
git add es && git commit -q -m "ES depth all years ($(ls ~/cvd-bot/es_full/ES_*.parquet | wc -l) days)" && git push >> "$LOG" 2>&1
echo "=== ALL DONE $(date): $(ls ~/cvd-bot/es_full/ES_*.parquet | wc -l) ES days uploaded" | tee -a "$LOG"
