#!/bin/bash
# run_es.sh - decode every ES replay day (book + tape 08:25-15:00 CT) month by month from ES 7z archives.
# usage: bash run_es.sh "~/Downloads/ES 2025 Continuous.7z.001" [more archives]
# Extracts one month to ~/Downloads/ES_tmp, decodes with es_nrd_full.py into ~/cvd-bot/es_full + es_full_trades,
# deletes the month, moves on. Days without depth (book match < 97%) are skipped and listed in log_es.txt.
# Safe to stop and restart: finished days are skipped by the decoder.
cd ~/cvd-bot || exit 1
command -v 7zz >/dev/null || { echo "7zz not found"; exit 1; }
TMP="$HOME/Downloads/ES_tmp"
LOG="$HOME/cvd-bot/log_es.txt"
for A in "$@"; do
  [ -f "$A" ] || { echo "missing: $A"; continue; }
  for m in $(7zz l "$A" | grep -oE "20[0-9]{6}\.nrd" | cut -c1-6 | sort -u); do
    free=$(df -g ~ | tail -1 | awk '{print $4}')
    echo "=== $(basename "$A")  month $m  (free disk: ${free} GB) ===" | tee -a "$LOG"
    if [ "$free" -lt 4 ]; then echo "STOP: less than 4 GB free" | tee -a "$LOG"; exit 1; fi
    rm -rf "$TMP"
    7zz x "$A" -o"$TMP" "*${m}??.nrd" -r -aoa > /dev/null
    python3 es_nrd_full.py --nrd-dir "$TMP" > "$TMP.log" 2>&1
    cat "$TMP.log" >> "$LOG"
    echo "  files: $(find "$TMP" -name '*.nrd' | wc -l)   ok: $(grep -c ': ok ' "$TMP.log")   no depth: $(grep -c 'SKIP book match' "$TMP.log")" | tee -a "$LOG"
    rm -rf "$TMP" "$TMP.log"
  done
done
echo "ES days decoded in total: $(ls es_full/ES_*.parquet 2>/dev/null | wc -l)" | tee -a "$LOG"
