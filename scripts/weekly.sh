#!/usr/bin/env bash
# Backup semanal de la DuckDB al dataset HF privado (EXTREMOS_HF_DB_REPO).
# Exporta solo las tablas fuente a parquet ZSTD (~75 MB), no los 342 MB del .duckdb.
# Crontab (`crontab -e`), domingos a las 04:00:
#
#   0 4 * * 0 /usr/local/bin/cron-alert "Backup DuckDB a HF (weekly)" ~/records-aemet-pipeline/scripts/weekly.sh >> ~/records-aemet-pipeline/weekly.log 2>&1
exec "$(dirname "$0")/run.sh" extremos-backup
