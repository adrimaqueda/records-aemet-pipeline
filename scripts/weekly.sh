#!/usr/bin/env bash
# Backup semanal de la DuckDB al dataset HF privado (EXTREMOS_HF_DB_REPO).
# Exporta solo las tablas fuente a parquet ZSTD (~75 MB), no los 342 MB del .duckdb.
#
# Crontab de ejemplo (`crontab -e`), domingos a las 04:00:
#
#   0 4 * * 0 ~/records-aemet-pipeline/scripts/weekly.sh >> ~/records-aemet-pipeline/weekly.log 2>&1

set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

# Cron arranca con un PATH mínimo, así que `uv` (instalado en ~/.local/bin)
# no suele estar disponible. Lo resolvemos buscando primero en el PATH y,
# si no, en las ubicaciones habituales de instalación.
UV="$(command -v uv || true)"
if [[ -z "$UV" ]]; then
  for candidate in "$HOME/.local/bin/uv" "$HOME/.cargo/bin/uv" /usr/local/bin/uv /opt/homebrew/bin/uv; do
    if [[ -x "$candidate" ]]; then
      UV="$candidate"
      break
    fi
  done
fi

if [[ -z "$UV" ]]; then
  echo "ERROR: no se encontró 'uv' ni en el PATH ni en las rutas habituales." >&2
  echo "Instálalo (https://docs.astral.sh/uv/) o ajusta la ruta en weekly.sh." >&2
  exit 127
fi

exec "$UV" run extremos-backup
