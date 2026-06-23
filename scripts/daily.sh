#!/usr/bin/env bash
# Entrypoint para el cron de la Raspberry Pi.
# Carga variables de entorno desde .env y ejecuta el ciclo de actualización.
#
# Programación: dos veces al día, a las 09:00 y a las 21:00 (hora local).
# Crontab de ejemplo (`crontab -e`):
#
#   0 9,21 * * * ~/records-aemet-pipeline/scripts/daily.sh >> ~/records-aemet-pipeline/daily.log 2>&1
#
# Correr 2×/día mantiene fresco el provisional (la pasada de la tarde ya ve el
# pico de tmax del día); los extremos de cada día se acumulan entre pasadas
# (merge en provisional.py), no se reemplazan.

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
  echo "Instálalo (https://docs.astral.sh/uv/) o ajusta la ruta en daily.sh." >&2
  exit 127
fi

exec "$UV" run extremos-daily
