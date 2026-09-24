#!/usr/bin/env bash
# Lanzador común de los crons del pipeline: ejecuta con uv, desde la raíz del
# proyecto, el comando `extremos-*` que se le pase. El .env lo carga config.py.
# daily.sh y weekly.sh son atajos a este (el crontab los llama por su ruta).
set -euo pipefail
cd "$(dirname "$0")/.."
# Cron arranca con un PATH mínimo, sin ~/.local/bin (donde está uv).
export PATH="$HOME/.local/bin:$PATH"
exec uv run "$@"
