"""Rutas y constantes compartidas."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

PIPELINE_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PIPELINE_ROOT / "data"
DB_PATH = DATA_DIR / "aemet.duckdb"
OUTPUTS_DIR = PIPELINE_ROOT / "outputs"

# Carga pipeline/.env si existe. No falla si no está (entornos como GitHub Actions
# pasan las variables ya inyectadas).
load_dotenv(PIPELINE_ROOT / ".env")

HF_SOURCE_REPO = "datania/aemet"
# Dataset PÚBLICO donde se publican los JSONs ligeros (~28 MB) que consume la app.
HF_TARGET_REPO = os.environ.get("EXTREMOS_HF_REPO", "")
# Dataset PRIVADO donde se respalda la DuckDB (como parquet de las tablas fuente,
# ~75 MB). Sirve también para hacer bootstrap de la Pi sin re-backfill (ver backup.py).
HF_DB_REPO = os.environ.get("EXTREMOS_HF_DB_REPO", "")

AEMET_API_KEY = os.environ.get("AEMET_API_KEY", "")
AEMET_BASE_URL = "https://opendata.aemet.es/opendata"
AEMET_RATE_LIMIT_PER_MIN = 45

# Año mínimo a considerar en el backfill. Antes de 1975 la cobertura es anecdótica
# (apenas 16 estaciones, datania solo tiene 1920-1922).
MIN_BACKFILL_YEAR = int(os.environ.get("EXTREMOS_MIN_YEAR", "1975"))

# Estación activa = al menos N días reportados en los últimos 12 meses.
ACTIVE_STATION_MIN_DAYS = int(os.environ.get("EXTREMOS_ACTIVE_MIN_DAYS", "180"))

# "Madurez" de una serie para que sus récords cuenten como batidos. Un récord
# solo cuenta si la estación lleva ≥ RECORD_WARMUP_DAYS con datos desde que
# empezó (o desde que se reanudó tras un hueco largo). Durante ese primer año la
# serie aún está estableciendo su envolvente estacional: casi todo es "récord"
# por el simple avance de las estaciones, no por un extremo real. Generaliza la
# antigua regla del "primer año natural", que solo cubría el primer año de
# CALENDARIO y se dejaba fuera arranques a mitad de año, series dispersas y
# huecos (esto último inflaba los "mayores saltos": un récord nuevo se medía
# contra una base rancia fijada antes del hueco). Ver records.py.
RECORD_WARMUP_DAYS = int(os.environ.get("EXTREMOS_RECORD_WARMUP_DAYS", "365"))
# Un hueco de cobertura de ≥ este nº de días reinicia el contador de madurez: la
# estación vuelve a estar "estrenándose" y su récord vigente puede ser un valor
# fuera de temporada que ya no representa la envolvente real.
RECORD_GAP_RESET_DAYS = int(os.environ.get("EXTREMOS_RECORD_GAP_RESET_DAYS", "180"))

# Tamaño mínimo (en días) de un hueco de cobertura para exportarlo en el bloque
# `sinDatos` de cada estación, que la app pinta como overlay sobre los gráficos.
# Los huecos más cortos (la mitad son de 1 solo día) son invisibles en un eje de
# años y solo añaden ruido y peso al JSON, así que no se exportan.
HUECO_MIN_DIAS = int(os.environ.get("EXTREMOS_HUECO_MIN_DIAS", "7"))

# AEMET publica las climatologías diarias con ~4 días de retraso (su FAQ oficial,
# v1.4 jul-2025, §4.2: "aproximadamente 4 días"). Usamos 3, por debajo de ese
# retraso, para priorizar la frescura de los datos: pedimos el día más reciente
# aunque a veces AEMET aún no lo haya publicado. En ese caso el chunk más fresco
# devuelve un 404 "sin datos", que tratamos como condición normal (no como error;
# ver AemetNoData en aemet.py) y se rellena solo en la siguiente pasada.
# https://opendata.aemet.es/centrodedescargas/docs/FAQs220424.pdf
INGEST_LAG_DAYS = 3

# Un récord provisional que sigue sin confirmarse pasados estos días se purga:
# AEMET ya debería haber publicado el definitivo; si no reemplazó la fila es que
# la estación nunca reportó ese día. Debe ser holgadamente mayor que el retraso
# real de AEMET para no borrar un provisional a punto de confirmarse.
PROVISIONAL_MAX_AGE_DAYS = int(
    os.environ.get("EXTREMOS_PROVISIONAL_MAX_AGE_DAYS", "15")
)
