"""Incremental: descarga datos nuevos desde la API de AEMET y los inserta en DuckDB.

Lógica:
1. Calcula la ventana [start, end]:
   - start = max(fecha) en observations + 1d  (o un default si la tabla está vacía)
   - end   = hoy - INGEST_LAG_DAYS  (AEMET publica con retraso)
2. Trocea la ventana en chunks de máx 31 días (límite de la API para todasestaciones).
3. Para cada chunk: AEMET → normalize → INSERT OR REPLACE en observations.
4. Opcionalmente refresca la tabla stations con el inventario actual.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, timedelta

import duckdb
import polars as pl

from extremos.aemet import AemetClient, AemetError, AemetNoData
from extremos.config import INGEST_LAG_DAYS
from extremos.db import connect
from extremos.parsing import normalize_observation, normalize_station

log = logging.getLogger("extremos.fetch")

DEFAULT_BACKSTOP = date(2024, 1, 1)  # solo se usa si la DB está vacía
CHUNK_DAYS = 15  # límite real de la API AEMET para todasestaciones


def _last_obs_date(con: duckdb.DuckDBPyConnection) -> date | None:
    # Solo definitivos: si contáramos los provisionales (de hoy/ayer), la
    # ventana incremental saltaría por delante de `end` y dejaríamos de bajar
    # el diario definitivo del hueco, que nunca reemplazaría a los provisionales.
    row = con.execute(
        "SELECT MAX(fecha) FROM observations WHERE NOT COALESCE(provisional, FALSE)"
    ).fetchone()
    return row[0] if row and row[0] else None


def _chunk_ranges(start: date, end: date, days: int = CHUNK_DAYS) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=days - 1), end)
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks


def _insert_observations(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> int:
    if not rows:
        return 0
    df = pl.DataFrame(rows, schema={
        "indicativo": pl.Utf8, "fecha": pl.Utf8,
        "tmed": pl.Float64, "tmin": pl.Float64, "tmax": pl.Float64,
        "horatmin": pl.Utf8, "horatmax": pl.Utf8,
        "prec": pl.Float64, "sol": pl.Float64,
        "hr_media": pl.Float64, "vel_media": pl.Float64,
        "pres_max": pl.Float64, "pres_min": pl.Float64,
    }).with_columns(
        pl.col("fecha").str.to_date(),
        # Dato definitivo: marca provisional=FALSE explícitamente. Imprescindible
        # porque INSERT OR REPLACE BY NAME sólo actualiza las columnas presentes,
        # así que un día que antes era provisional debe pisar también ese flag.
        pl.lit(False).alias("provisional"),
    )
    con.register("incoming", df)
    try:
        con.execute("INSERT OR REPLACE INTO observations BY NAME SELECT * FROM incoming")
    finally:
        con.unregister("incoming")
    return len(rows)


def _refresh_stations(con: duckdb.DuckDBPyConnection, client: AemetClient) -> int:
    log.info("Refrescando inventario de estaciones…")
    raw = client.inventory_stations()
    rows = [normalize_station(r) for r in raw if r.get("indicativo")]
    if not rows:
        return 0
    df = pl.DataFrame(rows, schema={
        "indicativo": pl.Utf8, "nombre": pl.Utf8, "provincia": pl.Utf8,
        "altitud": pl.Int64, "latitud": pl.Float64, "longitud": pl.Float64,
        "indsinop": pl.Utf8,
    })
    con.register("incoming_st", df)
    try:
        con.execute("INSERT OR REPLACE INTO stations BY NAME SELECT * FROM incoming_st")
    finally:
        con.unregister("incoming_st")
    return len(rows)


def fetch_range(con: duckdb.DuckDBPyConnection, client: AemetClient,
                start: date, end: date) -> int:
    total = 0
    for chunk_ini, chunk_fin in _chunk_ranges(start, end):
        log.info("AEMET %s → %s", chunk_ini, chunk_fin)
        try:
            raw = client.daily_observations(
                chunk_ini.isoformat(), chunk_fin.isoformat()
            )
        except AemetNoData:
            # AEMET aún no ha publicado el definitivo de ese día (retraso ~4-5d).
            # Normal: una línea y a otra cosa, sin traceback ni reintentos.
            log.info("  · sin datos definitivos todavía para %s..%s; sigo", chunk_ini, chunk_fin)
            continue
        except AemetError as e:
            # Error conocido de la API (429 tras agotar reintentos, estado raro…):
            # el mensaje ya es descriptivo, no hace falta el traceback completo.
            log.warning("  · fallo AEMET en %s..%s: %s; sigo", chunk_ini, chunk_fin, e)
            continue
        except Exception:  # noqa: BLE001
            log.exception("Fallo inesperado en chunk %s..%s, sigo con el siguiente", chunk_ini, chunk_fin)
            continue
        norm = []
        for r in raw:
            n = normalize_observation(r)
            if n["indicativo"] and n["fecha"]:
                norm.append(n)
        n = _insert_observations(con, norm)
        total += n
        log.info("  ✓ %d observaciones", n)
    return total


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for noisy in ("httpx", "httpcore", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    p = argparse.ArgumentParser(description="Incremental fetch desde la API de AEMET")
    p.add_argument("--from", dest="from_date", type=date.fromisoformat,
                   help="Fecha inicio YYYY-MM-DD (por defecto: último día en DB + 1)")
    p.add_argument("--to", dest="to_date", type=date.fromisoformat,
                   help="Fecha fin YYYY-MM-DD (por defecto: hoy - lag)")
    p.add_argument("--refresh-stations", action="store_true",
                   help="Actualiza el inventario de estaciones antes de bajar observaciones")
    args = p.parse_args(argv)

    con = connect()
    end = args.to_date or (date.today() - timedelta(days=INGEST_LAG_DAYS))

    if args.from_date:
        start = args.from_date
    else:
        last = _last_obs_date(con)
        start = (last + timedelta(days=1)) if last else DEFAULT_BACKSTOP

    if start > end:
        log.info("Nada que descargar: start=%s > end=%s", start, end)
        return

    with AemetClient() as client:
        if args.refresh_stations:
            n = _refresh_stations(con, client)
            log.info("Estaciones refrescadas: %d", n)
        total = fetch_range(con, client, start, end)

    log.info("Fetch incremental terminado. %d observaciones nuevas/actualizadas.", total)


if __name__ == "__main__":
    main(sys.argv[1:])
