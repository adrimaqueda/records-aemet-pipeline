"""Incremental: descarga datos nuevos desde la API de AEMET y los inserta en DuckDB.

Lógica:
1. Calcula la ventana [start, end]:
   - start = último día DEFINITIVO en observations + 1d (o un default si está vacía)
   - end   = hoy - INGEST_LAG_DAYS  (AEMET publica con retraso)
2. Trocea la ventana en chunks de máx CHUNK_DAYS días (límite de la API para
   todasestaciones).
3. Para cada chunk: AEMET → normalize → INSERT OR REPLACE en observations.
4. Opcionalmente refresca la tabla stations con el inventario actual.

La ventana ignora los provisionales a propósito: si los contara, `start` saltaría
por delante de `end` y nunca bajaríamos el diario definitivo que los reemplaza.
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

import duckdb

from extremos.aemet import AemetClient, AemetError, AemetNoData
from extremos.config import INGEST_LAG_DAYS
from extremos.db import connect, last_definitive_date
from extremos.ingest import insert_observations, insert_stations
from extremos.logconf import setup_logging
from extremos.parsing import normalize_observations, normalize_station

log = logging.getLogger("extremos.fetch")

DEFAULT_BACKSTOP = date(2024, 1, 1)  # solo se usa si la DB está vacía
CHUNK_DAYS = 15  # límite real de la API AEMET para todasestaciones (documenta 31)


def _chunk_ranges(start: date, end: date) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=CHUNK_DAYS - 1), end)
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks


def fetch_range(con: duckdb.DuckDBPyConnection, client: AemetClient,
                start: date, end: date) -> int:
    total = 0
    for ini, fin in _chunk_ranges(start, end):
        log.info("AEMET %s → %s", ini, fin)
        try:
            raw = client.daily_observations(ini, fin)
        except AemetNoData:
            # AEMET aún no ha publicado el definitivo de ese día. Normal.
            log.info("  · sin datos definitivos todavía para %s..%s; sigo", ini, fin)
            continue
        except AemetError as e:
            # 429 tras agotar reintentos, estado raro…: el mensaje ya lo explica.
            log.warning("  · fallo AEMET en %s..%s: %s; sigo", ini, fin, e)
            continue
        except Exception:
            log.exception("Fallo inesperado en chunk %s..%s, sigo con el siguiente", ini, fin)
            continue
        n = insert_observations(con, normalize_observations(raw))
        total += n
        log.info("  ✓ %d observaciones", n)
    return total


def main(argv: list[str] | None = None) -> None:
    setup_logging()
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
    start = args.from_date
    if start is None:
        last = last_definitive_date(con)
        start = last + timedelta(days=1) if last else DEFAULT_BACKSTOP

    if start > end:
        log.info("Nada que descargar: start=%s > end=%s", start, end)
        return

    with AemetClient() as client:
        if args.refresh_stations:
            log.info("Refrescando inventario de estaciones…")
            raw = client.inventory_stations()
            n = insert_stations(con, [normalize_station(r) for r in raw if r.get("indicativo")])
            log.info("Estaciones refrescadas: %d", n)
        total = fetch_range(con, client, start, end)

    log.info("Fetch incremental terminado. %d observaciones nuevas/actualizadas.", total)


if __name__ == "__main__":
    main()
