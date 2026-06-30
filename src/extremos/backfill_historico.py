"""Backfill histórico per-estación: rellena los años ANTERIORES a 1975.

El backfill bulk (`backfill.py`, desde datania) y el incremental (`fetch.py`,
endpoint `todasestaciones`) sólo cubren de 1975 en adelante. Pero el endpoint
*por estación* de AEMET (`.../diarios/datos/.../estacion/{idema}`) sirve la serie
completa, que para muchas estaciones se remonta a ~1920. Este módulo recorre, de
una en una, las estaciones cuya serie en la DB arranca en 1975 (truncadas por el
suelo del bulk) y baja hacia atrás su histórico previo, en ventanas semestrales
(el endpoint limita cada petición a 6 meses).

Es un proceso de UNA SOLA VEZ (caro: ~horas de API) y reanudable: una tabla
`historico_progress` marca qué estaciones ya están completas, así que relanzarlo
salta las hechas. Las observaciones entran como definitivas (provisional=FALSE)
con INSERT OR REPLACE, así que es idempotente.

Tras correrlo hay que recalcular: `records` → `stats`/`rankings` → `export` →
`publish` (o simplemente `extremos-daily`).
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

import duckdb

from extremos.aemet import AemetClient, AemetError, AemetNoData
from extremos.config import HISTORICO_EMPTY_STOP, MIN_HISTORICO_YEAR
from extremos.db import connect
from extremos.fetch import _insert_observations
from extremos.parsing import normalize_observation

log = logging.getLogger("extremos.backfill_historico")

PROGRESS_SQL = """
CREATE TABLE IF NOT EXISTS historico_progress (
    indicativo  VARCHAR PRIMARY KEY,
    earliest    DATE,            -- día más antiguo bajado para la estación
    n_rows      INTEGER,         -- filas nuevas/actualizadas insertadas
    complete    BOOLEAN DEFAULT FALSE,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def _half_year_windows(from_year: int, to_year: int) -> list[tuple[date, date]]:
    """Ventanas semestrales (≤6 meses) de `to_year` hacia atrás hasta `from_year`.

    Devuelve [(ini, fin)] de la MÁS RECIENTE a la más antigua, para poder ir
    bajando en el tiempo y cortar pronto cuando se agota la serie.
    """
    windows: list[tuple[date, date]] = []
    for year in range(to_year, from_year - 1, -1):
        windows.append((date(year, 7, 1), date(year, 12, 31)))
        windows.append((date(year, 1, 1), date(year, 6, 30)))
    return windows


def _target_stations(
    con: duckdb.DuckDBPyConnection, *, include_complete: bool = False
) -> list[tuple[str, date]]:
    """Estaciones pendientes de histórico, con (indicativo, primer_día_en_db).

    Una estación es objetivo si NO está marcada como completa y, o bien:
      - su serie aún arranca en 1975 (truncada por el suelo del bulk, intacta), o
      - tiene una marca en `historico_progress` (la empezamos pero se interrumpió
        a media descarga: ya no arranca en 1975, pero falta histórico por bajar).
    Ese segundo caso reanuda solo los orphans (estaciones a medias) en la próxima
    pasada. Con `include_complete` se ignora la marca de completa (para --force).
    Sólo definitivos (los provisionales son de hoy).
    """
    complete_filter = "" if include_complete else "NOT COALESCE(hp.complete, FALSE) AND"
    return con.execute(
        f"""
        WITH starts AS (
            SELECT indicativo, MIN(fecha) AS desde
            FROM observations
            WHERE NOT COALESCE(provisional, FALSE)
            GROUP BY indicativo
        )
        SELECT s.indicativo, s.desde
        FROM starts s
        LEFT JOIN historico_progress hp USING (indicativo)
        WHERE {complete_filter}
              (EXTRACT(year FROM s.desde) = 1975 OR hp.indicativo IS NOT NULL)
        ORDER BY s.indicativo
        """
    ).fetchall()


def backfill_station(
    con: duckdb.DuckDBPyConnection,
    client: AemetClient,
    indicativo: str,
    cutoff: date,
    floor_year: int,
    empty_stop: int,
) -> tuple[int, date | None]:
    """Baja el histórico de UNA estación anterior a `cutoff` (exclusivo).

    Va hacia atrás en ventanas semestrales desde el año anterior a `cutoff` hasta
    `floor_year`, parando antes si encadena `empty_stop` ventanas vacías (la
    estación ya no existía). Devuelve (filas_insertadas, día_más_antiguo).
    """
    total = 0
    earliest: date | None = None
    consecutive_empty = 0
    # Hasta cutoff.year (no cutoff.year-1): si `cutoff` cae a mitad de año (p. ej.
    # al reanudar una estación que se interrumpió a media descarga), todavía falta
    # el semestre anterior de ese mismo año. El guard `win_ini >= cutoff` descarta
    # las ventanas ya cubiertas.
    for win_ini, win_fin in _half_year_windows(floor_year, cutoff.year):
        if win_ini >= cutoff:
            continue
        try:
            raw = client.station_daily(
                indicativo, win_ini.isoformat(), win_fin.isoformat()
            )
        except AemetNoData:
            consecutive_empty += 1
            if consecutive_empty >= empty_stop:
                log.info("    · %s: %d ventanas vacías seguidas; serie agotada",
                         indicativo, consecutive_empty)
                break
            continue
        except AemetError as e:
            log.warning("    · %s %s..%s: fallo AEMET: %s; sigo",
                        indicativo, win_ini, win_fin, e)
            continue
        norm = [
            n for r in raw
            if (n := normalize_observation(r))["indicativo"] and n["fecha"]
        ]
        if not norm:
            consecutive_empty += 1
            if consecutive_empty >= empty_stop:
                log.info("    · %s: %d ventanas vacías seguidas; serie agotada",
                         indicativo, consecutive_empty)
                break
            continue
        consecutive_empty = 0
        total += _insert_observations(con, norm)
        earliest = win_ini
    return total, earliest


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for noisy in ("httpx", "httpcore", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    p = argparse.ArgumentParser(
        description="Backfill histórico per-estación (años anteriores a 1975)"
    )
    p.add_argument("--floor-year", type=int, default=MIN_HISTORICO_YEAR,
                   help=f"Año más antiguo a pedir (default: {MIN_HISTORICO_YEAR})")
    p.add_argument("--empty-stop", type=int, default=HISTORICO_EMPTY_STOP,
                   help=f"Ventanas vacías seguidas que agotan una serie "
                        f"(default: {HISTORICO_EMPTY_STOP})")
    p.add_argument("--limit", type=int, default=None,
                   help="Procesa como mucho N estaciones (para pruebas)")
    p.add_argument("--station", action="append", dest="stations",
                   help="Procesa sólo este indicativo (repetible). Ignora el "
                        "filtro de 'arranca en 1975'.")
    p.add_argument("--force", action="store_true",
                   help="Reprocesa estaciones ya marcadas como completas")
    args = p.parse_args(argv)

    con = connect()
    con.execute(PROGRESS_SQL)

    if args.stations:
        pending = con.execute(
            "SELECT indicativo, MIN(fecha) FROM observations "
            "WHERE indicativo IN ({}) AND NOT COALESCE(provisional, FALSE) "
            "GROUP BY indicativo ORDER BY indicativo".format(
                ",".join("?" * len(args.stations))),
            args.stations,
        ).fetchall()
    else:
        pending = _target_stations(con, include_complete=args.force)
    if args.limit:
        pending = pending[: args.limit]

    log.info("Estaciones pendientes de histórico: %d", len(pending))

    grand_total = 0
    with AemetClient() as client:
        for n, (indicativo, cutoff) in enumerate(pending, 1):
            log.info("[%d/%d] %s (histórico < %s)", n, len(pending), indicativo, cutoff)
            # Marca "en curso" (complete=FALSE) ANTES de empezar: si el proceso
            # se interrumpe a media estación, la marca queda y la próxima pasada
            # la reanuda (el orphan ya no arranca en 1975 pero sí tiene fila aquí).
            con.execute(
                "INSERT OR REPLACE INTO historico_progress"
                "(indicativo, complete, updated_at) VALUES (?, FALSE, CURRENT_TIMESTAMP)",
                [indicativo],
            )
            try:
                inserted, earliest = backfill_station(
                    con, client, indicativo, cutoff, args.floor_year, args.empty_stop
                )
            except Exception:  # noqa: BLE001
                log.exception("Fallo en %s, continúo con la siguiente", indicativo)
                continue
            con.execute(
                "INSERT OR REPLACE INTO historico_progress"
                "(indicativo, earliest, n_rows, complete, updated_at) "
                "VALUES (?, ?, ?, TRUE, CURRENT_TIMESTAMP)",
                [indicativo, earliest, inserted],
            )
            grand_total += inserted
            log.info("  ✓ %s: +%d obs (desde %s)", indicativo, inserted,
                     earliest or "sin histórico previo")

    log.info("Backfill histórico terminado. %d observaciones nuevas en total.",
             grand_total)
    log.info("Siguiente paso: recalcular con `extremos-records` y luego "
             "`extremos-stats`/`-rankings`/`-export`/`-publish` (o `extremos-daily`).")


if __name__ == "__main__":
    main(sys.argv[1:])
