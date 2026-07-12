"""Récords provisionales mientras AEMET publica el diario definitivo.

AEMET publica el diario climatológico con ~4-5 días de retraso (`INGEST_LAG_DAYS`),
así que durante esa ventana no hay dato para los días más recientes. Este módulo
lo cubre con dos fuentes independientes, ambas insertadas en `observations` con
`provisional = TRUE`:

1. **Horario OpenData** (`/observacion/convencional/todas`): registros horarios
   de todas las estaciones, agregados a `tmax`/`tmin` por día natural local.
   Pese a que AEMET documenta 24 h, en la práctica devuelve ~12-13 h (medido
   2026-07-12 en vivo y contrastado con los volúmenes de los logs), de modo que
   con el cron 2×/día (09:00 y 21:00) la unión de pasadas cubre el día justo,
   SIN margen: una pasada perdida deja horas sin ver.
2. **CSV de resumen de la web** (`webcsv.py`): extremos del día COMPLETO de los
   últimos 7 días más el día en curso, ~830 estaciones por petición. Repara los
   huecos que deja (1) — pasadas perdidas, ventana corta — y sigue funcionando
   cuando opendata.aemet.es está caída (host distinto).

Reglas comunes:
- El día es el natural en hora local de Madrid (en la web, la "hora oficial").
- Nunca se pisa un dato definitivo: si ya existe una fila no provisional para
  (indicativo, fecha), se respeta. El resto se acumula como provisional con
  merge `GREATEST`/`LEAST` (ver `_insert_provisional`), así el extremo del día
  es el mejor visto entre TODAS las pasadas y fuentes.
- Cuando AEMET publica el diario definitivo, `fetch.py` reemplaza la fila por
  la PK (indicativo, fecha) con `provisional = FALSE`.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

import duckdb
import polars as pl

from extremos import webcsv
from extremos.aemet import AemetClient
from extremos.config import PROVISIONAL_MAX_AGE_DAYS
from extremos.db import connect
from extremos.logconf import setup_logging
from extremos.parsing import normalize_hourly

log = logging.getLogger("extremos.provisional")

LOCAL_TZ = "Europe/Madrid"


def _purge_stale(con: duckdb.DuckDBPyConnection) -> int:
    """Borra los provisionales que ya deberían haberse confirmado.

    Ciclo de vida de un récord provisional:
      - Se cumplió y AEMET publicó el definitivo → `fetch` reemplazó la fila por
        la real (provisional=FALSE) y `records` la confirma con su temperatura
        definitiva. (No hay nada que limpiar aquí.)
      - No se cumplió pero el definitivo llegó → la fila pasa a definitiva con la
        temperatura real; al recalcular, el récord no se genera y desaparece.
      - El definitivo NUNCA llega (la estación no reportó ese día) → la fila se
        quedaría colgada como provisional. Pasados PROVISIONAL_MAX_AGE_DAYS la
        damos por no confirmada y la borramos.
    """
    where = (
        "COALESCE(provisional, FALSE) "
        f"AND fecha < CURRENT_DATE - INTERVAL '{PROVISIONAL_MAX_AGE_DAYS} days'"
    )
    n = con.execute(f"SELECT COUNT(*) FROM observations WHERE {where}").fetchone()[0]
    if n:
        con.execute(f"DELETE FROM observations WHERE {where}")
    return int(n)


def _aggregate_daily(raw: list[dict]) -> pl.DataFrame:
    """Agrega registros horarios crudos a tmax/tmin por (indicativo, día local)."""
    rows = [
        n for r in raw
        if (n := normalize_hourly(r))["indicativo"] and n["fint"]
    ]
    if not rows:
        return pl.DataFrame()

    df = pl.DataFrame(
        rows,
        schema={
            "indicativo": pl.Utf8,
            "fint": pl.Utf8,
            "ta": pl.Float64,
            "tamax": pl.Float64,
            "tamin": pl.Float64,
        },
    )

    # fint viene en ISO CON offset de zona (p. ej. "2026-06-16T19:00:00+0000").
    # Lo parseamos con formato explícito (%z capta el offset) y lo pasamos a hora
    # local para asignar cada lectura a su día natural. strict=False → las fechas
    # no parseables quedan nulas y se descartan.
    df = df.with_columns(
        pl.col("fint")
        .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
        .dt.convert_time_zone(LOCAL_TZ)
        .dt.date()
        .alias("fecha")
    ).drop_nulls("fecha")

    # Por lectura, el mejor candidato a máximo/mínimo (ignorando nulos).
    df = df.with_columns(
        pl.max_horizontal("tamax", "ta").alias("hi"),
        pl.min_horizontal("tamin", "ta").alias("lo"),
    )

    agg = (
        df.group_by("indicativo", "fecha")
        .agg(
            pl.max("hi").alias("tmax"),
            pl.min("lo").alias("tmin"),
        )
        .filter(pl.col("tmax").is_not_null() | pl.col("tmin").is_not_null())
    )
    return agg


def _insert_provisional(con: duckdb.DuckDBPyConnection, agg: pl.DataFrame) -> int:
    """Acumula filas provisionales sin pisar ningún dato definitivo existente.

    Una pasada puede ver un día natural parcial (la API horaria da ~12-13 h; el
    CSV web del día en curso llega hasta su hora de actualización). En vez de
    reemplazar la fila, hacemos un merge `GREATEST`/`LEAST` contra la provisional
    existente, de modo que el `tmax`/`tmin` del día es el extremo sobre TODO lo
    visto entre pasadas y fuentes, no sólo la última ventana.
    """
    if agg.is_empty():
        return 0
    con.register("incoming_prov", agg)
    try:
        con.execute("""
            INSERT INTO observations (indicativo, fecha, tmin, tmax, provisional)
            SELECT i.indicativo, i.fecha, i.tmin, i.tmax, TRUE
            FROM incoming_prov i
            WHERE NOT EXISTS (
                SELECT 1 FROM observations o
                WHERE o.indicativo = i.indicativo
                  AND o.fecha = i.fecha
                  AND o.provisional = FALSE
            )
            ON CONFLICT (indicativo, fecha) DO UPDATE SET
                tmax = greatest(
                    coalesce(observations.tmax, excluded.tmax),
                    coalesce(excluded.tmax, observations.tmax)
                ),
                tmin = least(
                    coalesce(observations.tmin, excluded.tmin),
                    coalesce(excluded.tmin, observations.tmin)
                )
                WHERE observations.provisional
        """)
        written = con.execute("""
            SELECT COUNT(*) FROM incoming_prov i
            WHERE NOT EXISTS (
                SELECT 1 FROM observations o
                WHERE o.indicativo = i.indicativo
                  AND o.fecha = i.fecha
                  AND o.provisional = FALSE
            )
        """).fetchone()[0]
    finally:
        con.unregister("incoming_prov")
    return int(written)


def _dates_needing_web(con: duckdb.DuckDBPyConnection) -> list[date]:
    """Días sin definitivo aún, acotados a la ventana del CSV web (hoy-7..hoy)."""
    row = con.execute(
        "SELECT MAX(fecha) FROM observations WHERE NOT COALESCE(provisional, FALSE)"
    ).fetchone()
    last = row[0] if row else None
    hoy = date.today()
    start = hoy - timedelta(days=7)
    if last:
        start = max(start, last + timedelta(days=1))
    return [start + timedelta(days=i) for i in range((hoy - start).days + 1)]


def run_web(con: duckdb.DuckDBPyConnection) -> int:
    """Extremos del CSV web nacional para los días que aún no tienen definitivo."""
    dates = _dates_needing_web(con)
    if not dates:
        log.info("CSV web: ningún día pendiente de definitivo; nada que pedir.")
        return 0
    log.info("Descargando resúmenes web de AEMET (%s → %s)…", dates[0], dates[-1])
    agg = webcsv.daily_extremes(dates)
    if agg.is_empty():
        log.info("CSV web sin datos utilizables; nada que insertar.")
        return 0
    written = _insert_provisional(con, agg)
    log.info(
        "Provisionales (web): %d días-estación escritos (%d días, %d estaciones).",
        written,
        agg.select(pl.col("fecha").n_unique()).item(),
        agg.select(pl.col("indicativo").n_unique()).item(),
    )
    return written


def run(con: duckdb.DuckDBPyConnection, client: AemetClient) -> int:
    log.info("Descargando observación horaria de OpenData (~12-13 h)…")
    raw = client.realtime_observations()
    log.info("  %d registros horarios", len(raw))

    agg = _aggregate_daily(raw)
    if agg.is_empty():
        log.info("Sin datos horarios agregables; nada que insertar.")
        return 0

    written = _insert_provisional(con, agg)
    dias = agg.select(pl.col("fecha").n_unique()).item()
    log.info(
        "Provisionales: %d días-estación escritos (%d días distintos, %d estaciones).",
        written, dias, agg.select(pl.col("indicativo").n_unique()).item(),
    )
    return written


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    argparse.ArgumentParser(
        description="Reconstruye récords provisionales desde el horario en tiempo real"
    ).parse_args(argv)

    con = connect()

    # Conciliación: borra provisionales caducados sin confirmar. Se hace siempre,
    # incluso si la API horaria falla luego (es una limpieza independiente y los
    # confirmados ya los reemplazó `fetch` antes en el ciclo diario).
    purged = _purge_stale(con)
    if purged:
        log.info("Provisionales caducados (sin confirmar) purgados: %d", purged)

    # Las dos fuentes son independientes y ninguna debe tumbar el ciclo diario:
    # si OpenData está caída, el CSV web (otro host) suele seguir vivo, y viceversa.
    try:
        with AemetClient() as client:
            run(con, client)
    except Exception:  # noqa: BLE001
        log.exception("Fallo en el horario de OpenData; se omite en esta ejecución.")

    try:
        run_web(con)
    except Exception:  # noqa: BLE001
        log.exception("Fallo en el CSV web de AEMET; se omite en esta ejecución.")


if __name__ == "__main__":
    main(sys.argv[1:])
