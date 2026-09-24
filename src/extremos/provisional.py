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
  merge `greatest`/`least` (ver `_insert_provisional`), así el extremo del día
  es el mejor visto entre TODAS las pasadas y fuentes.
- Cuando AEMET publica el diario definitivo, `fetch.py` reemplaza la fila por
  la PK (indicativo, fecha) con `provisional = FALSE`.
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

import duckdb
import polars as pl

from extremos import webcsv
from extremos.aemet import AemetClient
from extremos.config import PROVISIONAL_MAX_AGE_DAYS
from extremos.db import connect, last_definitive_date
from extremos.logconf import setup_logging
from extremos.parsing import normalize_hourly

log = logging.getLogger("extremos.provisional")

LOCAL_TZ = "Europe/Madrid"


def _purge_stale(con: duckdb.DuckDBPyConnection) -> int:
    """Borra los provisionales que ya deberían haberse confirmado.

    Ciclo de vida de un récord provisional:
      - AEMET publicó el definitivo → `fetch` reemplazó la fila por la real
        (provisional=FALSE) y `records` confirma o descarta el récord con esa
        temperatura. No hay nada que limpiar aquí.
      - El definitivo NUNCA llega (la estación no reportó ese día) → la fila se
        quedaría colgada como provisional. Pasados PROVISIONAL_MAX_AGE_DAYS la
        damos por no confirmada y la borramos.
    """
    return con.execute(
        "DELETE FROM observations WHERE provisional "
        f"AND fecha < CURRENT_DATE - INTERVAL '{PROVISIONAL_MAX_AGE_DAYS} days'"
    ).fetchone()[0]


def _aggregate_hourly(raw: list[dict]) -> pl.DataFrame:
    """Agrega registros horarios crudos a tmax/tmin por (indicativo, día local)."""
    rows = [
        n for r in raw
        if (n := normalize_hourly(r))["indicativo"] and n["fint"]
    ]
    if not rows:
        return pl.DataFrame()

    df = pl.DataFrame(rows, schema={
        "indicativo": pl.Utf8, "fint": pl.Utf8,
        "ta": pl.Float64, "tamax": pl.Float64, "tamin": pl.Float64,
    })
    # fint viene en ISO con offset (p. ej. "2026-06-16T19:00:00+0000"): lo
    # pasamos a hora local para asignar cada lectura a su día natural. Las
    # fechas no parseables quedan nulas y se descartan.
    return (
        df.with_columns(
            pl.col("fint")
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
            .dt.convert_time_zone(LOCAL_TZ)
            .dt.date()
            .alias("fecha"),
            # Por lectura, el mejor candidato a máximo/mínimo (ignorando nulos).
            pl.max_horizontal("tamax", "ta").alias("hi"),
            pl.min_horizontal("tamin", "ta").alias("lo"),
        )
        .drop_nulls("fecha")
        .group_by("indicativo", "fecha")
        .agg(pl.max("hi").alias("tmax"), pl.min("lo").alias("tmin"))
        .filter(pl.col("tmax").is_not_null() | pl.col("tmin").is_not_null())
    )


def _insert_provisional(con: duckdb.DuckDBPyConnection, agg: pl.DataFrame) -> int:
    """Acumula filas provisionales sin pisar ningún dato definitivo existente.

    Una pasada puede ver un día natural parcial (la API horaria da ~12-13 h; el
    CSV web del día en curso llega hasta su hora de actualización). En vez de
    reemplazar la fila, se fusiona con `greatest`/`least` (que ignoran NULL)
    contra la provisional existente, así el `tmax`/`tmin` del día es el extremo
    de TODO lo visto entre pasadas y fuentes. El `WHERE` del `DO UPDATE` deja
    intactas las filas definitivas. Devuelve las filas escritas.
    """
    con.register("incoming_prov", agg)
    try:
        return con.execute("""
            INSERT INTO observations (indicativo, fecha, tmin, tmax, provisional)
            SELECT indicativo, fecha, tmin, tmax, TRUE FROM incoming_prov
            ON CONFLICT (indicativo, fecha) DO UPDATE SET
                tmax = greatest(observations.tmax, excluded.tmax),
                tmin = least(observations.tmin, excluded.tmin)
            WHERE observations.provisional
        """).fetchone()[0]
    finally:
        con.unregister("incoming_prov")


def _ingest(con: duckdb.DuckDBPyConnection, fuente: str, agg: pl.DataFrame) -> None:
    if agg.is_empty():
        log.info("Provisionales (%s): sin datos utilizables; nada que insertar.", fuente)
        return
    written = _insert_provisional(con, agg)
    log.info(
        "Provisionales (%s): %d días-estación escritos (%d días, %d estaciones).",
        fuente, written, agg["fecha"].n_unique(), agg["indicativo"].n_unique(),
    )


def _dates_needing_web(con: duckdb.DuckDBPyConnection) -> list[date]:
    """Días sin definitivo aún, acotados a la ventana del CSV web (hoy-7..hoy)."""
    hoy = date.today()
    start = hoy - timedelta(days=7)
    if last := last_definitive_date(con):
        start = max(start, last + timedelta(days=1))
    return [start + timedelta(days=i) for i in range((hoy - start).days + 1)]


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    argparse.ArgumentParser(
        description="Récords provisionales desde el horario en tiempo real y el CSV web"
    ).parse_args(argv)

    con = connect()

    # Conciliación: se hace siempre, aunque luego fallen las fuentes.
    if purged := _purge_stale(con):
        log.info("Provisionales caducados (sin confirmar) purgados: %d", purged)

    # Las dos fuentes son independientes y ninguna debe tumbar el ciclo diario:
    # si OpenData está caída, el CSV web (otro host) suele seguir vivo, y viceversa.
    try:
        log.info("Descargando observación horaria de OpenData (~12-13 h)…")
        with AemetClient() as client:
            raw = client.realtime_observations()
        log.info("  %d registros horarios", len(raw))
        _ingest(con, "horario", _aggregate_hourly(raw))
    except Exception:
        log.exception("Fallo en el horario de OpenData; se omite en esta ejecución.")

    try:
        dates = _dates_needing_web(con)
        log.info("Descargando resúmenes web de AEMET (%s → %s)…", dates[0], dates[-1])
        _ingest(con, "web", webcsv.daily_extremes(dates))
    except Exception:
        log.exception("Fallo en el CSV web de AEMET; se omite en esta ejecución.")


if __name__ == "__main__":
    main()
