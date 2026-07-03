"""Récords provisionales a partir de la observación horaria en tiempo real.

AEMET publica el diario climatológico con ~5 días de retraso (`INGEST_LAG_DAYS`),
así que durante esa ventana no hay dato para los días más recientes. Este módulo
descarga la observación convencional de las últimas ~24 h de todas las estaciones
(`/observacion/convencional/todas`), la agrega a `tmax`/`tmin` por día y la inserta
en `observations` marcada como `provisional = TRUE`.

Reglas:
- El día se calcula en hora local de Madrid (los extremos diarios son por día
  natural local).
- Nunca se pisa un dato definitivo: si ya existe una fila no provisional para
  (indicativo, fecha), se respeta. El resto se inserta/actualiza como provisional.
- Cuando AEMET publique el diario definitivo de esos días, `fetch.py` lo escribe
  con `provisional = FALSE` y reemplaza a la provisional por la PK.

Como la API sólo cubre 24 h, cada pasada ve un día parcial. El cron corre 2×/día
(09:00 y 21:00) y los extremos de un mismo día se acumulan entre pasadas (merge
`GREATEST`/`LEAST`, ver `_insert_provisional`); el hueco de ~5 días se va cubriendo
a medida que el cron corre y se reemplaza por el definitivo cuando llega.
"""
from __future__ import annotations

import argparse
import logging
import sys

import duckdb
import polars as pl

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

    La API horaria sólo da 24 h, así que una pasada ve un día natural parcial. Con
    varias pasadas al día (cron 2×/día) los extremos de un mismo día se van
    refinando: en vez de reemplazar la fila, hacemos un merge `GREATEST`/`LEAST`
    contra la provisional existente, de modo que el `tmax`/`tmin` del día es el
    extremo sobre TODAS las horas vistas, no sólo las de la última ventana.
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


def run(con: duckdb.DuckDBPyConnection, client: AemetClient) -> int:
    log.info("Descargando observación horaria (últimas 24 h)…")
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

    try:
        with AemetClient() as client:
            run(con, client)
    except Exception:  # noqa: BLE001
        # No queremos que un fallo del horario tumbe el ciclo diario completo.
        log.exception("Fallo al calcular provisionales; se omiten en esta ejecución.")


if __name__ == "__main__":
    main(sys.argv[1:])
