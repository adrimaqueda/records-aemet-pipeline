"""Escritura compartida de observaciones y estaciones en DuckDB.

Los tres ingestores (backfill bulk desde datania, backfill histórico por
estación e incremental desde la API) normalizan a los mismos dicts (ver
parsing.py) y escriben con el mismo INSERT OR REPLACE; aquí viven los esquemas
Polars y esa escritura para no repetirlos en cada módulo.
"""
from __future__ import annotations

import duckdb
import polars as pl

OBS_SCHEMA = {
    "indicativo": pl.Utf8, "fecha": pl.Utf8,
    "tmed": pl.Float64, "tmin": pl.Float64, "tmax": pl.Float64,
    "horatmin": pl.Utf8, "horatmax": pl.Utf8,
    "prec": pl.Float64, "sol": pl.Float64,
    "hr_media": pl.Float64, "vel_media": pl.Float64,
    "pres_max": pl.Float64, "pres_min": pl.Float64,
}

STATION_SCHEMA = {
    "indicativo": pl.Utf8, "nombre": pl.Utf8, "provincia": pl.Utf8,
    "altitud": pl.Int64, "latitud": pl.Float64, "longitud": pl.Float64,
    "indsinop": pl.Utf8,
}


def insert_df(con: duckdb.DuckDBPyConnection, table: str, df: pl.DataFrame) -> None:
    """INSERT OR REPLACE de un DataFrame en una tabla con PK."""
    con.register("incoming", df)
    try:
        con.execute(f"INSERT OR REPLACE INTO {table} BY NAME SELECT * FROM incoming")
    finally:
        con.unregister("incoming")


def insert_observations(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> int:
    """Inserta observaciones DEFINITIVAS ya normalizadas; devuelve nº de filas.

    Marca provisional=FALSE explícitamente. Imprescindible porque INSERT OR
    REPLACE BY NAME solo actualiza las columnas presentes, y un día que antes
    era provisional debe perder ese flag al llegar el dato definitivo.
    """
    if not rows:
        return 0
    df = pl.DataFrame(rows, schema=OBS_SCHEMA).with_columns(
        pl.col("fecha").str.to_date(),
        pl.lit(False).alias("provisional"),
    )
    insert_df(con, "observations", df)
    return len(rows)


def insert_stations(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> int:
    """Inserta/actualiza estaciones ya normalizadas; devuelve nº de filas."""
    if not rows:
        return 0
    insert_df(con, "stations", pl.DataFrame(rows, schema=STATION_SCHEMA))
    return len(rows)
