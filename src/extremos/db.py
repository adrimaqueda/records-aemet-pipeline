"""Conexión y esquema DuckDB."""
from __future__ import annotations

import duckdb

from extremos.config import DATA_DIR, DB_PATH

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stations (
    indicativo  VARCHAR PRIMARY KEY,
    nombre      VARCHAR,
    provincia   VARCHAR,
    altitud     INTEGER,
    latitud     DOUBLE,
    longitud    DOUBLE,
    indsinop    VARCHAR
);

CREATE TABLE IF NOT EXISTS observations (
    indicativo  VARCHAR NOT NULL,
    fecha       DATE    NOT NULL,
    tmed        DOUBLE,
    tmin        DOUBLE,
    tmax        DOUBLE,
    horatmin    VARCHAR,
    horatmax    VARCHAR,
    prec        DOUBLE,
    sol         DOUBLE,
    hr_media    DOUBLE,
    vel_media   DOUBLE,
    pres_max    DOUBLE,
    pres_min    DOUBLE,
    -- TRUE para los días reconstruidos a partir del horario en tiempo real
    -- (récord provisional). El dato diario definitivo de AEMET los reemplaza
    -- por la PK (indicativo, fecha) con provisional = FALSE.
    provisional BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (indicativo, fecha)
);

CREATE INDEX IF NOT EXISTS observations_fecha_idx ON observations(fecha);
"""

# Migraciones para DBs creadas antes de añadir columnas nuevas.
# Cada entrada: (tabla, columna, definición DDL).
#
# OJO: NO usar `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ... DEFAULT ...`. En
# DuckDB, si la columna YA existe, ese ALTER no es un no-op: reescribe la columna
# a su DEFAULT, borrando los valores existentes (p. ej. resetea `provisional` a
# FALSE en cada arranque). Por eso comprobamos la existencia antes de ejecutarlo.
_MIGRATIONS = [
    ("observations", "provisional", "BOOLEAN DEFAULT FALSE"),
]


def _migrate(con: duckdb.DuckDBPyConnection) -> None:
    for table, column, ddl in _MIGRATIONS:
        exists = con.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = ? AND column_name = ?",
            [table, column],
        ).fetchone()
        if not exists:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def connect() -> duckdb.DuckDBPyConnection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(SCHEMA_SQL)
    _migrate(con)
    return con
