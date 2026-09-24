"""Conexión y esquema DuckDB.

Tablas fuente (las que se respaldan, ver backup.py): `stations`, `observations`
y las de progreso de los backfills. Las derivadas (`record_events`,
`station_coverage`…) las recrea `records.py` en cada pasada.
"""
from __future__ import annotations

from datetime import date

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
    -- TRUE para los días reconstruidos a partir del tiempo real (récord
    -- provisional). El dato diario definitivo de AEMET los reemplaza por la PK
    -- (indicativo, fecha) con provisional = FALSE.
    provisional BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (indicativo, fecha)
);

CREATE INDEX IF NOT EXISTS observations_fecha_idx ON observations(fecha);

-- Meses de datania ya ingeridos por `backfill.py`.
CREATE TABLE IF NOT EXISTS backfill_progress (
    year        INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    n_rows      INTEGER NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (year, month)
);

-- Estaciones procesadas por `backfill_historico.py`.
CREATE TABLE IF NOT EXISTS historico_progress (
    indicativo  VARCHAR PRIMARY KEY,
    earliest    DATE,            -- día más antiguo bajado para la estación
    n_rows      INTEGER,         -- filas nuevas/actualizadas insertadas
    complete    BOOLEAN DEFAULT FALSE,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def connect() -> duckdb.DuckDBPyConnection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(SCHEMA_SQL)
    return con


def last_definitive_date(con: duckdb.DuckDBPyConnection) -> date | None:
    """Último día con dato DEFINITIVO (los provisionales no cuentan)."""
    return con.execute(
        "SELECT MAX(fecha) FROM observations WHERE NOT provisional"
    ).fetchone()[0]
