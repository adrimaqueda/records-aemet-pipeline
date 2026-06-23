"""Backfill: descarga histórico desde HuggingFace datania/aemet a DuckDB.

Procesa mes a mes para acotar el uso de disco en la Raspberry Pi.
Cada mes: snapshot_download → parsea → INSERT en DuckDB → borra cache.
Reanudable: la tabla backfill_progress marca qué meses ya se han ingerido.
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb
import polars as pl
from huggingface_hub import HfApi, snapshot_download

from extremos.config import HF_SOURCE_REPO, MIN_BACKFILL_YEAR
from extremos.db import connect
from extremos.parsing import normalize_observation, normalize_station

log = logging.getLogger("extremos.backfill")

OBS_BASE = "valores-climatologicos"
STATIONS_BASE = "estaciones"

PROGRESS_SQL = """
CREATE TABLE IF NOT EXISTS backfill_progress (
    year       INTEGER NOT NULL,
    month      INTEGER NOT NULL,
    n_rows     INTEGER NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (year, month)
);
"""

OBS_COLS = (
    "indicativo", "fecha", "tmed", "tmin", "tmax", "horatmin", "horatmax",
    "prec", "sol", "hr_media", "vel_media", "pres_max", "pres_min",
)

STATION_COLS = (
    "indicativo", "nombre", "provincia", "altitud", "latitud", "longitud", "indsinop",
)


def _list_subdirs(api: HfApi, path: str) -> list[str]:
    items = api.list_repo_tree(
        HF_SOURCE_REPO, repo_type="dataset", path_in_repo=path, recursive=False
    )
    return sorted(Path(it.path).name for it in items if it.path != path)


def list_year_months(api: HfApi) -> list[tuple[int, int]]:
    pairs: list[tuple[int, int]] = []
    for y in _list_subdirs(api, OBS_BASE):
        if not y.isdigit():
            continue
        year = int(y)
        for m in _list_subdirs(api, f"{OBS_BASE}/{year}"):
            if m.isdigit():
                pairs.append((year, int(m)))
    return sorted(pairs)


def download(pattern: str) -> Path:
    tmp = Path(tempfile.mkdtemp(prefix="extremos-hf-"))
    snapshot_download(
        repo_id=HF_SOURCE_REPO,
        repo_type="dataset",
        allow_patterns=[pattern],
        local_dir=str(tmp),
    )
    return tmp


def _insert_df(con: duckdb.DuckDBPyConnection, table: str, df: pl.DataFrame) -> None:
    """INSERT OR REPLACE de un DataFrame en una tabla con PK."""
    con.register("incoming", df)
    try:
        con.execute(f"INSERT OR REPLACE INTO {table} BY NAME SELECT * FROM incoming")
    finally:
        con.unregister("incoming")


def ingest_stations(con: duckdb.DuckDBPyConnection) -> int:
    log.info("Descargando metadatos de estaciones…")
    tmp = download(f"{STATIONS_BASE}/*.json")
    try:
        rows = []
        for fp in sorted((tmp / STATIONS_BASE).glob("*.json")):
            with fp.open("rb") as f:
                norm = normalize_station(json.load(f))
            if norm["indicativo"]:
                rows.append(norm)
        if not rows:
            return 0
        df = pl.DataFrame(rows, schema={
            "indicativo": pl.Utf8, "nombre": pl.Utf8, "provincia": pl.Utf8,
            "altitud": pl.Int64, "latitud": pl.Float64, "longitud": pl.Float64,
            "indsinop": pl.Utf8,
        })
        _insert_df(con, "stations", df)
        return len(rows)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def ingest_month(con: duckdb.DuckDBPyConnection, year: int, month: int) -> int:
    pattern = f"{OBS_BASE}/{year}/{month:02d}/*.json"
    tmp = download(pattern)
    try:
        month_dir = tmp / OBS_BASE / str(year) / f"{month:02d}"
        rows: list[dict] = []
        for fp in sorted(month_dir.glob("*.json")):
            with fp.open("rb") as f:
                for raw in json.load(f):
                    norm = normalize_observation(raw)
                    if norm["indicativo"] and norm["fecha"]:
                        rows.append(norm)
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
            # Histórico definitivo: pisa también el flag por si solapa un día
            # que estuviera provisional (ver nota en fetch._insert_observations).
            pl.lit(False).alias("provisional"),
        )
        _insert_df(con, "observations", df)
        return len(rows)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for noisy in ("httpx", "httpcore", "huggingface_hub", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    p = argparse.ArgumentParser(description="Backfill AEMET desde HF datania/aemet")
    p.add_argument("--from-year", type=int, default=MIN_BACKFILL_YEAR,
                   help=f"Año mínimo a procesar (default: {MIN_BACKFILL_YEAR}, "
                        f"override con EXTREMOS_MIN_YEAR)")
    p.add_argument("--to-year", type=int, default=None)
    p.add_argument("--force", action="store_true", help="Reprocesa meses ya marcados")
    p.add_argument("--skip-stations", action="store_true")
    args = p.parse_args(argv)

    con = connect()
    con.execute(PROGRESS_SQL)

    if not args.skip_stations:
        n = ingest_stations(con)
        log.info("Estaciones cargadas/actualizadas: %d", n)

    api = HfApi()
    pairs = list_year_months(api)
    if args.from_year:
        pairs = [p for p in pairs if p[0] >= args.from_year]
    if args.to_year:
        pairs = [p for p in pairs if p[0] <= args.to_year]
    log.info(
        "Meses disponibles: %d (de %s a %s)",
        len(pairs), pairs[0] if pairs else None, pairs[-1] if pairs else None,
    )

    done = {(y, m) for y, m in con.execute("SELECT year, month FROM backfill_progress").fetchall()}

    for year, month in pairs:
        if not args.force and (year, month) in done:
            continue
        try:
            n = ingest_month(con, year, month)
        except Exception:  # noqa: BLE001
            log.exception("Fallo en %d-%02d, continuando", year, month)
            continue
        con.execute(
            "INSERT OR REPLACE INTO backfill_progress(year, month, n_rows) VALUES (?, ?, ?)",
            [year, month, n],
        )
        log.info("  ✓ %d-%02d: %d obs", year, month, n)

    total = con.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    log.info("Backfill terminado. Total observaciones en DB: %d", total)


if __name__ == "__main__":
    main(sys.argv[1:])
