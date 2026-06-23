"""Respaldo y restauración de la DuckDB en un dataset HF privado.

No subimos los 342 MB del `.duckdb`. Exportamos solo las tablas **fuente**
(`observations`, `stations`, `backfill_progress`) a Parquet ZSTD (~75 MB en total):
las derivadas (`record_events`, `station_coverage`) se reconstruyen con
`extremos-records`. Eso minimiza el peso que se sube (la preocupación principal) y
da un punto de restauración para:

  - recuperar la Pi si muere la SD, sin re-backfill de horas;
  - hacer *bootstrap* de una Pi nueva: `extremos-backup --restore` y luego
    `extremos-daily` (que ya hace fetch + records + export + publish).

Uso:
    extremos-backup            # exporta a parquet y sube a EXTREMOS_HF_DB_REPO
    extremos-backup --restore  # descarga el último parquet y reconstruye la DB
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
import tempfile
from pathlib import Path

from huggingface_hub import HfApi, snapshot_download

from extremos.backfill import PROGRESS_SQL
from extremos.config import DB_PATH, HF_DB_REPO
from extremos.db import connect

log = logging.getLogger("extremos.backup")

# Tablas fuente que respaldamos (las derivadas se recalculan con `records`).
SOURCE_TABLES = ("observations", "stations", "backfill_progress")


def backup(repo: str) -> None:
    api = HfApi()
    api.create_repo(repo, repo_type="dataset", exist_ok=True, private=True)

    con = connect()
    con.execute(PROGRESS_SQL)  # por si la DB se creó sin backfill previo
    tmp = Path(tempfile.mkdtemp(prefix="extremos-bk-"))
    try:
        total = 0
        for tbl in SOURCE_TABLES:
            dst = tmp / f"{tbl}.parquet"
            con.execute(
                f"COPY {tbl} TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            sz = dst.stat().st_size
            total += sz
            log.info("  %s → %.1f MB", tbl, sz / 1e6)
        log.info("Subiendo %.1f MB a %s…", total / 1e6, repo)
        api.upload_folder(
            folder_path=str(tmp),
            repo_id=repo,
            repo_type="dataset",
            commit_message="Backup DuckDB (parquet de tablas fuente)",
            allow_patterns=["*.parquet"],
        )
        log.info("Backup completado en https://huggingface.co/datasets/%s", repo)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def restore(repo: str) -> None:
    if DB_PATH.exists():
        raise SystemExit(
            f"{DB_PATH} ya existe. Muévela/bórrala antes de restaurar para no "
            "mezclar datos."
        )
    log.info("Descargando parquet de %s…", repo)
    tmp = Path(snapshot_download(
        repo_id=repo, repo_type="dataset", allow_patterns=["*.parquet"]
    ))
    con = connect()                 # crea el esquema vacío con sus PK
    con.execute(PROGRESS_SQL)
    for tbl in SOURCE_TABLES:
        src = tmp / f"{tbl}.parquet"
        if not src.exists():
            log.warning("  falta %s.parquet en el backup, lo salto", tbl)
            continue
        con.execute(
            f"INSERT INTO {tbl} BY NAME SELECT * FROM read_parquet('{src}')"
        )
        n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        log.info("  %s ← %d filas", tbl, n)
    log.info("DB restaurada en %s. Ahora ejecuta `extremos-daily` para "
             "recalcular récords, exportar y publicar.", DB_PATH)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for noisy in ("httpx", "httpcore", "huggingface_hub", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    p = argparse.ArgumentParser(description="Backup/restore de la DuckDB en HF privado")
    p.add_argument("--restore", action="store_true",
                   help="Restaura la DB desde el backup en lugar de subirla")
    p.add_argument("--repo", default=HF_DB_REPO,
                   help="Dataset HF privado (default: EXTREMOS_HF_DB_REPO)")
    args = p.parse_args(argv)

    if not args.repo:
        raise SystemExit("EXTREMOS_HF_DB_REPO no configurado (dataset privado de backup).")

    if args.restore:
        restore(args.repo)
    else:
        backup(args.repo)


if __name__ == "__main__":
    main(sys.argv[1:])
