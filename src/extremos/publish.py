"""Publica los JSONs de `outputs/` en el dataset de HuggingFace que lee la app.

Solo viajan a HF los JSONs ligeros (~28 MB): `stations.json`, `stats.json`,
`rankings.json` y los `stations/{indicativo}.json`. La DuckDB pesada (342 MB) se
queda en la Pi; su respaldo va aparte y comprimido (ver `backup.py`).

La app los consume vía `VITE_DATA_BASE_URL` apuntando a
`https://huggingface.co/datasets/<repo>/resolve/main`.
"""
from __future__ import annotations

import argparse
import logging
import sys

from huggingface_hub import HfApi

from extremos.config import HF_TARGET_REPO, OUTPUTS_DIR
from extremos.logconf import setup_logging

log = logging.getLogger("extremos.publish")


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    # `upload_folder` avisa (a nivel WARNING si hay >200 ficheros) de que subimos
    # una "carpeta grande" y sugiere `upload_large_folder`. Aquí es un falso
    # positivo: son ~870 JSONs pero solo 29 MB, el commit único va sobrado y
    # `upload_large_folder` no soporta `delete_patterns` (perderíamos la purga de
    # estaciones inactivas). Los fallos reales de subida llegan como excepción.
    logging.getLogger("huggingface_hub").setLevel(logging.ERROR)
    p = argparse.ArgumentParser(description="Publica outputs/ en el dataset HF de la app")
    p.add_argument("--repo", default=HF_TARGET_REPO,
                   help="Dataset destino (default: EXTREMOS_HF_REPO)")
    args = p.parse_args(argv)

    if not args.repo:
        log.warning("EXTREMOS_HF_REPO no configurado; salto la publicación a HF.")
        return
    if not OUTPUTS_DIR.exists():
        raise SystemExit(f"No existe {OUTPUTS_DIR}. Ejecuta `extremos-export` primero.")

    api = HfApi()
    api.create_repo(args.repo, repo_type="dataset", exist_ok=True, private=False)
    api.upload_folder(
        folder_path=str(OUTPUTS_DIR),
        repo_id=args.repo,
        repo_type="dataset",
        commit_message="Actualiza datos de la app",
        # Purga en remoto las estaciones que ya no exportamos (dejaron de estar
        # activas o desaparecieron). Los summaries de la raíz (stations.json,
        # stats.json, rankings.json) siempre se reescriben.
        delete_patterns=["stations/*.json"],
    )
    log.info("Publicado outputs/ en https://huggingface.co/datasets/%s", args.repo)


if __name__ == "__main__":
    main(sys.argv[1:])
