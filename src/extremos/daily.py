"""Orquesta el ciclo diario: fetch -> provisional -> records -> stats -> rankings -> export -> publish."""
from __future__ import annotations

import logging
import sys


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for noisy in ("httpx", "httpcore", "urllib3", "huggingface_hub"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    from extremos import export, fetch, provisional, publish, rankings, records, stats

    log = logging.getLogger("extremos.daily")
    log.info("=== fetch ===")
    fetch.main(["--refresh-stations"])
    log.info("=== provisional (horario) ===")
    provisional.main([])
    log.info("=== records ===")
    records.main([])
    log.info("=== stats ===")
    stats.main([])
    log.info("=== rankings ===")
    rankings.main([])
    log.info("=== export ===")
    export.main([])
    log.info("=== publish (HF) ===")
    publish.main([])
    log.info("Done.")


if __name__ == "__main__":
    main()
