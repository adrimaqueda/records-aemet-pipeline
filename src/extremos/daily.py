"""Orquesta el ciclo diario: fetch -> provisional -> records -> stats -> rankings -> export -> publish."""
from __future__ import annotations

import logging

from extremos.logconf import setup_logging


def main() -> None:
    setup_logging()

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
