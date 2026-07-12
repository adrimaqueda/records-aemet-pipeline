"""Orquesta el ciclo diario: fetch -> provisional -> records -> stats -> rankings -> export -> publish -> notify."""
from __future__ import annotations

from extremos.logconf import Run, setup_logging


def main() -> None:
    setup_logging()

    from extremos import export, fetch, notify, provisional, publish, rankings, records, stats

    with Run("daily", total=8) as run:
        with run.step("fetch"):
            fetch.main(["--refresh-stations"])
        with run.step("provisional (horario + web)"):
            provisional.main([])
        with run.step("records"):
            records.main([])
        with run.step("stats"):
            stats.main([])
        with run.step("rankings"):
            rankings.main([])
        with run.step("export"):
            export.main([])
        with run.step("publish (HF)"):
            publish.main([])
        with run.step("notify (Telegram)"):
            notify.main([])


if __name__ == "__main__":
    main()
