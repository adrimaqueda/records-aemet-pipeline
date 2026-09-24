"""Orquesta el ciclo diario: fetch -> provisional -> records -> stats -> rankings -> export -> publish.

Si existe el módulo local `notify` (no se publica en el repo), añade al final
su aviso de los récords batidos en la pasada.
"""
from __future__ import annotations

from extremos.logconf import Run, setup_logging


def main() -> None:
    setup_logging()

    from extremos import export, fetch, provisional, publish, rankings, records, stats

    try:
        from extremos import notify
    except ImportError:
        notify = None

    with Run("daily", total=8 if notify else 7) as run:
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
        if notify:
            with run.step("notify"):
                notify.main([])


if __name__ == "__main__":
    main()
