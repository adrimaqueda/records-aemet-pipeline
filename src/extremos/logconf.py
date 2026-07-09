"""Configuración de logging compartida por todos los CLI del pipeline."""
from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager
from typing import Iterator

# Librerías que loguean cada petición HTTP a nivel INFO; a WARNING no ensucian
# los logs del cron. Se silencian siempre, las use o no cada comando.
_NOISY_LOGGERS = ("httpx", "httpcore", "urllib3", "huggingface_hub")

log = logging.getLogger("extremos.run")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
    if not sys.stderr.isatty():
        # En cron, las barras de progreso de huggingface_hub llenan el log de
        # cientos de líneas de retorno de carro; solo valen en un terminal.
        from huggingface_hub.utils import disable_progress_bars

        disable_progress_bars()


def fmt_duration(seconds: float) -> str:
    s = round(seconds)
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"


class Run:
    """Delimita una ejecución en el log: banner de inicio, pasos cronometrados
    y línea final con OK/FALLO y duraciones, pensado para logs de cron donde
    se acumulan muchas ejecuciones seguidas.

        with Run("daily", total=7) as run:
            with run.step("fetch"):
                fetch.main([])

    Si un paso lanza una excepción, el traceback sale por el logger (con
    timestamp) en vez de por stderr suelto, y el proceso termina con exit
    code 1 para que el wrapper del cron detecte el fallo.
    """

    def __init__(self, name: str, total: int | None = None) -> None:
        self.name = name
        self.total = total
        self.timings: list[tuple[str, float]] = []
        self.failed_step: str | None = None

    def __enter__(self) -> "Run":
        self._t0 = time.monotonic()
        log.info("════════ ▶ INICIO %s ════════", self.name)
        return self

    @contextmanager
    def step(self, name: str) -> Iterator[None]:
        idx = len(self.timings) + 1
        label = f"[{idx}/{self.total}] {name}" if self.total else name
        log.info("%s…", label)
        t = time.monotonic()
        try:
            yield
        except SystemExit:
            # Aborto deliberado (argparse, validaciones): sin traceback.
            self.failed_step = name
            raise
        except BaseException:
            self.failed_step = name
            log.exception("✗ %s FALLÓ tras %s", label, fmt_duration(time.monotonic() - t))
            raise
        dur = time.monotonic() - t
        self.timings.append((name, dur))
        log.info("✓ %s en %s", label, fmt_duration(dur))

    def __exit__(self, exc_type, exc, tb) -> bool:
        total = fmt_duration(time.monotonic() - self._t0)
        if exc_type is None:
            log.info("════════ ✔ FIN %s · OK · %s ════════", self.name, total)
            if self.timings:
                log.info(
                    "  %s",
                    " · ".join(f"{n} {fmt_duration(d)}" for n, d in self.timings),
                )
            return False
        donde = f" en {self.failed_step}" if self.failed_step else ""
        if issubclass(exc_type, SystemExit):
            if isinstance(exc.code, str):
                # `raise SystemExit("mensaje")`: al log con timestamp, no a
                # stderr suelto; el exit code pasa a ser 1, como haría Python.
                log.error("%s", exc.code)
            log.error("════════ ✖ FIN %s · ABORTADO%s · %s ════════", self.name, donde, total)
            if isinstance(exc.code, str):
                raise SystemExit(1)
            return False
        if self.failed_step is None:
            # Excepción fuera de todo paso: que también quede en el log.
            log.error("Error no capturado", exc_info=(exc_type, exc, tb))
        log.error("════════ ✖ FIN %s · FALLO%s · %s ════════", self.name, donde, total)
        raise SystemExit(1)
