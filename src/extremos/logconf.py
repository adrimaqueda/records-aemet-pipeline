"""Configuración de logging compartida por todos los CLI del pipeline."""
from __future__ import annotations

import logging

# Librerías que loguean cada petición HTTP a nivel INFO; a WARNING no ensucian
# los logs del cron. Se silencian siempre, las use o no cada comando.
_NOISY_LOGGERS = ("httpx", "httpcore", "urllib3", "huggingface_hub")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
