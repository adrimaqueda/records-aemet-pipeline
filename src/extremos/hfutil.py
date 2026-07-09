"""Subidas a HuggingFace con reintentos.

El Hub devuelve de vez en cuando errores 5xx transitorios en el endpoint de
commit (visto un 500 el 2026-07-05 que tumbó el daily de las 21:00). La subida
es idempotente —un commit con los mismos ficheros—, así que reintentar con
espera es seguro. Los 4xx (auth, repo inexistente…) no se reintentan.
"""
from __future__ import annotations

import logging
import time

from huggingface_hub import HfApi
from huggingface_hub.errors import HfHubHTTPError

log = logging.getLogger("extremos.hfutil")

# Esperas entre intentos: total ~3,5 min antes de rendirse.
_BACKOFF = (30, 60, 120)


def upload_folder_with_retry(api: HfApi, **kwargs) -> None:
    """`api.upload_folder(**kwargs)` reintentando los errores 5xx del Hub."""
    for i, wait in enumerate((*_BACKOFF, None)):
        try:
            api.upload_folder(**kwargs)
            return
        except HfHubHTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if wait is None or status is None or status < 500:
                raise
            log.warning(
                "El Hub devolvió %s al subir (intento %d/%d); reintento en %d s…",
                status, i + 1, len(_BACKOFF) + 1, wait,
            )
            time.sleep(wait)
