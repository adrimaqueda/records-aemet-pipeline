"""Cliente HTTP para la API REST de AEMET OpenData.

La API funciona en dos pasos: la primera llamada devuelve una URL firmada
({"estado": 200, "datos": "<url>"}), y hay que hacer un segundo GET a esa URL
para obtener los datos reales en JSON.

Aplica un rate limit token-bucket (configurable) y reintentos con backoff
exponencial ante 429, 5xx o errores de red.
"""
from __future__ import annotations

import json as _json
import logging
import time
from collections import deque
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from extremos.config import AEMET_API_KEY, AEMET_BASE_URL, AEMET_RATE_LIMIT_PER_MIN

log = logging.getLogger("extremos.aemet")

# Techo de espera por intento. El servidor OpenData de AEMET devuelve 429 a
# menudo aunque vayas por debajo de tu propio rate limit; cuando lo hace suele
# mandar un Retry-After (~60 s). Lo respetamos, pero acotado para no colgar el
# cron indefinidamente si AEMET pide una espera absurda.
_MAX_WAIT_S = 120.0
_EXP_WAIT = wait_exponential(multiplier=2, min=2, max=60)


class AemetError(RuntimeError):
    pass


class AemetRateLimit(AemetError):
    """429 de AEMET. Lleva el Retry-After (en segundos) si la respuesta lo indica."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class AemetNoData(AemetError):
    """AEMET responde estado=404 "No hay datos que satisfagan esos criterios".

    No es un fallo: pasa de forma rutinaria al pedir el diario definitivo de un
    día que AEMET aún no ha publicado (lo hace con ~4-5 días de retraso). Es
    determinista, así que NO se reintenta y el llamador lo trata como "sin datos
    todavía", no como error.
    """


def _should_retry(exc: BaseException) -> bool:
    """Reintenta errores de red/HTTP y de AEMET, salvo el 404 'sin datos'."""
    if isinstance(exc, AemetNoData):
        return False
    return isinstance(exc, (httpx.HTTPError, AemetError))


def _retry_after_seconds(resp: httpx.Response) -> float | None:
    """Lee la cabecera Retry-After (segundos o fecha HTTP). None si no viene."""
    raw = (resp.headers.get("Retry-After") or "").strip()
    if not raw:
        return None
    if raw.isdigit():
        return float(raw)
    try:  # formato fecha HTTP (RFC 7231) en vez de segundos
        when = parsedate_to_datetime(raw)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return max((when - datetime.now(timezone.utc)).total_seconds(), 0.0)
    except (TypeError, ValueError):
        return None


def _aemet_wait(retry_state: Any) -> float:
    """Backoff exponencial, salvo que AEMET indique Retry-After en un 429."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(exc, AemetRateLimit) and exc.retry_after is not None:
        return min(exc.retry_after + 1.0, _MAX_WAIT_S)  # +1 s de margen
    return _EXP_WAIT(retry_state)


class _RateLimiter:
    """Token bucket sencillo: nunca más de `per_minute` llamadas en 60 s."""

    def __init__(self, per_minute: int) -> None:
        self.per_minute = per_minute
        self._calls: deque[float] = deque()

    def acquire(self) -> None:
        now = time.monotonic()
        while self._calls and now - self._calls[0] > 60:
            self._calls.popleft()
        if len(self._calls) >= self.per_minute:
            sleep_for = 60 - (now - self._calls[0]) + 0.05
            if sleep_for > 0:
                log.debug("Rate limit AEMET: durmiendo %.1fs", sleep_for)
                time.sleep(sleep_for)
                return self.acquire()
        self._calls.append(now)


class AemetClient:
    def __init__(self, api_key: str | None = None, *, timeout: float = 60.0) -> None:
        self.api_key = api_key or AEMET_API_KEY
        if not self.api_key:
            raise AemetError("AEMET_API_KEY no configurada")
        self._client = httpx.Client(
            base_url=AEMET_BASE_URL,
            headers={"api_key": self.api_key, "Accept": "application/json"},
            timeout=timeout,
        )
        self._limiter = _RateLimiter(AEMET_RATE_LIMIT_PER_MIN)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "AemetClient":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    @retry(
        reraise=True,
        stop=stop_after_attempt(7),
        wait=_aemet_wait,
        retry=retry_if_exception(_should_retry),
    )
    def _fetch_data_url(self, path: str) -> tuple[str, str | None]:
        """Primera llamada: devuelve (datos_url, metadatos_url)."""
        self._limiter.acquire()
        r = self._client.get(path)
        if r.status_code == 429:
            raise AemetRateLimit(
                f"429 Too Many Requests en {path}",
                retry_after=_retry_after_seconds(r),
            )
        r.raise_for_status()
        body = r.json()
        estado = body.get("estado")
        if estado == 404:
            raise AemetNoData(f"AEMET 404 (sin datos) en {path}: {body.get('descripcion')}")
        if estado not in (200, None):
            raise AemetError(f"AEMET estado={estado} en {path}: {body.get('descripcion')}")
        datos = body.get("datos")
        if not datos:
            raise AemetError(f"Respuesta AEMET sin campo 'datos' en {path}: {body}")
        return datos, body.get("metadatos")

    @retry(
        reraise=True,
        stop=stop_after_attempt(7),
        wait=_aemet_wait,
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    def _fetch_payload(self, url: str) -> Any:
        """Segunda llamada: descarga el JSON real desde la URL firmada.

        AEMET sirve el cuerpo en latin-1 (ISO-8859-15) aunque no siempre lo
        declara correctamente, así que forzamos la decodificación.
        """
        self._limiter.acquire()
        r = httpx.get(url, timeout=120.0)
        r.raise_for_status()
        try:
            return _json.loads(r.content.decode("latin-1"))
        except UnicodeDecodeError:
            return _json.loads(r.content.decode("utf-8", errors="replace"))

    def get(self, path: str) -> Any:
        """Patrón completo en dos pasos."""
        datos_url, _ = self._fetch_data_url(path)
        return self._fetch_payload(datos_url)

    def daily_observations(self, ini: str, fin: str) -> list[dict[str, Any]]:
        """Diarios de todas las estaciones entre dos fechas (max 31 días).

        `ini` y `fin` en formato YYYY-MM-DD; los formateamos al esquema AEMET.
        """
        ini_str = f"{ini}T00:00:00UTC"
        fin_str = f"{fin}T23:59:59UTC"
        path = (
            f"/api/valores/climatologicos/diarios/datos"
            f"/fechaini/{ini_str}/fechafin/{fin_str}/todasestaciones"
        )
        return self.get(path)

    def inventory_stations(self) -> list[dict[str, Any]]:
        """Inventario actualizado de todas las estaciones."""
        return self.get(
            "/api/valores/climatologicos/inventarioestaciones/todasestaciones"
        )

    def realtime_observations(self) -> list[dict[str, Any]]:
        """Observación convencional de las últimas ~24 h de todas las estaciones.

        Devuelve registros horarios (`idema`, `fint` en UTC, `ta`, `tamax`,
        `tamin`, …). Es la fuente para reconstruir récords provisionales mientras
        AEMET publica el diario definitivo (con ~5 días de retraso). Ojo: esta
        API sólo cubre las últimas 24 h, no un rango histórico.
        """
        return self.get("/api/observacion/convencional/todas")
