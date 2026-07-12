"""Extremos diarios desde los CSV de resumen de la web de AEMET (www.aemet.es).

La web publica un CSV nacional con los extremos diarios (tmax/tmin con su hora,
tmed, racha, precipitación) de ~830 estaciones: el día en curso (w=1) y cada uno
de los 7 días anteriores completos (w=2 con x=d01..d07, donde d07 es ayer). Es
dato provisional en tiempo real, pero cubre el día natural COMPLETO, cosa que la
API horaria de OpenData no garantiza (devuelve ~12-13 h aunque documenta 24), y
vive en un host distinto de opendata.aemet.es, así que además sirve de fallback
cuando OpenData está caída. No requiere api_key.

El CSV identifica estaciones por (nombre, provincia), sin indicativo. El mapeo a
indicativo se scrapea de la página HTML del resumen (cada fila enlaza a
`ultimosdatos?l=<indicativo>`) y se cachea en `data/web_station_map.json`; si al
cruzar aparecen nombres sin mapear (estación nueva o renombrada) se reconstruye
una vez y se reintenta.

Ojo: endpoint web sin contrato (AEMET puede cambiarlo sin avisar). Por eso todo
el parseo valida lo que asume (content-type CSV, nº de estaciones del mapeo,
fecha declarada en el propio fichero) y el llamador lo trata como fuente
prescindible: si algo no cuadra, se avisa y se sigue sin datos web.
"""
from __future__ import annotations

import csv
import io
import json
import logging
import re
from datetime import date

import httpx
import polars as pl
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from extremos.config import DATA_DIR

log = logging.getLogger("extremos.webcsv")

BASE_URL = "https://www.aemet.es/es/eltiempo/observacion"
MAP_PATH = DATA_DIR / "web_station_map.json"

# La web sirve en ISO-8859-15 (a veces sin declararlo); latin-1 no falla nunca
# al decodificar, así que los caracteres raros degradan en vez de romper.
_ENCODING = "latin-1"
_TIMEOUT = 30.0
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; records-aemet-pipeline)"}

# Mínimo de estaciones para dar por bueno un scrape del mapeo (hoy son ~830;
# muchas menos indica que AEMET cambió el HTML y la regex ya no casa).
_MAP_MIN_STATIONS = 500

_MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}
_FECHA_RE = re.compile(r"(\d{1,2})\s+(\w+)\s+(\d{4})")
_NUM_RE = re.compile(r"-?\d+(?:[.,]\d+)?")
# Fila de estación en el HTML del resumen nacional: enlace con el indicativo y
# el nombre, seguido de la celda de provincia.
_MAP_ROW_RE = re.compile(
    r"ultimosdatos\?k=&amp;l=([0-9A-Z]+)&amp;w=2&amp;datos=det&amp;f=\"[^>]*>"
    r"([^<]+)</a></td>\s*<td[^>]*>([^<]+)</td>"
)


class WebCsvError(RuntimeError):
    pass


@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type(httpx.HTTPError),
)
def _get(client: httpx.Client, path: str, params: dict[str, str]) -> str:
    r = client.get(path, params=params)
    r.raise_for_status()
    return r.content.decode(_ENCODING, errors="replace")


def _parse_fecha(line: str) -> date | None:
    m = _FECHA_RE.search(line)
    if not m:
        return None
    dia, mes, anio = m.groups()
    mes_n = _MESES.get(mes.lower())
    return date(int(anio), mes_n, int(dia)) if mes_n else None


def _parse_temp(cell: str) -> float | None:
    """Extrae el valor de una celda tipo '36.8 (16:40)' (la hora se descarta)."""
    m = _NUM_RE.search((cell or "").split("(")[0])
    return float(m.group().replace(",", ".")) if m else None


def _parse_summary(text: str) -> tuple[date | None, list[dict]]:
    """Parsea un CSV de resumen → (fecha declarada, filas nombre/provincia/tmax/tmin).

    La fecha sale de la línea "Fecha: sábado, 11 julio 2026" (día cerrado, w=2)
    o, si no existe (día en curso, w=1), de la línea "Actualizado: …".
    """
    lines = text.splitlines()
    fecha = None
    for line in lines[:6]:
        if line.startswith("Fecha:"):
            fecha = _parse_fecha(line)
            break
        if fecha is None and line.startswith("Actualizado:"):
            fecha = _parse_fecha(line)

    idx: dict[str, int] = {}
    rows: list[dict] = []
    for record in csv.reader(io.StringIO(text)):
        if not idx:
            for i, col in enumerate(record):
                if col.startswith("Estaci"):
                    idx["nombre"] = i
                elif col == "Provincia":
                    idx["provincia"] = i
                elif col.startswith("Temperatura m") and "xima" in col:
                    idx["tmax"] = i
                elif col.startswith("Temperatura m") and "nima" in col:
                    idx["tmin"] = i
            if len(idx) < 4:
                idx = {}
            continue
        if len(record) <= max(idx.values()):
            continue
        rows.append({
            "nombre": record[idx["nombre"]].strip(),
            "provincia": record[idx["provincia"]].strip(),
            "tmax": _parse_temp(record[idx["tmax"]]),
            "tmin": _parse_temp(record[idx["tmin"]]),
        })
    return fecha, rows


def _load_map() -> dict[tuple[str, str], str]:
    if not MAP_PATH.exists():
        return {}
    try:
        triples = json.loads(MAP_PATH.read_text(encoding="utf-8"))
        return {(n, p): ind for n, p, ind in triples}
    except (ValueError, TypeError):
        log.warning("Mapeo web corrupto en %s; se reconstruirá.", MAP_PATH)
        return {}


def refresh_station_map() -> dict[tuple[str, str], str]:
    """Scrapea (nombre, provincia) → indicativo del HTML y lo cachea en disco."""
    log.info("Reconstruyendo mapeo nombre→indicativo desde la web de AEMET…")
    with httpx.Client(base_url=BASE_URL, headers=_HEADERS, timeout=_TIMEOUT) as client:
        html = _get(client, "/ultimosdatos", {"k": "esp", "w": "2", "datos": "det"})
    pairs = _MAP_ROW_RE.findall(html)
    if len(pairs) < _MAP_MIN_STATIONS:
        raise WebCsvError(
            f"Solo {len(pairs)} estaciones al scrapear el mapeo; "
            "¿ha cambiado el HTML de aemet.es?"
        )
    mapping = {(n.strip(), p.strip()): ind for ind, n, p in pairs}
    MAP_PATH.write_text(
        json.dumps([[n, p, ind] for (n, p), ind in sorted(mapping.items())],
                   ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("Mapeo web actualizado: %d estaciones.", len(mapping))
    return mapping


def _fetch_days(dates: list[date]) -> dict[date, list[dict]]:
    """Descarga el CSV de cada día pedido. Días fuera de la ventana web se omiten."""
    hoy = date.today()
    out: dict[date, list[dict]] = {}
    with httpx.Client(base_url=BASE_URL, headers=_HEADERS, timeout=_TIMEOUT) as client:
        for d in sorted(set(dates)):
            k = (hoy - d).days
            if k == 0:
                path = "/ultimosdatos_espana_resumen-hoy.csv"
                params = {"k": "esp", "datos": "det", "w": "1", "f": ""}
            elif 1 <= k <= 7:
                path = "/ultimosdatos_espana_resumenes-diarios-anteriores.csv"
                params = {"k": "", "datos": "det", "w": "2", "f": "", "x": f"d{8 - k:02d}"}
            else:
                log.warning("Día %s fuera de la ventana del CSV web (hoy-7..hoy); se omite.", d)
                continue
            try:
                text = _get(client, path, params)
                declared, rows = _parse_summary(text)
            except Exception as e:  # noqa: BLE001 — fuente prescindible, día a día
                log.warning("CSV web de %s no disponible: %s", d, e)
                continue
            if not rows:
                log.warning("CSV web de %s sin filas parseables; se omite.", d)
                continue
            # La fecha que manda es la que declara el propio fichero: en el
            # cambio de día los índices d01..d07 se desplazan y podríamos
            # recibir un día distinto del pedido.
            fecha = declared or d
            if fecha != d:
                log.warning("Pedí el CSV web de %s pero AEMET sirvió %s; uso la fecha declarada.", d, fecha)
            if abs((hoy - fecha).days) > 8:
                log.warning("Fecha declarada %s inverosímil; se omite.", fecha)
                continue
            out[fecha] = rows
    return out


def daily_extremes(dates: list[date]) -> pl.DataFrame:
    """Extremos diarios web de los días pedidos → DF (indicativo, fecha, tmax, tmin).

    Días no disponibles se omiten con aviso; si nada es utilizable devuelve un
    DataFrame vacío (el llamador ya trata esta fuente como opcional).
    """
    by_date = _fetch_days(dates)
    if not by_date:
        return pl.DataFrame()

    mapping = _load_map()
    for intento in ("cache", "refresco"):
        out, unmatched = [], set()
        for fecha, rows in by_date.items():
            for r in rows:
                if r["tmax"] is None and r["tmin"] is None:
                    continue
                ind = mapping.get((r["nombre"], r["provincia"]))
                if not ind:
                    unmatched.add((r["nombre"], r["provincia"]))
                    continue
                out.append({"indicativo": ind, "fecha": fecha,
                            "tmax": r["tmax"], "tmin": r["tmin"]})
        if not unmatched or intento == "refresco":
            break
        # Nombres sin mapear: el cache está desactualizado (estación nueva o
        # renombrada) o no existía. Un único refresco por pasada.
        try:
            mapping = refresh_station_map()
        except Exception as e:  # noqa: BLE001
            log.warning("No se pudo refrescar el mapeo web: %s", e)
            break

    if unmatched:
        log.warning(
            "%d estaciones del CSV web sin indicativo conocido (p. ej. %s); se omiten.",
            len(unmatched), "; ".join(f"{n} ({p})" for n, p in sorted(unmatched)[:3]),
        )
    if not out:
        return pl.DataFrame()
    return pl.DataFrame(
        out,
        schema={"indicativo": pl.Utf8, "fecha": pl.Date,
                "tmax": pl.Float64, "tmin": pl.Float64},
    )
