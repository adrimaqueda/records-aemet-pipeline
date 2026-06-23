"""Parsers para los formatos crudos de AEMET / datania."""
from __future__ import annotations

import re
from typing import Any

_COORD_RE = re.compile(r"^(\d{2})(\d{2})(\d{2})([NSEW])$")


def parse_decimal(value: Any) -> float | None:
    """Convierte strings con coma decimal a float. Centinelas de AEMET → None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s or s in {"Ip", "Acum", "Varias"}:
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


def parse_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def parse_coord(value: Any) -> float | None:
    """Convierte coords DMS de AEMET (ej. "405729N" → 40.9581) a grados decimales."""
    if not value:
        return None
    m = _COORD_RE.match(str(value).strip())
    if not m:
        return None
    deg, mn, sec, hemi = m.groups()
    val = int(deg) + int(mn) / 60 + int(sec) / 3600
    if hemi in ("S", "W"):
        val = -val
    return val


OBSERVATION_FIELDS = (
    ("indicativo", "indicativo", str),
    ("fecha",      "fecha",      str),
    ("tmed",       "tmed",       parse_decimal),
    ("tmin",       "tmin",       parse_decimal),
    ("tmax",       "tmax",       parse_decimal),
    ("horatmin",   "horatmin",   str),
    ("horatmax",   "horatmax",   str),
    ("prec",       "prec",       parse_decimal),
    ("sol",        "sol",        parse_decimal),
    ("hr_media",   "hrMedia",    parse_decimal),
    ("vel_media",  "velmedia",   parse_decimal),
    ("pres_max",   "presMax",    parse_decimal),
    ("pres_min",   "presMin",    parse_decimal),
)


def normalize_observation(raw: dict[str, Any]) -> dict[str, Any]:
    """Aplica conversores a una observación cruda; devuelve dict listo para insertar."""
    out: dict[str, Any] = {}
    for db_col, json_key, conv in OBSERVATION_FIELDS:
        value = raw.get(json_key)
        if conv is str:
            out[db_col] = value if value not in (None, "") else None
        else:
            out[db_col] = conv(value)
    return out


def normalize_hourly(raw: dict[str, Any]) -> dict[str, Any]:
    """Normaliza una observación horaria de `/observacion/convencional/todas`.

    El identificador de estación viene como `idema` (equivalente al `indicativo`
    climatológico) y `fint` es la fecha-hora del intervalo en UTC. `ta` es la
    temperatura instantánea; `tamax`/`tamin` los extremos de la última hora.
    """
    return {
        "indicativo": raw.get("idema") or None,
        "fint": raw.get("fint") or None,
        "ta": parse_decimal(raw.get("ta")),
        "tamax": parse_decimal(raw.get("tamax")),
        "tamin": parse_decimal(raw.get("tamin")),
    }


def normalize_station(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "indicativo": raw.get("indicativo"),
        "nombre":     (raw.get("nombre") or "").strip() or None,
        "provincia":  (raw.get("provincia") or "").strip() or None,
        "altitud":    parse_int(raw.get("altitud")),
        "latitud":    parse_coord(raw.get("latitud")),
        "longitud":   parse_coord(raw.get("longitud")),
        "indsinop":   (raw.get("indsinop") or "").strip() or None,
    }
