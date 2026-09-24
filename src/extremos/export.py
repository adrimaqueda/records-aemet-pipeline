"""Genera los JSONs que consume la app.

Salida en `outputs/`:
  stations.json                 -- array, una entrada por estación activa (para el mapa)
  stations/{indicativo}.json    -- detalle por estación: vigentes + timeline de eventos
  README.md                     -- tarjeta del dataset HF (ver dataset_card.py)

Convenciones:
  - Solo se exportan estaciones marcadas `activa` en `station_coverage`.
  - Un evento "batido" es el que tiene `valor_anterior` (ver records.py). Los
    contadores (`totales`, `recientes15d`), `ultimoPorTipo` y `ultimoRecord`
    cuentan solo esos.
  - `eventos`: escalera completa de récords (todos los peldaños). Los que no se
    cuentan como batidos —el primero de la serie y los del warm-up— van con
    `valorAnterior: null` y `inicial: true`. Garantiza `vigentes ⊆ eventos`: el
    récord vigente siempre tiene su punto en el timeline (gráfico nunca vacío).
  - `vigentes.absolutoMax`: TMAX más alta jamás registrada (día más caluroso).
  - `vigentes.absolutoMin`: TMIN más alta jamás registrada (noche más cálida).
  - `mensuales`: 12 entradas (una por mes) con max/min vigentes y su fecha. El
    récord mensual que coincide con el absoluto vigente lleva `abs: true`.
  - `sinDatos`: huecos de cobertura interiores de ≥ `HUECO_MIN_DIAS` días, como
    intervalos `{desde, hasta, dias}`, para pintar un overlay sobre los gráficos.
    Un día con dato provisional no es hueco (en el gráfico se dibuja igual).
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb

from extremos import dataset_card
from extremos.config import HUECO_MIN_DIAS, OUTPUTS_DIR
from extremos.db import connect
from extremos.logconf import setup_logging

log = logging.getLogger("extremos.export")

TIPO_KEY = {
    "absoluto-max": "absolutoMax",
    "absoluto-min": "absolutoMin",
    "mensual-max":  "mensualMax",
    "mensual-min":  "mensualMin",
}
# Familia de cada tipo, para marcar `abs` en los mensuales.
FAMILIA = {"absoluto-max": "max", "absoluto-min": "min",
           "mensual-max": "max", "mensual-min": "min"}


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def _iso(d: date | None) -> str | None:
    return d.isoformat() if d else None


def _record(fecha: date, valor: float, provisional: bool) -> dict[str, Any]:
    rec: dict[str, Any] = {"fecha": fecha.isoformat(), "valor": valor}
    if provisional:
        rec["provisional"] = True
    return rec


def _eventos_por_estacion(con: duckdb.DuckDBPyConnection) -> dict[str, list[tuple]]:
    """`record_events` de las estaciones activas, agrupados por estación.

    Una sola consulta para todas (con ~830 estaciones el coste de hacer varias
    por estación era la sobrecarga por consulta). Cada lista va ordenada
    `fecha DESC, tipo`, así que el primer evento que aparece de cada categoría
    es el más reciente.
    """
    rows = con.execute("""
        SELECT e.indicativo, e.fecha, e.tipo, e.mes, e.valor, e.valor_anterior,
               e.dias_desde_anterior, e.provisional
        FROM record_events e
        JOIN station_coverage c USING (indicativo)
        WHERE c.activa
        ORDER BY e.indicativo, e.fecha DESC, e.tipo
    """).fetchall()
    out: dict[str, list[tuple]] = {}
    for indicativo, *ev in rows:
        out.setdefault(indicativo, []).append(tuple(ev))
    return out


def _huecos_por_estacion(con: duckdb.DuckDBPyConnection) -> dict[str, list[dict[str, Any]]]:
    rows = con.execute("""
        WITH dd AS (
            SELECT DISTINCT o.indicativo, o.fecha
            FROM observations o
            JOIN station_coverage c USING (indicativo)
            WHERE c.activa AND (o.tmax IS NOT NULL OR o.tmin IS NOT NULL)
        ),
        gaps AS (
            SELECT indicativo,
                   LAG(fecha) OVER (PARTITION BY indicativo ORDER BY fecha) + 1 AS desde,
                   fecha - 1 AS hasta
            FROM dd
        )
        SELECT indicativo, desde, hasta, (hasta - desde + 1)::INTEGER AS dias
        FROM gaps
        WHERE hasta - desde + 1 >= ?
        ORDER BY indicativo, desde
    """, [HUECO_MIN_DIAS]).fetchall()
    out: dict[str, list[dict[str, Any]]] = {}
    for indicativo, desde, hasta, dias in rows:
        out.setdefault(indicativo, []).append(
            {"desde": desde.isoformat(), "hasta": hasta.isoformat(), "dias": dias}
        )
    return out


def _resumen_eventos(eventos: list[tuple]) -> dict[str, Any]:
    """Campos del mapa y de la ficha derivados de los eventos de una estación."""
    ceros = dict.fromkeys(TIPO_KEY.values(), 0)
    totales, recientes = dict(ceros), dict(ceros)
    ultimo_por_tipo: dict[str, Any] = dict.fromkeys(TIPO_KEY.values())
    ultimo_record = None
    desde_15d = date.today() - timedelta(days=15)
    for fecha, tipo, mes, valor, valor_anterior, _dias, provisional in eventos:
        if valor_anterior is None:
            continue
        key = TIPO_KEY[tipo]
        totales[key] += 1
        if fecha >= desde_15d:
            recientes[key] += 1
        if ultimo_por_tipo[key] is None:
            entry = {"fecha": fecha.isoformat(), "valor": valor}
            if mes is not None:
                entry["mes"] = mes
            if provisional:
                entry["provisional"] = True
            ultimo_por_tipo[key] = entry
        if ultimo_record is None:
            ultimo_record = {"fecha": fecha.isoformat(), "tipo": tipo, "mes": mes,
                             "valor": valor, "provisional": provisional}
    return {"totales": totales, "recientes15d": recientes,
            "ultimoPorTipo": ultimo_por_tipo, "ultimoRecord": ultimo_record}


def _vigentes(eventos: list[tuple]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Récords vigentes (el último evento de cada categoría, inicial o no).

    Devuelve (`vigentes` absolutos, `mensuales` de los 12 meses). Cada mensual
    lleva `abs: true` si cae el mismo día que el absoluto vigente de su familia
    (el mes en que se fijó el máximo/mínimo histórico).
    """
    vigentes: dict[str, Any] = {"absolutoMax": None, "absolutoMin": None}
    abs_fecha: dict[str, date] = {}
    mensuales = [{"mes": m, "max": None, "min": None} for m in range(1, 13)]
    for fecha, tipo, mes, valor, _va, _dias, provisional in eventos:
        fam = FAMILIA[tipo]
        if mes is None:
            if vigentes[TIPO_KEY[tipo]] is None:
                vigentes[TIPO_KEY[tipo]] = _record(fecha, valor, provisional)
                abs_fecha[fam] = fecha
        elif mensuales[mes - 1][fam] is None:
            mensuales[mes - 1][fam] = _record(fecha, valor, provisional)
    for m in mensuales:
        for fam in ("max", "min"):
            if m[fam] and m[fam]["fecha"] == _iso(abs_fecha.get(fam)):
                m[fam]["abs"] = True
    return vigentes, mensuales


def _timeline(eventos: list[tuple]) -> list[dict[str, Any]]:
    """Timeline desc de la escalera completa; `inicial` marca los no batidos."""
    out = []
    for fecha, tipo, mes, valor, valor_anterior, dias, provisional in eventos:
        ev: dict[str, Any] = {
            "fecha": fecha.isoformat(), "tipo": tipo, "mes": mes, "valor": valor,
            "valorAnterior": valor_anterior, "diasDesdeAnterior": dias,
            "provisional": provisional,
        }
        if valor_anterior is None:
            ev["inicial"] = True
        out.append(ev)
    return out


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    p = argparse.ArgumentParser(description="Genera JSONs en outputs/")
    p.add_argument("--clean", action="store_true",
                   help="Borra outputs/ antes de regenerar")
    args = p.parse_args(argv)

    con = connect()
    if args.clean and OUTPUTS_DIR.exists():
        shutil.rmtree(OUTPUTS_DIR)

    estaciones = con.execute("""
        SELECT s.indicativo, s.nombre, s.provincia, s.altitud, s.latitud, s.longitud,
               c.datos_desde, c.datos_hasta, c.dias_con_datos
        FROM stations s
        JOIN station_coverage c USING (indicativo)
        WHERE c.activa
        ORDER BY s.indicativo
    """).fetchall()
    eventos = _eventos_por_estacion(con)
    huecos = _huecos_por_estacion(con)

    summary = []
    for ind, nombre, provincia, altitud, lat, lon, desde, hasta, dias in estaciones:
        evs = eventos.get(ind, [])
        r = _resumen_eventos(evs)
        tot = r["totales"]
        cabecera = {
            "indicativo": ind, "nombre": nombre, "provincia": provincia,
            "altitud": altitud, "lat": lat, "lon": lon,
            "datosDesde": _iso(desde), "datosHasta": _iso(hasta), "diasConDatos": dias,
        }
        summary.append({
            **cabecera,
            "ultimoPorTipo": r["ultimoPorTipo"],
            "recientes15d": r["recientes15d"],
            "totalRecordsAbsolutos": tot["absolutoMax"] + tot["absolutoMin"],
            "totalRecordsMensuales": tot["mensualMax"] + tot["mensualMin"],
            "totales": tot,
        })
        vigentes, mensuales = _vigentes(evs)
        write_json(OUTPUTS_DIR / "stations" / f"{ind}.json", {
            **cabecera,
            "totales": tot,
            "vigentes": vigentes,
            "ultimoRecord": r["ultimoRecord"],
            "mensuales": mensuales,
            "eventos": _timeline(evs),
            "sinDatos": huecos.get(ind, []),
        })

    write_json(OUTPUTS_DIR / "stations.json", summary)
    log.info("stations.json y %d fichas de estación escritos en %s/",
             len(summary), OUTPUTS_DIR)

    # Tarjeta del dataset: publish la sube con las cifras de esta misma pasada.
    dataset_card.write(con, OUTPUTS_DIR)
    log.info("README.md (tarjeta del dataset) generado")


if __name__ == "__main__":
    main()
