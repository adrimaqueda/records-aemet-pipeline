"""Genera los JSONs que consume la app.

Salida en `pipeline/outputs/`:
  stations.json                 -- array, una entrada por estación activa (para el mapa)
  stations/{indicativo}.json    -- detalle por estación: vigentes + timeline de eventos

Convenciones:
  - Solo se exportan estaciones marcadas `activa` en `station_coverage`.
  - Los contadores (`totales`) cuentan solo récords realmente batidos
    (`valor_anterior IS NOT NULL`). El primer valor de cada serie define el récord
    inicial pero no es un "evento de récord".
  - `eventos`: escalera completa de récords (todos los peldaños). Los que no se
    cuentan como batidos —el primero de la serie y los del warm-up— van con
    `valorAnterior: null` y `inicial: true`. Garantiza `vigentes ⊆ eventos`: el
    récord vigente siempre tiene su punto en el timeline (gráfico nunca vacío).
  - `vigentes.absolutoMax`: TMAX más alta jamás registrada (día más caluroso).
  - `vigentes.absolutoMin`: TMIN más alta jamás registrada (noche más cálida).
  - `mensuales`: 12 entradas (una por mes) con max/min vigentes y su fecha. El
    récord mensual que coincide con el absoluto vigente lleva `abs: true`.
  - `ultimoRecord` en el detalle: evento de récord más reciente, cualquier tipo.
  - `sinDatos`: tramos de cobertura sin ningún dato (huecos interiores), como
    intervalos `{desde, hasta, dias}`, para pintar un overlay sobre los gráficos.
    Solo huecos de ≥ `HUECO_MIN_DIAS` días (ver config).
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from extremos import dataset_card
from extremos.config import HUECO_MIN_DIAS, OUTPUTS_DIR
from extremos.db import connect

log = logging.getLogger("extremos.export")

STATIONS_DIR_NAME = "stations"


def _iso(d: date | None) -> str | None:
    return d.isoformat() if d else None


def _record(fecha: date | None, valor: float | None,
            provisional: bool = False) -> dict[str, Any] | None:
    if fecha is None or valor is None:
        return None
    rec: dict[str, Any] = {"fecha": fecha.isoformat(), "valor": valor}
    if provisional:
        rec["provisional"] = True
    return rec


_TIPO_TO_KEY = {
    "absoluto-max": "absolutoMax",
    "absoluto-min": "absolutoMin",
    "mensual-max":  "mensualMax",
    "mensual-min":  "mensualMin",
}


def _build_stations_summary(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Una fila por estación activa para el mapa.

    Devuelve, para cada estación, el último evento de récord realmente batido
    de cada una de las 4 categorías, para que el mapa pueda filtrarlas/colorearlas
    de forma independiente.
    """
    base_rows = con.execute("""
        WITH counts AS (
            SELECT indicativo,
                COUNT(*) FILTER (WHERE tipo='absoluto-max' AND valor_anterior IS NOT NULL) AS n_abs_max,
                COUNT(*) FILTER (WHERE tipo='absoluto-min' AND valor_anterior IS NOT NULL) AS n_abs_min,
                COUNT(*) FILTER (WHERE tipo='mensual-max' AND valor_anterior IS NOT NULL) AS n_mes_max,
                COUNT(*) FILTER (WHERE tipo='mensual-min' AND valor_anterior IS NOT NULL) AS n_mes_min
            FROM record_events GROUP BY indicativo
        )
        SELECT
            s.indicativo, s.nombre, s.provincia, s.altitud,
            s.latitud, s.longitud,
            c.datos_desde, c.datos_hasta, c.dias_con_datos,
            COALESCE(co.n_abs_max, 0) AS n_abs_max,
            COALESCE(co.n_abs_min, 0) AS n_abs_min,
            COALESCE(co.n_mes_max, 0) AS n_mes_max,
            COALESCE(co.n_mes_min, 0) AS n_mes_min
        FROM stations s
        JOIN station_coverage c USING (indicativo)
        LEFT JOIN counts co USING (indicativo)
        WHERE c.activa
        ORDER BY s.indicativo
    """).fetchall()

    # Último evento de cada (indicativo, tipo). Solo récords realmente batidos.
    last_per_tipo_rows = con.execute("""
        SELECT indicativo, tipo, fecha, valor, mes, provisional
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY indicativo, tipo
                ORDER BY fecha DESC
            ) AS rn
            FROM record_events
            WHERE valor_anterior IS NOT NULL
        )
        WHERE rn = 1
    """).fetchall()

    ultimo_por_estacion: dict[str, dict[str, dict[str, Any]]] = {}
    for indicativo, tipo, fecha, valor, mes, provisional in last_per_tipo_rows:
        key = _TIPO_TO_KEY.get(tipo)
        if key is None:
            continue
        entry: dict[str, Any] = {"fecha": fecha.isoformat(), "valor": valor}
        if mes is not None:
            entry["mes"] = int(mes)
        if provisional:
            entry["provisional"] = True
        ultimo_por_estacion.setdefault(indicativo, {})[key] = entry

    # Conteo de récords batidos en los últimos 15 días por (indicativo, tipo).
    recientes_rows = con.execute("""
        SELECT indicativo, tipo, COUNT(*) AS n
        FROM record_events
        WHERE valor_anterior IS NOT NULL
          AND fecha >= CURRENT_DATE - INTERVAL '15 days'
        GROUP BY indicativo, tipo
    """).fetchall()
    recientes_por_estacion: dict[str, dict[str, int]] = {}
    for indicativo, tipo, n in recientes_rows:
        key = _TIPO_TO_KEY.get(tipo)
        if key is None:
            continue
        recientes_por_estacion.setdefault(
            indicativo, {"absolutoMax": 0, "absolutoMin": 0, "mensualMax": 0, "mensualMin": 0}
        )[key] = int(n)

    out: list[dict[str, Any]] = []
    for (indicativo, nombre, provincia, altitud, lat, lon,
         desde, hasta, dias, n_abs_max, n_abs_min, n_mes_max, n_mes_min) in base_rows:
        ultimo = ultimo_por_estacion.get(indicativo, {})
        recientes = recientes_por_estacion.get(
            indicativo, {"absolutoMax": 0, "absolutoMin": 0, "mensualMax": 0, "mensualMin": 0}
        )
        out.append({
            "indicativo": indicativo,
            "nombre": nombre,
            "provincia": provincia,
            "altitud": altitud,
            "lat": lat,
            "lon": lon,
            "datosDesde": _iso(desde),
            "datosHasta": _iso(hasta),
            "diasConDatos": dias,
            "ultimoPorTipo": {
                "absolutoMax": ultimo.get("absolutoMax"),
                "absolutoMin": ultimo.get("absolutoMin"),
                "mensualMax":  ultimo.get("mensualMax"),
                "mensualMin":  ultimo.get("mensualMin"),
            },
            # Récords realmente batidos en los últimos 15 días, por tipo.
            "recientes15d": recientes,
            "totalRecordsAbsolutos": n_abs_max + n_abs_min,
            "totalRecordsMensuales": n_mes_max + n_mes_min,
            "totales": {
                "absolutoMax": n_abs_max,
                "absolutoMin": n_abs_min,
                "mensualMax": n_mes_max,
                "mensualMin": n_mes_min,
            },
        })
    return out


def _vigentes_for(con: duckdb.DuckDBPyConnection, indicativo: str) -> dict[str, Any]:
    """Récords absolutos vigentes (último evento de cada categoría)."""
    abs_max = con.execute("""
        SELECT fecha, valor, provisional FROM record_events
        WHERE indicativo=? AND tipo='absoluto-max'
        ORDER BY fecha DESC LIMIT 1
    """, [indicativo]).fetchone()
    abs_min = con.execute("""
        SELECT fecha, valor, provisional FROM record_events
        WHERE indicativo=? AND tipo='absoluto-min'
        ORDER BY fecha DESC LIMIT 1
    """, [indicativo]).fetchone()
    return {
        "absolutoMax": _record(abs_max[0], abs_max[1], abs_max[2]) if abs_max else None,
        "absolutoMin": _record(abs_min[0], abs_min[1], abs_min[2]) if abs_min else None,
    }


def _mensuales_for(con: duckdb.DuckDBPyConnection, indicativo: str) -> list[dict[str, Any]]:
    """Récords mensuales vigentes: 12 entradas, cada una con max y min y sus fechas.

    "max" = TMAX más alta de ese mes calendario · "min" = TMIN más alta de ese mes.

    Cada récord mensual lleva `abs: true` cuando coincide con el récord ABSOLUTO
    vigente de su familia (el mes en que se fijó el máximo/mínimo histórico). Se
    detecta por la fecha: el absoluto vigente cae en un día concreto, y el récord
    mensual vigente de ese mes es ese mismo día. La clave se omite cuando es false
    (igual que `provisional`).
    """
    rows = con.execute("""
        WITH ranked AS (
            SELECT
                mes, tipo, fecha, valor, provisional,
                ROW_NUMBER() OVER (PARTITION BY mes, tipo ORDER BY fecha DESC) AS rn
            FROM record_events
            WHERE indicativo=? AND tipo IN ('mensual-max', 'mensual-min')
        )
        SELECT mes, tipo, fecha, valor, provisional FROM ranked WHERE rn = 1
        ORDER BY mes, tipo
    """, [indicativo]).fetchall()

    # Fecha del récord absoluto vigente de cada familia (max/min) para marcar qué
    # récord mensual coincide con el absoluto.
    abs_fecha: dict[str, date] = {}
    for tipo, key in (("absoluto-max", "max"), ("absoluto-min", "min")):
        r = con.execute("""
            SELECT fecha FROM record_events
            WHERE indicativo=? AND tipo=? ORDER BY fecha DESC LIMIT 1
        """, [indicativo, tipo]).fetchone()
        if r is not None:
            abs_fecha[key] = r[0]

    by_mes: dict[int, dict[str, Any]] = {m: {"mes": m, "max": None, "min": None} for m in range(1, 13)}
    for mes, tipo, fecha, valor, provisional in rows:
        key = "max" if tipo == "mensual-max" else "min"
        rec = _record(fecha, valor, provisional)
        if rec is not None and abs_fecha.get(key) == fecha:
            rec["abs"] = True
        by_mes[int(mes)][key] = rec
    return [by_mes[m] for m in range(1, 13)]


def _ultimo_record_for(con: duckdb.DuckDBPyConnection, indicativo: str) -> dict[str, Any] | None:
    """El evento de récord más reciente (cualquier tipo) — el "último récord batido"."""
    row = con.execute("""
        SELECT fecha, tipo, mes, valor, provisional
        FROM record_events
        WHERE indicativo=? AND valor_anterior IS NOT NULL
        ORDER BY fecha DESC, tipo LIMIT 1
    """, [indicativo]).fetchone()
    if not row:
        return None
    fecha, tipo, mes, valor, provisional = row
    return {
        "fecha": fecha.isoformat(),
        "tipo": tipo,
        "mes": int(mes) if mes is not None else None,
        "valor": valor,
        "provisional": bool(provisional),
    }


def _eventos_for(con: duckdb.DuckDBPyConnection, indicativo: str) -> list[dict[str, Any]]:
    """Timeline desc de la *escalera* completa de récords.

    Incluye todos los eventos que fijaron un récord (cada fila de record_events ya
    es un peldaño: solo se generan cuando el valor mejora el récord vigente),
    estén o no contabilizados como "batidos".

    Un evento cuenta como récord batido —y lleva ``valorAnterior`` no nulo— solo si
    superó al vigente con la serie ya madura. Los demás (el primero de cada serie y
    los fijados durante el warm-up) salen con ``valorAnterior: null`` y la marca
    ``inicial: true``: establecen el récord —por eso aparecen aquí y casan con
    ``vigentes``— pero no se contabilizan como batidos.

    De este modo se mantiene el invariante ``vigentes ⊆ eventos`` (el gráfico de
    evolución dibuja la escalera real y nunca queda vacío cuando hay récord
    vigente), mientras los contadores de récords batidos siguen filtrando
    ``valorAnterior != null`` por su cuenta (``totales``, ``ultimoRecord``…).
    """
    rows = con.execute("""
        SELECT fecha, tipo, mes, valor, valor_anterior, dias_desde_anterior, provisional
        FROM record_events
        WHERE indicativo=?
        ORDER BY fecha DESC, tipo
    """, [indicativo]).fetchall()
    eventos: list[dict[str, Any]] = []
    for fecha, tipo, mes, valor, valor_anterior, dias, provisional in rows:
        ev: dict[str, Any] = {
            "fecha": fecha.isoformat(),
            "tipo": tipo,
            "mes": mes,
            "valor": valor,
            "valorAnterior": valor_anterior,
            "diasDesdeAnterior": dias,
            "provisional": bool(provisional),
        }
        # `inicial`: el evento establece el récord pero no se cuenta como batido
        # (primer evento de la serie o fijado durante el warm-up).
        if valor_anterior is None:
            ev["inicial"] = True
        eventos.append(ev)
    return eventos


def _huecos_for(con: duckdb.DuckDBPyConnection, indicativo: str) -> list[dict[str, Any]]:
    """Huecos de cobertura (tramos sin ningún dato) para pintar un overlay.

    Cada hueco es un intervalo ``{desde, hasta, dias}`` de días consecutivos sin
    observación entre dos días que sí la tienen (huecos interiores; no hay tramos
    al principio ni al final por definición). ``dias`` = número de días sin dato.

    Un día "tiene dato" si registró TMAX o TMIN (provisional incluido: en el gráfico
    se dibuja igual, así que no es un hueco). Solo se exportan huecos de
    ``HUECO_MIN_DIAS`` días o más: en un eje de años, los de 1-2 días son invisibles
    y solo añaden ruido y peso al JSON.
    """
    rows = con.execute("""
        WITH dd AS (
            SELECT DISTINCT fecha
            FROM observations
            WHERE indicativo = ? AND (tmax IS NOT NULL OR tmin IS NOT NULL)
        ),
        gaps AS (
            SELECT
                LAG(fecha) OVER (ORDER BY fecha) + 1 AS desde,
                fecha - 1                           AS hasta,
                (fecha - LAG(fecha) OVER (ORDER BY fecha))::INTEGER - 1 AS dias
            FROM dd
        )
        SELECT desde, hasta, dias
        FROM gaps
        WHERE dias >= ?
        ORDER BY desde
    """, [indicativo, HUECO_MIN_DIAS]).fetchall()
    return [
        {"desde": desde.isoformat(), "hasta": hasta.isoformat(), "dias": int(dias)}
        for (desde, hasta, dias) in rows
    ]


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Genera JSONs en outputs/")
    p.add_argument("--clean", action="store_true",
                   help="Borra outputs/ antes de regenerar")
    args = p.parse_args(argv)

    con = connect()
    out_dir = OUTPUTS_DIR
    if args.clean and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = _build_stations_summary(con)
    _write_json(out_dir / "stations.json", summary)
    log.info("stations.json escrito (%d estaciones activas)", len(summary))

    detail_dir = out_dir / STATIONS_DIR_NAME
    detail_dir.mkdir(parents=True, exist_ok=True)
    for st in summary:
        indicativo = st["indicativo"]
        payload = {
            "indicativo": indicativo,
            "nombre": st["nombre"],
            "provincia": st["provincia"],
            "altitud": st["altitud"],
            "lat": st["lat"],
            "lon": st["lon"],
            "datosDesde": st["datosDesde"],
            "datosHasta": st["datosHasta"],
            "diasConDatos": st["diasConDatos"],
            "totales": st["totales"],
            "vigentes": _vigentes_for(con, indicativo),
            "ultimoRecord": _ultimo_record_for(con, indicativo),
            "mensuales": _mensuales_for(con, indicativo),
            "eventos": _eventos_for(con, indicativo),
            "sinDatos": _huecos_for(con, indicativo),
        }
        _write_json(detail_dir / f"{indicativo}.json", payload)
    log.info("Escritos %d JSONs de detalle en %s/", len(summary), detail_dir)

    # Tarjeta del dataset (README.md): publish la sube como descripción del
    # dataset en HF, con las cifras de esta misma pasada.
    dataset_card.write(con, out_dir)
    log.info("README.md (tarjeta del dataset) generado en %s/", out_dir)


if __name__ == "__main__":
    main(sys.argv[1:])
