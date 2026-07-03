"""Agregados anuales y mensuales para la página /datos de la app.

Salida: pipeline/outputs/stats.json

Cada "grupo" representa un ámbito de análisis:
  - "total"         → todas las estaciones del país.
  - "<provincia>"   → estaciones de esa provincia.

Por cada grupo y cada año (o año+mes) calculamos:
  - records: nº de récords batidos por tipo (4 categorías + total).
  - estacionesConDatos: nº de estaciones que reportaron tmin/tmax al menos
    una vez en ese periodo (denominador del porcentaje).
  - estacionesQueBatieron: nº de estaciones que batieron al menos un récord
    en ese periodo (numerador del porcentaje).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from extremos.config import MIN_HISTORICO_YEAR, OUTPUTS_DIR
from extremos.db import connect
from extremos.logconf import setup_logging
from extremos.provincias import PROVINCIA_NAMES, PROVINCIA_NORM, rows_for_duckdb

log = logging.getLogger("extremos.stats")


def _sort_key(s: str) -> str:
    """Clave de ordenación insensible a tildes ("Ávila" entre "Asturias" y "Badajoz")."""
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()

# Suelo de los agregados de /datos. Coincide con el del backfill histórico
# per-estación (config.MIN_HISTORICO_YEAR): una vez rellenado el histórico previo
# a 1975, los agregados nacionales/provinciales lo incluyen. El eje de años se
# deriva de los datos, así que basta con bajar este suelo para que aparezcan.
MIN_YEAR = MIN_HISTORICO_YEAR


def _create_lookup(con: duckdb.DuckDBPyConnection) -> None:
    """Crea una vista temporal `station_groups(indicativo, prov_id)`.

    La normalización colapsa BALEARES/ILLES BALEARS y SANTA CRUZ/STA. CRUZ
    bajo el mismo identificador estable.
    """
    rows = rows_for_duckdb()
    values = ",".join("(?, ?)" for _ in rows)
    flat: list[str] = [v for pair in rows for v in pair]
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE provincia_norm AS "
        f"SELECT * FROM (VALUES {values}) t(provincia, prov_id)",
        flat,
    )
    con.execute("""
        CREATE OR REPLACE TEMP VIEW station_groups AS
        SELECT s.indicativo, pn.prov_id AS grupo
        FROM stations s
        LEFT JOIN provincia_norm pn ON pn.provincia = s.provincia
    """)


def _records_query(group_by_month: bool) -> str:
    """SQL agregando récords batidos por (año[, mes], provincia) y por (año[, mes]) total.

    Usa un CTE para nombrar las extracciones de fecha y luego GROUPING SETS
    para obtener en la misma pasada las filas por provincia y la fila "total".
    `es_total` (GROUPING) distingue la fila de rollup de una provincia sin
    mapear (ambas llevan grupo NULL); sin él, una provincia desconocida se
    confundiría con el total.
    """
    month_select = ", base.mes" if group_by_month else ""
    month_group = ", base.mes" if group_by_month else ""
    return f"""
        WITH base AS (
            SELECT
                r.indicativo,
                r.tipo,
                EXTRACT(year FROM r.fecha)::INTEGER  AS anio,
                EXTRACT(month FROM r.fecha)::INTEGER AS mes
            FROM record_events r
            WHERE r.valor_anterior IS NOT NULL
              AND NOT COALESCE(r.provisional, FALSE)
              AND EXTRACT(year FROM r.fecha) >= {MIN_YEAR}
        )
        SELECT
            base.anio{month_select},
            sg.grupo,
            GROUPING(sg.grupo) AS es_total,
            COUNT(*) FILTER (WHERE base.tipo='absoluto-max') AS abs_max,
            COUNT(*) FILTER (WHERE base.tipo='absoluto-min') AS abs_min,
            COUNT(*) FILTER (WHERE base.tipo='mensual-max') AS mes_max,
            COUNT(*) FILTER (WHERE base.tipo='mensual-min') AS mes_min,
            COUNT(DISTINCT base.indicativo) FILTER (
                WHERE base.tipo IN ('absoluto-max','mensual-max')
            ) AS estaciones_batieron_max,
            COUNT(DISTINCT base.indicativo) FILTER (
                WHERE base.tipo IN ('absoluto-min','mensual-min')
            ) AS estaciones_batieron_min
        FROM base
        JOIN station_groups sg USING (indicativo)
        GROUP BY GROUPING SETS (
            (base.anio{month_group}, sg.grupo),
            (base.anio{month_group})
        )
    """


def _observations_query(group_by_month: bool) -> str:
    """Estaciones con datos de temperatura por (año[, mes], grupo)."""
    month_select = ", base.mes" if group_by_month else ""
    month_group = ", base.mes" if group_by_month else ""
    return f"""
        WITH base AS (
            SELECT
                o.indicativo,
                EXTRACT(year FROM o.fecha)::INTEGER  AS anio,
                EXTRACT(month FROM o.fecha)::INTEGER AS mes
            FROM observations o
            WHERE (o.tmin IS NOT NULL OR o.tmax IS NOT NULL)
              AND NOT COALESCE(o.provisional, FALSE)
              AND EXTRACT(year FROM o.fecha) >= {MIN_YEAR}
        )
        SELECT
            base.anio{month_select},
            sg.grupo,
            GROUPING(sg.grupo) AS es_total,
            COUNT(DISTINCT base.indicativo) AS n
        FROM base
        JOIN station_groups sg USING (indicativo)
        GROUP BY GROUPING SETS (
            (base.anio{month_group}, sg.grupo),
            (base.anio{month_group})
        )
    """


# Formato tupla compacto. Cada fila lleva ROW_FIELDS en este orden. Los ejes
# (anios; anios+meses) viajan por separado para no duplicar claves en cada
# uno de los ~30k registros.
ROW_FIELDS = (
    "absolutoMax", "absolutoMin",
    "mensualMax", "mensualMin",
    "estacionesConDatos",
    "estacionesBatieronMax",
    "estacionesBatieronMin",
)
# Posiciones para las inserciones desde SQL.
_IDX = {name: i for i, name in enumerate(ROW_FIELDS)}
_EMPTY_ROW = [0] * len(ROW_FIELDS)


def _collect_cells(
    con: duckdb.DuckDBPyConnection, group_ids: list[str], group_by_month: bool
) -> dict[tuple, list[int]]:
    """Celdas {(grupo, anio[, mes]): tupla-fila} combinando récords y cobertura.

    Las filas SQL empiezan por el periodo (anio o anio+mes) seguido de
    (grupo, es_total). Con `es_total` la fila es el rollup nacional ("total");
    con grupo NULL sin ser total, la provincia no está en el lookup y se ignora.
    """
    nk = 2 if group_by_month else 1  # columnas de periodo
    cells: dict[tuple, list[int]] = {}

    def _key(row: tuple) -> tuple | None:
        grupo = "total" if row[nk + 1] else row[nk]
        if grupo is None or grupo not in group_ids:
            return None
        return (grupo, *(int(v) for v in row[:nk]))

    for row in con.execute(_records_query(group_by_month)).fetchall():
        key = _key(row)
        if key is None:
            continue
        cell = cells.setdefault(key, list(_EMPTY_ROW))
        abs_max, abs_min, mes_max, mes_min, n_max, n_min = row[nk + 2:]
        cell[_IDX["absolutoMax"]] = int(abs_max)
        cell[_IDX["absolutoMin"]] = int(abs_min)
        cell[_IDX["mensualMax"]]  = int(mes_max)
        cell[_IDX["mensualMin"]]  = int(mes_min)
        cell[_IDX["estacionesBatieronMax"]] = int(n_max)
        cell[_IDX["estacionesBatieronMin"]] = int(n_min)

    for row in con.execute(_observations_query(group_by_month)).fetchall():
        key = _key(row)
        if key is None:
            continue
        cell = cells.setdefault(key, list(_EMPTY_ROW))
        cell[_IDX["estacionesConDatos"]] = int(row[nk + 2])

    return cells


def _build_anual(con: duckdb.DuckDBPyConnection, group_ids: list[str]) -> tuple[list[int], dict[str, list[list[int]]]]:
    cells = _collect_cells(con, group_ids, group_by_month=False)
    anios = sorted({anio for _, anio in cells})
    out = {g: [cells.get((g, y), list(_EMPTY_ROW)) for y in anios] for g in group_ids}
    return anios, out


def _build_mensual(con: duckdb.DuckDBPyConnection, group_ids: list[str], anios: list[int]) -> tuple[list[list[int]], dict[str, list[list[int]]]]:
    cells = _collect_cells(con, group_ids, group_by_month=True)
    eje: list[list[int]] = [[y, m] for y in anios for m in range(1, 13)]
    out = {g: [cells.get((g, y, m), list(_EMPTY_ROW)) for y, m in eje] for g in group_ids}
    return eje, out


def _group_meta(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Información estática de cada grupo: id, nombre, nº de estaciones del
    histórico (cualquier estación con al menos un día de datos)."""
    rows = con.execute("""
        WITH activas AS (
            SELECT DISTINCT o.indicativo
            FROM observations o
            WHERE (o.tmin IS NOT NULL OR o.tmax IS NOT NULL)
              AND NOT COALESCE(o.provisional, FALSE)
        )
        SELECT sg.grupo, GROUPING(sg.grupo) AS es_total, COUNT(*) AS n
        FROM station_groups sg
        JOIN activas USING (indicativo)
        GROUP BY GROUPING SETS ((sg.grupo), ())
    """).fetchall()

    counts: dict[str, int] = {}
    for grupo, es_total, n in rows:
        key = "total" if es_total else grupo
        if key is not None:  # grupo NULL sin ser total = provincia sin mapear
            counts[key] = int(n)

    # Reverse del lookup: grupo_id → lista de nombres AEMET. Necesario para
    # que la UI sepa qué provincias de stations.json corresponden a un grupo
    # (en los dos casos con alias colapsados, hay más de una).
    aemet_by_group: dict[str, list[str]] = {}
    for aemet_name, prov_id in PROVINCIA_NORM.items():
        aemet_by_group.setdefault(prov_id, []).append(aemet_name)

    # Construimos el array ordenado: total primero, después provincias por
    # nombre presentable (insensible a tildes).
    out: list[dict[str, Any]] = [{
        "id": "total",
        "nombre": "Todas las estaciones",
        "nEstaciones": counts.get("total", 0),
        "provinciasAemet": [],
    }]
    for prov_id, nombre in sorted(PROVINCIA_NAMES.items(), key=lambda kv: _sort_key(kv[1])):
        out.append({
            "id": prov_id,
            "nombre": nombre,
            "nEstaciones": counts.get(prov_id, 0),
            "provinciasAemet": sorted(aemet_by_group.get(prov_id, [])),
        })
    return out


def build(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    _create_lookup(con)
    grupos = _group_meta(con)
    group_ids = [g["id"] for g in grupos]
    anios, anual = _build_anual(con, group_ids)
    eje_mensual, mensual = _build_mensual(con, group_ids, anios)

    return {
        # Momento real de generación, con hora y offset (p.ej.
        # "2026-06-18T09:02:29+02:00"). Es el "actualizado a las…" de la app,
        # distinto de la fecha del último dato definitivo (datosHasta).
        "generadoEn": datetime.now().astimezone().isoformat(timespec="seconds"),
        "anioMin": anios[0] if anios else None,
        "anioMax": anios[-1] if anios else None,
        # Esquema de cada tupla en `anual.*[i]` y `mensual.*[i]`.
        # Mantenerlo aquí permite que la app construya objetos sin hardcodear el orden.
        "rowFields": list(ROW_FIELDS),
        "anios": anios,
        "ejeMensual": eje_mensual,  # array paralelo [[anio, mes], ...]
        "grupos": grupos,
        "anual": anual,
        "mensual": mensual,
    }


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    argparse.ArgumentParser(description="Genera stats.json").parse_args(argv)

    con = connect()
    payload = build(con)

    out_path: Path = OUTPUTS_DIR / "stats.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    n_grupos = len(payload["grupos"])
    n_anios = len(payload["anios"])
    log.info("stats.json escrito (%d grupos, %d años)", n_grupos, n_anios)


if __name__ == "__main__":
    main(sys.argv[1:])
