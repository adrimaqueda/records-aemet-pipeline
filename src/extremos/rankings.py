"""Rankings ("leaderboards") para la página /datos de la app.

Salida: pipeline/outputs/rankings.json

Calcula varias clasificaciones sobre `record_events` que la app no podría
derivar de `stations.json` (que solo lleva el absoluto vigente y el último
mensual por estación). Aquí tenemos el evento completo, así que podemos rankear
por mes, por margen de salto, por antigüedad, etc.

Convenciones:
  - Solo estaciones ACTIVAS (`station_coverage.activa`), igual que el resto de
    la app: una estación que dejó de reportar no debe encabezar un ranking con
    un récord que ya nadie vigila.
  - TODAS las tablas (topAbs, topMes, recientes, longevos, mayorSalto,
    masActivas) usan SOLO récords definitivos (`NOT provisional`), como
    `stats.py`: un provisional sin confirmar no debe colarse como récord.
  - En concreto "recientes" ya NO incluye provisionales: muchos no se acaban
    confirmando (AEMET no publica el definitivo o lo corrige a la baja), así que
    mostrarlos como "lo último batido" llenaba el panel de récords que luego no
    cuajan. Mejor enseñar solo lo confirmado aunque la foto vaya unos días por
    detrás.
  - Semántica de valores (siempre "el más alto"):
      absoluto-max → TMAX más alta jamás (día más caluroso).
      absoluto-min → TMIN más alta jamás (noche más cálida).
      mensual-*    → ídem segmentado por mes calendario.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

from extremos.config import OUTPUTS_DIR
from extremos.db import connect
from extremos.logconf import setup_logging

log = logging.getLogger("extremos.rankings")

# Cuántas filas guardamos por tabla. Generosos: la app pagina/recorta.
N_TOP = 10      # top absoluto y top por mes (por familia)
N_REC = 25      # récords más recientes
N_LONG = 20     # récords vigentes más longevos (por familia)
N_SALTO = 15    # mayores saltos sobre el récord anterior
N_ACT = 12      # estaciones que más récords baten


def _iso(d: date | None) -> str | None:
    return d.isoformat() if d else None


def _num(v: Any) -> float | None:
    """Normaliza un valor de temperatura a float (o None)."""
    return None if v is None else round(float(v), 1)


def _abs_vigente_fechas(con: duckdb.DuckDBPyConnection) -> dict[tuple[str, str], date]:
    """`(indicativo, 'max'|'min') -> fecha` del récord absoluto vigente (definitivo).

    Sirve para marcar `abs: true` en los récords mensuales que coinciden con el
    absoluto de su estación (el mes en que se fijó el máximo/mínimo histórico). El
    absoluto vigente cae en un día concreto y el récord mensual vigente de ese mes
    es ese mismo día, así que el match es por fecha. Solo definitivos, como el
    resto de rankings.
    """
    rows = con.execute(
        """
        WITH v AS (
            SELECT indicativo, tipo, fecha,
                ROW_NUMBER() OVER (PARTITION BY indicativo, tipo ORDER BY fecha DESC) AS rn
            FROM record_events
            WHERE tipo IN ('absoluto-max', 'absoluto-min')
              AND NOT COALESCE(provisional, FALSE)
        )
        SELECT indicativo, tipo, fecha FROM v WHERE rn = 1
        """
    ).fetchall()
    out: dict[tuple[str, str], date] = {}
    for ind, tipo, f in rows:
        out[(ind, "max" if tipo == "absoluto-max" else "min")] = f
    return out


def _top_abs(con: duckdb.DuckDBPyConnection, tipo: str) -> list[dict[str, Any]]:
    """Top-N estaciones por récord absoluto vigente más alto (definitivo)."""
    rows = con.execute(
        """
        WITH ev AS (
            SELECT re.indicativo, re.valor, re.fecha,
                ROW_NUMBER() OVER (
                    PARTITION BY re.indicativo ORDER BY re.valor DESC, re.fecha
                ) AS rn
            FROM record_events re
            JOIN station_coverage c USING (indicativo)
            WHERE re.tipo = ? AND c.activa AND NOT COALESCE(re.provisional, FALSE)
        )
        SELECT s.indicativo, s.nombre, s.provincia, s.altitud, ev.valor, ev.fecha
        FROM ev JOIN stations s USING (indicativo)
        WHERE ev.rn = 1
        ORDER BY ev.valor DESC, ev.fecha
        LIMIT ?
        """,
        [tipo, N_TOP],
    ).fetchall()
    return [
        {"ind": ind, "nombre": nom, "prov": prov, "alt": alt,
         "valor": _num(val), "fecha": _iso(f)}
        for (ind, nom, prov, alt, val, f) in rows
    ]


def _top_mes(con: duckdb.DuckDBPyConnection, tipo: str,
             abs_fechas: dict[tuple[str, str], date]) -> dict[str, list[dict[str, Any]]]:
    """Top-N por mes calendario (1..12) del récord mensual vigente más alto.

    Marca `abs: true` la entrada cuyo récord mensual coincide con el absoluto
    vigente de su estación (clave omitida si es false).
    """
    fam = "max" if tipo == "mensual-max" else "min"
    rows = con.execute(
        """
        WITH ev AS (
            SELECT re.indicativo, re.mes, re.valor, re.fecha,
                ROW_NUMBER() OVER (
                    PARTITION BY re.indicativo, re.mes ORDER BY re.valor DESC, re.fecha
                ) AS rn
            FROM record_events re
            JOIN station_coverage c USING (indicativo)
            WHERE re.tipo = ? AND c.activa AND NOT COALESCE(re.provisional, FALSE)
        ),
        best AS (SELECT * FROM ev WHERE rn = 1),
        ranked AS (
            SELECT b.*, ROW_NUMBER() OVER (
                PARTITION BY b.mes ORDER BY b.valor DESC, b.fecha
            ) AS pos
            FROM best b
        )
        SELECT r.mes, s.indicativo, s.nombre, s.provincia, s.altitud, r.valor, r.fecha
        FROM ranked r JOIN stations s USING (indicativo)
        WHERE r.pos <= ?
        ORDER BY r.mes, r.pos
        """,
        [tipo, N_TOP],
    ).fetchall()
    out: dict[str, list[dict[str, Any]]] = {str(m): [] for m in range(1, 13)}
    for (mes, ind, nom, prov, alt, val, f) in rows:
        entry: dict[str, Any] = {
            "ind": ind, "nombre": nom, "prov": prov, "alt": alt,
            "valor": _num(val), "fecha": _iso(f),
        }
        if abs_fechas.get((ind, fam)) == f:
            entry["abs"] = True
        out[str(int(mes))].append(entry)
    return out


def _recientes(con: duckdb.DuckDBPyConnection,
               abs_fechas: dict[tuple[str, str], date]) -> list[dict[str, Any]]:
    """Récords realmente batidos más recientes en toda la red (solo definitivos).

    Excluimos provisionales: muchos no se confirman (AEMET no publica el
    definitivo o lo corrige), así que no deben aparecer como "lo último batido".
    El campo `provisional` se mantiene en la salida por compatibilidad, pero ahora
    será siempre `false`. Las entradas mensuales que coinciden con el absoluto
    vigente de su estación llevan `abs: true` (clave omitida si es false).
    """
    rows = con.execute(
        """
        SELECT s.indicativo, s.nombre, s.provincia,
               re.tipo, re.mes, re.valor, re.valor_anterior, re.fecha, re.provisional
        FROM record_events re
        JOIN station_coverage c USING (indicativo)
        JOIN stations s USING (indicativo)
        WHERE re.valor_anterior IS NOT NULL AND c.activa
          AND NOT COALESCE(re.provisional, FALSE)
        ORDER BY re.fecha DESC, (re.valor - re.valor_anterior) DESC
        LIMIT ?
        """,
        [N_REC],
    ).fetchall()
    out: list[dict[str, Any]] = []
    for (ind, nom, prov, tipo, mes, val, va, f, provisional) in rows:
        entry: dict[str, Any] = {
            "ind": ind, "nombre": nom, "prov": prov, "tipo": tipo,
            "mes": int(mes) if mes is not None else None,
            "valor": _num(val), "valorAnterior": _num(va), "fecha": _iso(f),
            "provisional": bool(provisional),
        }
        if tipo in ("mensual-max", "mensual-min"):
            fam = "max" if tipo == "mensual-max" else "min"
            if abs_fechas.get((ind, fam)) == f:
                entry["abs"] = True
        out.append(entry)
    return out


def _longevos(con: duckdb.DuckDBPyConnection, tipo: str) -> list[dict[str, Any]]:
    """Récords absolutos vigentes que llevan más tiempo sin superarse (definitivos)."""
    rows = con.execute(
        """
        WITH vig AS (
            SELECT re.indicativo, re.valor, re.fecha,
                ROW_NUMBER() OVER (
                    PARTITION BY re.indicativo ORDER BY re.fecha DESC
                ) AS rn
            FROM record_events re
            JOIN station_coverage c USING (indicativo)
            WHERE re.tipo = ? AND c.activa AND NOT COALESCE(re.provisional, FALSE)
        )
        SELECT s.indicativo, s.nombre, s.provincia, s.altitud, vig.valor, vig.fecha
        FROM vig JOIN stations s USING (indicativo)
        WHERE vig.rn = 1
        ORDER BY vig.fecha ASC
        LIMIT ?
        """,
        [tipo, N_LONG],
    ).fetchall()
    return [
        {"ind": ind, "nombre": nom, "prov": prov, "alt": alt,
         "valor": _num(val), "fecha": _iso(f)}
        for (ind, nom, prov, alt, val, f) in rows
    ]


def _mayor_salto(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Récords absolutos que superaron al anterior por mayor margen (definitivos)."""
    rows = con.execute(
        """
        SELECT s.indicativo, s.nombre, s.provincia,
               re.tipo, re.valor, re.valor_anterior, re.fecha
        FROM record_events re
        JOIN station_coverage c USING (indicativo)
        JOIN stations s USING (indicativo)
        WHERE re.valor_anterior IS NOT NULL AND c.activa
          AND NOT COALESCE(re.provisional, FALSE)
          AND re.tipo IN ('absoluto-max', 'absoluto-min')
        ORDER BY (re.valor - re.valor_anterior) DESC, re.fecha DESC
        LIMIT ?
        """,
        [N_SALTO],
    ).fetchall()
    return [
        {"ind": ind, "nombre": nom, "prov": prov, "tipo": tipo,
         "valor": _num(val), "valorAnterior": _num(va),
         "salto": _num(val - va), "fecha": _iso(f)}
        for (ind, nom, prov, tipo, val, va, f) in rows
    ]


def _mas_activas(con: duckdb.DuckDBPyConnection, where: str, params: list[Any]) -> list[dict[str, Any]]:
    rows = con.execute(
        f"""
        SELECT s.indicativo, s.nombre, s.provincia, COUNT(*) AS n
        FROM record_events re
        JOIN station_coverage c USING (indicativo)
        JOIN stations s USING (indicativo)
        WHERE re.valor_anterior IS NOT NULL AND c.activa
          AND NOT COALESCE(re.provisional, FALSE)
          AND {where}
        GROUP BY 1, 2, 3
        ORDER BY n DESC, s.nombre
        LIMIT ?
        """,
        [*params, N_ACT],
    ).fetchall()
    return [
        {"ind": ind, "nombre": nom, "prov": prov, "n": int(n)}
        for (ind, nom, prov, n) in rows
    ]


def build(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    anio_max = con.execute("SELECT EXTRACT(year FROM MAX(fecha))::INTEGER FROM record_events").fetchone()[0]
    # Fecha del absoluto vigente por estación, para marcar `abs` en los mensuales.
    abs_fechas = _abs_vigente_fechas(con)

    return {
        "generadoEn": datetime.now().astimezone().isoformat(timespec="seconds"),
        # Top absoluto por familia.
        "topAbs": {
            "max": _top_abs(con, "absoluto-max"),
            "min": _top_abs(con, "absoluto-min"),
        },
        # Top por mes calendario, por familia: { "max": {"1":[...]}, "min": {...} }.
        "topMes": {
            "max": _top_mes(con, "mensual-max", abs_fechas),
            "min": _top_mes(con, "mensual-min", abs_fechas),
        },
        "recientes": _recientes(con, abs_fechas),
        "longevos": {
            "max": _longevos(con, "absoluto-max"),
            "min": _longevos(con, "absoluto-min"),
        },
        "mayorSalto": _mayor_salto(con),
        "masActivas": {
            "anio": int(anio_max) if anio_max is not None else None,
            "esteAnio": _mas_activas(con, "EXTRACT(year FROM re.fecha) = ?", [anio_max]),
            "ultimos12m": _mas_activas(con, "re.fecha >= CURRENT_DATE - INTERVAL '12 months'", []),
        },
    }


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    argparse.ArgumentParser(description="Genera rankings.json").parse_args(argv)

    con = connect()
    payload = build(con)

    out_path: Path = OUTPUTS_DIR / "rankings.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    log.info("rankings.json escrito")


if __name__ == "__main__":
    main(sys.argv[1:])
