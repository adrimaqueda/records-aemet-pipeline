"""Cómputo de récords absolutos y mensuales por estación.

Genera dos tablas (drop+create cada vez, es barato):

  record_events(indicativo, tipo, fecha, mes, valor, valor_anterior, dias_desde_anterior)
    - tipo ∈ {'absoluto-max','absoluto-min','mensual-max','mensual-min'}
    - mes: 1..12 para los tipos mensuales; NULL para los absolutos.
    - valor_anterior: el récord vigente justo antes. Es NULL (= "no cuenta como
      récord batido") en dos casos: (a) el primer evento de la serie, y (b)
      cualquier evento mientras la serie aún NO es "madura". Una serie es madura
      cuando lleva ≥ RECORD_WARMUP_DAYS con datos desde que empezó (o desde que
      se reanudó tras un hueco de ≥ RECORD_GAP_RESET_DAYS días). Durante ese
      primer año casi todo es "récord" porque la serie está estableciendo su
      envolvente estacional (cada mes que avanza hace más calor que el anterior),
      así que esos eventos no se contabilizan como récords batidos. Importante:
      el valor sí sigue contando para el récord vigente — si el máximo histórico
      se fijó antes de la madurez y nunca se superó, sigue siendo el récord en
      curso. (Generaliza la antigua regla del "primer año natural", que solo
      cubría el primer año de calendario; ver config.RECORD_WARMUP_DAYS.)
      Los eventos no maduros (inicial) de un MISMO segmento se colapsan en uno
      solo —el de mayor valor, el máximo del warm-up—, así que el récord inicial
      es ese máximo y no toda la escalera que sube por el avance estacional (ver
      CTE `collapsed`). Cada segmento (serie inicial o reanudación tras un hueco)
      deja como mucho un evento inicial por categoría/mes.
    - dias_desde_anterior: días entre este evento y el anterior de la misma categoría.

  station_coverage(indicativo, datos_desde, datos_hasta, dias_con_datos, dias_ultimo_anio, activa)
    - activa: estación con ≥ ACTIVE_STATION_MIN_DAYS días reportados en los últimos 12 meses.

Además genera record_events_new: los récords BATIDOS (valor_anterior no nulo)
que no existían en la pasada anterior. `provisional` y `valor` forman parte de
la clave del diff, así que cuando AEMET confirma un dato provisional el evento
reaparece como récord definitivo nuevo (y si el dato confirmado ya no bate el
récord, simplemente desaparece sin avisar). Sirve para avisar de los récords
de cada pasada. Si `record_events` no existía (primera pasada o DB reconstruida)
se deja vacía para no avisar de todo el histórico de golpe.

Semántica de los récords (siempre buscamos "el más alto"):
  - absoluto-max  → TMAX más alta jamás registrada en la estación (día más caluroso).
  - absoluto-min  → TMIN más alta jamás registrada en la estación (noche más cálida).
  - mensual-max   → ídem TMAX, segmentado por mes calendario.
  - mensual-min   → ídem TMIN, segmentado por mes calendario.
Empates no cuentan (estrictamente superior).
"""
from __future__ import annotations

import argparse
import logging

import duckdb

from extremos.config import (
    ACTIVE_STATION_MIN_DAYS,
    RECORD_GAP_RESET_DAYS,
    RECORD_WARMUP_DAYS,
)
from extremos.db import connect
from extremos.logconf import setup_logging

log = logging.getLogger("extremos.records")

# ---------------------------------------------------------------------------
# Cálculo de eventos de récord.
# `serie` despliega cada observación en sus 4 categorías (tipo, mes, valor); una
# sola ventana MAX OVER (excluyendo la fila actual) por categoría da el récord
# vigente previo, y nos quedamos con las filas que lo mejoran.
#
# `maduro` decide si un evento cuenta como récord batido. Una serie es "madura"
# cuando lleva ≥ RECORD_WARMUP_DAYS de calendario con datos desde el inicio de su
# segmento de cobertura actual; un hueco de ≥ RECORD_GAP_RESET_DAYS días abre un
# segmento nuevo (la serie se "reanuda" y vuelve a estrenarse). A los eventos no
# maduros se les anula `valor_anterior`, porque mientras la serie estrena su
# envolvente estacional casi todo es "récord" por el simple avance estacional, y
# tras un hueco el récord vigente puede ser un valor rancio fuera de temporada
# que dispara saltos ficticios. El `prev` se sigue calculando sobre todo el
# histórico, así que el récord vigente no se ve afectado.
# ---------------------------------------------------------------------------

RECORD_EVENTS_SQL = f"""
DROP TABLE IF EXISTS record_events;
CREATE TABLE record_events AS
WITH dias AS (
    -- Un día con cualquier dato de temperatura. Marcamos el inicio de segmento
    -- cuando hay un hueco ≥ RECORD_GAP_RESET_DAYS respecto al día anterior.
    SELECT
        indicativo,
        fecha,
        CASE
            WHEN (fecha - LAG(fecha) OVER (PARTITION BY indicativo ORDER BY fecha))
                 >= {RECORD_GAP_RESET_DAYS}
            THEN 1 ELSE 0
        END AS abre_segmento
    FROM observations
    WHERE tmax IS NOT NULL OR tmin IS NOT NULL
),
segmentos AS (
    SELECT
        indicativo,
        fecha,
        -- id de segmento creciente: +1 cada vez que se abre uno nuevo.
        SUM(abre_segmento) OVER (
            PARTITION BY indicativo ORDER BY fecha ROWS UNBOUNDED PRECEDING
        ) AS seg_id
    FROM dias
),
base AS (
    SELECT
        o.indicativo,
        o.fecha,
        sg.seg_id,
        EXTRACT(MONTH FROM o.fecha)::INTEGER AS mes,
        o.tmax,
        o.tmin,
        -- Madura cuando han pasado ≥ RECORD_WARMUP_DAYS desde el primer día del
        -- segmento de cobertura actual (inicio de la serie o reanudación).
        o.fecha >= MIN(o.fecha) OVER (PARTITION BY o.indicativo, sg.seg_id)
                   + INTERVAL '{RECORD_WARMUP_DAYS}' DAY AS maduro
    FROM observations o
    JOIN segmentos sg USING (indicativo, fecha)
),
serie AS (
    SELECT indicativo, fecha, seg_id, maduro, 'absoluto-max' AS tipo,
           NULL::INTEGER AS mes, tmax AS valor FROM base
    UNION ALL
    SELECT indicativo, fecha, seg_id, maduro, 'absoluto-min', NULL, tmin FROM base
    UNION ALL
    SELECT indicativo, fecha, seg_id, maduro, 'mensual-max', mes, tmax FROM base
    UNION ALL
    SELECT indicativo, fecha, seg_id, maduro, 'mensual-min', mes, tmin FROM base
),
eventos AS (
    SELECT indicativo, tipo, fecha, seg_id, mes, valor,
           -- Mientras la serie no es madura anulamos valor_anterior: esos
           -- eventos no cuentan como récords batidos (el valor sí queda vigente).
           CASE WHEN maduro THEN prev END AS valor_anterior
    FROM (
        SELECT *,
               MAX(valor) OVER (
                   PARTITION BY indicativo, tipo, mes ORDER BY fecha
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
               ) AS prev
        FROM serie
        WHERE valor IS NOT NULL
    )
    WHERE prev IS NULL OR valor > prev
),
collapsed AS (
    -- Los récords batidos (maduros, valor_anterior no nulo) pasan todos: cada uno
    -- es un peldaño real de la escalera.
    SELECT indicativo, tipo, fecha, mes, valor, valor_anterior
    FROM eventos
    WHERE valor_anterior IS NOT NULL
    UNION ALL
    -- Los eventos `inicial` (warm-up) de un mismo segmento se colapsan en UNO
    -- solo: el de mayor valor, que es el máximo establecido durante ese warm-up
    -- (el primer año de la serie, o la reanudación tras una interrupción). Así el
    -- récord inicial es ese máximo y no toda la escalera de "récords" que sube sin
    -- más por el avance estacional. El máximo es el último peldaño (la envolvente
    -- crece de forma monótona), así que sigue siendo el evento más reciente del
    -- segmento → `vigentes`/`mensuales` (que toman el último por fecha) no cambian.
    SELECT indicativo, tipo, fecha, mes, valor, valor_anterior
    FROM eventos
    WHERE valor_anterior IS NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY indicativo, tipo, mes, seg_id
        ORDER BY valor DESC, fecha DESC
    ) = 1
)
SELECT
    c.indicativo,
    c.tipo,
    c.fecha,
    c.mes,
    c.valor,
    c.valor_anterior,
    o.provisional,
    (c.fecha - LAG(c.fecha) OVER (
        PARTITION BY c.indicativo, c.tipo, c.mes ORDER BY c.fecha
    ))::INTEGER AS dias_desde_anterior
FROM collapsed c
JOIN observations o
    ON o.indicativo = c.indicativo AND o.fecha = c.fecha
ORDER BY c.indicativo, c.tipo, COALESCE(c.mes, 0), c.fecha;
"""

NEW_EVENTS_SQL = """
DROP TABLE IF EXISTS record_events_new;
CREATE TABLE record_events_new AS
SELECT e.*
FROM record_events e
WHERE e.valor_anterior IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM _prev_events p
      WHERE p.indicativo = e.indicativo
        AND p.tipo = e.tipo
        AND p.mes IS NOT DISTINCT FROM e.mes
        AND p.fecha = e.fecha
        AND p.valor = e.valor
        AND p.provisional = e.provisional
  );
"""

COVERAGE_SQL = f"""
DROP TABLE IF EXISTS station_coverage;
CREATE TABLE station_coverage AS
SELECT
    indicativo,
    MIN(fecha) AS datos_desde,
    MAX(fecha) AS datos_hasta,
    COUNT(*)   AS dias_con_datos,
    SUM(CASE WHEN fecha >= CURRENT_DATE - INTERVAL '12 months' THEN 1 ELSE 0 END)
        AS dias_ultimo_anio,
    SUM(CASE WHEN fecha >= CURRENT_DATE - INTERVAL '12 months' THEN 1 ELSE 0 END)
        >= {ACTIVE_STATION_MIN_DAYS} AS activa
FROM observations
-- La cobertura ("datos hasta", días con datos, estación activa) se mide sólo
-- sobre el dato definitivo; los provisionales no cuentan como "actualizado".
WHERE (tmax IS NOT NULL OR tmin IS NOT NULL) AND NOT provisional
GROUP BY indicativo;
"""


def compute(con: duckdb.DuckDBPyConnection) -> None:
    prev_exists = con.execute(
        "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'record_events'"
    ).fetchone()[0] > 0
    if prev_exists:
        con.execute("""
            CREATE OR REPLACE TEMP TABLE _prev_events AS
            SELECT indicativo, tipo, mes, fecha, valor, provisional
            FROM record_events
            WHERE valor_anterior IS NOT NULL
        """)

    log.info("Calculando record_events…")
    con.execute(RECORD_EVENTS_SQL)
    if prev_exists:
        con.execute(NEW_EVENTS_SQL)
    else:
        con.execute("CREATE OR REPLACE TABLE record_events_new AS "
                    "SELECT * FROM record_events WHERE FALSE")

    log.info("Calculando station_coverage…")
    con.execute(COVERAGE_SQL)

    n_events, n_new, n_cov, n_active = con.execute("""
        SELECT (SELECT COUNT(*) FROM record_events),
               (SELECT COUNT(*) FROM record_events_new),
               COUNT(*), COUNT(*) FILTER (WHERE activa)
        FROM station_coverage
    """).fetchone()
    log.info(
        "Récords listos: %d eventos (%d nuevos en esta pasada) · %d estaciones con datos · %d activas",
        n_events, n_new, n_cov, n_active,
    )


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    argparse.ArgumentParser(description="Recalcula tablas de récords").parse_args(argv)
    compute(connect())


if __name__ == "__main__":
    main()
