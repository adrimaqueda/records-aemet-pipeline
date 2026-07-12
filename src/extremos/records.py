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
récord, simplemente desaparece sin avisar). La consume `notify` para el aviso
de Telegram. Si `record_events` no existía (primera pasada o DB reconstruida)
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
import sys

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
# Estructura: una CTE por categoría que aplica MAX/MIN OVER excluyendo la fila
# actual y se queda solo con las filas que mejoran el récord vigente; luego
# añadimos LAG para los días transcurridos y unimos todo.
#
# `maduro` decide si un evento cuenta como récord batido. Una serie es "madura"
# cuando lleva ≥ RECORD_WARMUP_DAYS de calendario con datos desde el inicio de su
# segmento de cobertura actual; un hueco de ≥ RECORD_GAP_RESET_DAYS días abre un
# segmento nuevo (la serie se "reanuda" y vuelve a estrenarse). Los eventos no
# maduros NO se cuentan como récords batidos (su `valor_anterior` se anula en
# `unioned`), porque mientras la serie estrena su envolvente estacional casi todo
# es "récord" por el simple avance estacional, y tras un hueco el récord vigente
# puede ser un valor rancio fuera de temporada que dispara saltos ficticios. El
# `prev` se sigue calculando sobre todo el histórico, así que el récord vigente
# no se ve afectado: si el máximo se fijó antes de la madurez, sigue vigente.
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
abs_max_e AS (
    SELECT indicativo, fecha, seg_id, maduro, tmax AS valor,
           MAX(tmax) OVER (
               PARTITION BY indicativo ORDER BY fecha
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS prev
    FROM base WHERE tmax IS NOT NULL
    QUALIFY prev IS NULL OR tmax > prev
),
abs_min_e AS (
    -- TMIN más alta jamás registrada (noche más cálida).
    SELECT indicativo, fecha, seg_id, maduro, tmin AS valor,
           MAX(tmin) OVER (
               PARTITION BY indicativo ORDER BY fecha
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS prev
    FROM base WHERE tmin IS NOT NULL
    QUALIFY prev IS NULL OR tmin > prev
),
mes_max_e AS (
    SELECT indicativo, fecha, seg_id, mes, maduro, tmax AS valor,
           MAX(tmax) OVER (
               PARTITION BY indicativo, mes ORDER BY fecha
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS prev
    FROM base WHERE tmax IS NOT NULL
    QUALIFY prev IS NULL OR tmax > prev
),
mes_min_e AS (
    -- TMIN más alta de cada mes calendario.
    SELECT indicativo, fecha, seg_id, mes, maduro, tmin AS valor,
           MAX(tmin) OVER (
               PARTITION BY indicativo, mes ORDER BY fecha
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS prev
    FROM base WHERE tmin IS NOT NULL
    QUALIFY prev IS NULL OR tmin > prev
),
unioned AS (
    -- Mientras la serie no es madura anulamos valor_anterior: esos eventos no
    -- cuentan como récords batidos (el valor sí queda como vigente).
    SELECT indicativo, 'absoluto-max' AS tipo, fecha, seg_id, NULL::INTEGER AS mes, valor,
           CASE WHEN maduro THEN prev END AS valor_anterior
    FROM abs_max_e
    UNION ALL
    SELECT indicativo, 'absoluto-min', fecha, seg_id, NULL::INTEGER, valor,
           CASE WHEN maduro THEN prev END FROM abs_min_e
    UNION ALL
    SELECT indicativo, 'mensual-max', fecha, seg_id, mes, valor,
           CASE WHEN maduro THEN prev END FROM mes_max_e
    UNION ALL
    SELECT indicativo, 'mensual-min', fecha, seg_id, mes, valor,
           CASE WHEN maduro THEN prev END FROM mes_min_e
),
collapsed AS (
    -- Los récords batidos (maduros, valor_anterior no nulo) pasan todos: cada uno
    -- es un peldaño real de la escalera.
    SELECT indicativo, tipo, fecha, mes, valor, valor_anterior
    FROM unioned
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
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY indicativo, tipo, COALESCE(mes, 0), seg_id
                   ORDER BY valor DESC, fecha DESC
               ) AS rn
        FROM unioned
        WHERE valor_anterior IS NULL
    )
    WHERE rn = 1
)
SELECT
    c.indicativo,
    c.tipo,
    c.fecha,
    c.mes,
    c.valor,
    c.valor_anterior,
    COALESCE(o.provisional, FALSE) AS provisional,
    (c.fecha - LAG(c.fecha) OVER (
        PARTITION BY c.indicativo, c.tipo, c.mes ORDER BY c.fecha
    ))::INTEGER AS dias_desde_anterior
FROM collapsed c
LEFT JOIN observations o
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
WHERE (tmax IS NOT NULL OR tmin IS NOT NULL) AND NOT COALESCE(provisional, FALSE)
GROUP BY indicativo;
"""


def compute(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
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
    n_events = con.execute("SELECT COUNT(*) FROM record_events").fetchone()[0]

    if prev_exists:
        con.execute(NEW_EVENTS_SQL)
    else:
        con.execute("DROP TABLE IF EXISTS record_events_new")
        con.execute("CREATE TABLE record_events_new AS SELECT * FROM record_events WHERE FALSE")
    n_new = con.execute("SELECT COUNT(*) FROM record_events_new").fetchone()[0]

    log.info("Calculando station_coverage…")
    con.execute(COVERAGE_SQL)
    n_cov = con.execute("SELECT COUNT(*) FROM station_coverage").fetchone()[0]
    n_active = con.execute("SELECT COUNT(*) FROM station_coverage WHERE activa").fetchone()[0]

    return {
        "events": n_events,
        "new_events": n_new,
        "stations_with_data": n_cov,
        "active_stations": n_active,
    }


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    argparse.ArgumentParser(description="Recalcula tablas de récords").parse_args(argv)

    con = connect()
    stats = compute(con)
    log.info(
        "Récords listos: %d eventos (%d nuevos en esta pasada) · %d estaciones con datos · %d activas",
        stats["events"], stats["new_events"],
        stats["stations_with_data"], stats["active_stations"],
    )


if __name__ == "__main__":
    main(sys.argv[1:])
