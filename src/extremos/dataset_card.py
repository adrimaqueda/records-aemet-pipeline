"""Genera el README.md del dataset HF (la "tarjeta") con cifras vivas.

Se escribe en `outputs/README.md` durante `export`, de modo que
`extremos-publish` (que sube toda la carpeta `outputs/`) lo publica como tarjeta
del dataset en cada pasada y nunca queda desfasada. Los huecos `@@...@@` se
rellenan con datos reales de la DuckDB; el resto (esquemas, ejemplos, semántica)
es estático.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb

from extremos.provincias import PROVINCIA_NAMES

_TEMPLATE = """---
language:
  - es
license: other
license_name: aemet-reutilizacion
license_link: https://www.aemet.es/es/nota_legal
pretty_name: Récords de temperatura por estación (AEMET)
tags:
  - climate
  - weather
  - temperature
  - records
  - spain
  - aemet
  - heat
size_categories:
  - 1K<n<10K
---

# Récords de temperatura por estación (AEMET)

Datos usados en https://records-temperatura.adrimaqueda.com/

JSONs ligeros con los **récords de temperatura** de las estaciones de AEMET,
derivados de las observaciones climatológicas diarias. Es la capa de datos que
consume directamente una app web (mapa + fichas de estación + agregados por
provincia); **no** es un dump crudo de observaciones.

> Los datos proceden de [AEMET](https://www.aemet.es/) (climatologías diarias de
> OpenData). Este dataset es una elaboración propia: récords calculados a partir
> de esas observaciones. No es un producto oficial de AEMET.

## Resumen

| | |
|---|---|
| Estaciones activas (fichas publicadas) | **@@ACTIVAS@@** |
| Estaciones con histórico (en los agregados) | @@HISTORICO@@ |
| Cobertura temporal | **@@ANIO_MIN@@ – @@ANIO_MAX@@** (definitivo hasta @@HASTA@@) |
| Récords batidos contabilizados | **@@BATIDOS@@** |
| Ámbitos de los agregados | 1 nacional + **@@PROVINCIAS@@ provincias** |
| Ficheros | @@NFICHEROS@@ JSON (~22 MB) |
| Actualización | 2×/día (09:00 y 21:00, hora peninsular) |

## Ficheros

```
stations.json              # array, 1 entrada por estación activa (resumen para el mapa)
stations/{indicativo}.json # detalle por estación: récords vigentes + timeline
stats.json                 # agregados anuales y mensuales (nacional y por provincia)
rankings.json              # clasificaciones (top absolutos, longevos, mayores saltos, …)
```

Codificación UTF-8, JSON minificado (sin espacios). El `{indicativo}` es el
identificador de estación de AEMET (p. ej. `0009X`).

---

## `stations.json`

Array con una entrada por estación activa. Pensado para pintar el mapa y filtrar.

| Campo | Tipo | Descripción |
|---|---|---|
| `indicativo` | string | ID de estación AEMET |
| `nombre` | string | Nombre de la estación |
| `provincia` | string | Provincia (nomenclatura AEMET) |
| `altitud` | int | Altitud en metros |
| `lat`, `lon` | float | Coordenadas (grados decimales) |
| `datosDesde` / `datosHasta` | date | Primer y último día con dato **definitivo** |
| `diasConDatos` | int | Nº de días con observación definitiva |
| `ultimoPorTipo` | object | Último récord batido de cada uno de los 4 tipos (o `null`) |
| `recientes15d` | object | Récords batidos en los últimos 15 días, por tipo |
| `totalRecordsAbsolutos` | int | `totales.absolutoMax + totales.absolutoMin` |
| `totalRecordsMensuales` | int | `totales.mensualMax + totales.mensualMin` |
| `totales` | object | Récords batidos acumulados por tipo: `absolutoMax`, `absolutoMin`, `mensualMax`, `mensualMin` |

```json
{
  "indicativo": "0009X",
  "nombre": "ALFORJA",
  "provincia": "TARRAGONA",
  "altitud": 406,
  "lat": 41.2139, "lon": 0.9633,
  "datosDesde": "2008-06-25",
  "datosHasta": "2026-06-14",
  "diasConDatos": 6196,
  "ultimoPorTipo": {
    "absolutoMax": { "fecha": "2023-08-23", "valor": 40.2 },
    "absolutoMin": { "fecha": "2015-07-29", "valor": 22.9 },
    "mensualMax":  { "fecha": "2026-05-28", "valor": 35.0, "mes": 5 },
    "mensualMin":  { "fecha": "2023-11-13", "valor": 16.0, "mes": 11 }
  },
  "recientes15d": { "absolutoMax": 0, "absolutoMin": 0, "mensualMax": 1, "mensualMin": 0 },
  "totalRecordsAbsolutos": 15,
  "totalRecordsMensuales": 165,
  "totales": { "absolutoMax": 8, "absolutoMin": 7, "mensualMax": 98, "mensualMin": 67 }
}
```

## `stations/{indicativo}.json`

Detalle de una estación: récords vigentes y el histórico de récords batidos.

| Campo | Tipo | Descripción |
|---|---|---|
| (cabecera) | | `indicativo`, `nombre`, `provincia`, `altitud`, `lat`, `lon`, `datosDesde`, `datosHasta`, `diasConDatos`, `totales` (igual que en `stations.json`) |
| `vigentes` | object | Récord **vigente** (el más alto jamás registrado): `absolutoMax`, `absolutoMin` |
| `ultimoRecord` | object\\|null | Récord batido más reciente (cualquier tipo) |
| `mensuales` | array[12] | Por mes (`mes` 1–12): `max` y `min` vigentes con su fecha. El récord mensual que coincide con el absoluto vigente lleva `abs: true` (clave omitida si es false) |
| `eventos` | array | Timeline (desc por fecha) de **récords batidos** |

Cada entrada de `eventos`:

| Campo | Tipo | Descripción |
|---|---|---|
| `fecha` | date | Día del récord |
| `tipo` | string | `absoluto-max` · `absoluto-min` · `mensual-max` · `mensual-min` |
| `mes` | int\\|null | Mes 1–12 en los tipos mensuales; `null` en los absolutos |
| `valor` | float | Temperatura del récord (°C) |
| `valorAnterior` | float | Récord vigente justo antes |
| `diasDesdeAnterior` | int | Días transcurridos desde el récord anterior de esa categoría |
| `provisional` | bool | `true` si el dato proviene del horario en tiempo real (aún no definitivo) |

```json
{
  "indicativo": "0009X", "nombre": "ALFORJA", "provincia": "TARRAGONA",
  "altitud": 406, "lat": 41.2139, "lon": 0.9633,
  "datosDesde": "2008-06-25", "datosHasta": "2026-06-14", "diasConDatos": 6196,
  "totales": { "absolutoMax": 8, "absolutoMin": 7, "mensualMax": 98, "mensualMin": 67 },
  "vigentes": {
    "absolutoMax": { "fecha": "2023-08-23", "valor": 40.2 },
    "absolutoMin": { "fecha": "2015-07-29", "valor": 22.9 }
  },
  "ultimoRecord": { "fecha": "2026-05-28", "tipo": "mensual-max", "mes": 5, "valor": 35.0, "provisional": false },
  "mensuales": [
    { "mes": 1, "max": { "fecha": "2022-01-02", "valor": 22.7 }, "min": { "fecha": "2018-01-04", "valor": 13.4 } },
    { "mes": 8, "max": { "fecha": "2023-08-23", "valor": 40.2, "abs": true }, "min": { "fecha": "2015-08-05", "valor": 21.8 } }
    /* … 12 meses; el récord mensual que coincide con el absoluto lleva "abs": true … */
  ],
  "eventos": [
    { "fecha": "2026-05-28", "tipo": "mensual-max", "mes": 5, "valor": 35.0, "valorAnterior": 34.7, "diasDesdeAnterior": 4032, "provisional": false }
    /* … */
  ]
}
```

## `stats.json`

Agregados para la página de estadísticas. Las filas viajan en **formato tupla
compacto** (un array por celda) para no repetir claves en ~30k registros; el
orden de la tupla lo define `rowFields`.

| Campo | Tipo | Descripción |
|---|---|---|
| `generadoEn` | datetime | Momento real de generación (ISO 8601 con offset, p. ej. `2026-06-18T21:02:35+02:00`) |
| `anioMin` / `anioMax` | int | Rango de años cubierto |
| `rowFields` | string[] | Nombres de las posiciones de cada tupla (ver abajo) |
| `anios` | int[] | Eje de años (paralelo a `anual.*`) |
| `ejeMensual` | [int,int][] | Eje `[año, mes]` (paralelo a `mensual.*`) |
| `grupos` | object[] | `id`, `nombre`, `nEstaciones`, `provinciasAemet[]` |
| `anual` | object | `{ grupo_id: [tupla por año] }` |
| `mensual` | object | `{ grupo_id: [tupla por (año,mes)] }` |

`rowFields` (orden de cada tupla):

```
[ absolutoMax, absolutoMin, mensualMax, mensualMin,
  estacionesConDatos, estacionesBatieronMax, estacionesBatieronMin ]
```

- `absolutoMax/Min`, `mensualMax/Min` → nº de récords batidos de ese tipo en el periodo.
- `estacionesConDatos` → estaciones que reportaron temperatura (denominador).
- `estacionesBatieronMax/Min` → estaciones que batieron ≥1 récord de máx / mín.

`grupo_id` es `"total"` (nacional) o el slug de provincia (`"a-coruna"`, …).

```json
{
  "generadoEn": "2026-06-18T21:02:35+02:00",
  "anioMin": 1975, "anioMax": 2026,
  "rowFields": ["absolutoMax","absolutoMin","mensualMax","mensualMin","estacionesConDatos","estacionesBatieronMax","estacionesBatieronMin"],
  "anios": [1975, 1976, 1977, "…"],
  "ejeMensual": [[1975,1],[1975,2], "…"],
  "grupos": [
    { "id": "total", "nombre": "Todas las estaciones", "nEstaciones": 944, "provinciasAemet": [] },
    { "id": "a-coruna", "nombre": "A Coruña", "nEstaciones": 19, "provinciasAemet": ["A CORUÑA"] }
  ],
  "anual":  { "total": [[0,0,0,0,107,0,0], [51,42,1312,859,109,104,105], "…"] },
  "mensual": { "total": [[0,0,0,0,105,0,0], "…"] }
}
```

---

## `rankings.json`

Clasificaciones ("leaderboards") para la página de estadísticas, derivadas del
historial de récords. **Solo estaciones activas y solo récords definitivos** (sin
provisionales).

| Campo | Tipo | Descripción |
|---|---|---|
| `generadoEn` | datetime | Momento real de generación (ISO 8601 con offset) |
| `topAbs` | `{max,min}` | Récords absolutos vigentes más altos (TMAX / TMIN) |
| `topMes` | `{max,min}` | Como `topAbs` pero por mes calendario: `{ "1": [...], …, "12": [...] }` |
| `recientes` | array | Récords batidos más recientes de toda la red (definitivos) |
| `longevos` | `{max,min}` | Récords absolutos vigentes que llevan más tiempo sin superarse |
| `mayorSalto` | array | Récords absolutos que más superaron al anterior (`salto = valor − valorAnterior`) |
| `masActivas` | object | Estaciones que más récords baten: `anio`, `esteAnio[]`, `ultimos12m[]` |

Cada entrada de estación lleva (según la tabla): `ind`, `nombre`, `prov`, `alt`,
`valor`, `fecha` y, donde aplica, `tipo`, `mes`, `valorAnterior`, `salto`, `n`.
Los récords **mensuales** (`topMes` y las entradas mensuales de `recientes`)
llevan `abs: true` cuando coinciden con el récord absoluto vigente de su estación
(clave omitida si es false).

```json
{
  "generadoEn": "2026-06-18T21:02:35+02:00",
  "topAbs": { "max": [{ "ind": "4642E", "nombre": "…", "prov": "…", "alt": 7, "valor": 47.6, "fecha": "2021-08-14" }], "min": ["…"] },
  "topMes": { "max": { "1": ["…"], "7": [{ "ind": "5361X", "nombre": "MONTORO", "prov": "…", "alt": 155, "valor": 47.3, "fecha": "2017-07-13", "abs": true }] }, "min": { "…": [] } },
  "recientes": [{ "ind": "1387", "nombre": "…", "prov": "…", "tipo": "mensual-max", "mes": 6, "valor": 38.4, "valorAnterior": 37.9, "fecha": "2026-06-14", "provisional": false, "abs": true }],
  "longevos": { "max": ["…"], "min": ["…"] },
  "mayorSalto": [{ "ind": "C419X", "nombre": "ADEJE", "prov": "…", "tipo": "absoluto-max", "valor": 44.0, "valorAnterior": 35.0, "salto": 9.0, "fecha": "1976-08-09" }],
  "masActivas": { "anio": 2026, "esteAnio": [{ "ind": "…", "nombre": "…", "prov": "…", "n": 12 }], "ultimos12m": ["…"] }
}
```

`recientes` incluye un campo `provisional`, pero será siempre `false`: los
provisionales se excluyen (muchos no se confirman). Ver más abajo.

---

## Cómo se calcula un récord

Siempre se busca **el valor más alto** (es un proyecto sobre calor):

- **`absoluto-max`** → la TMAX más alta jamás registrada (el día más caluroso).
- **`absoluto-min`** → la TMIN más alta jamás registrada (la **noche más cálida**; *no* la más fría).
- **`mensual-max` / `mensual-min`** → lo mismo, segmentado por mes calendario.

Reglas:

- **Empates no cuentan**: un récord se bate solo si es **estrictamente superior**.
- **Vigente vs. batido**: el récord *vigente* es el valor más alto en curso
  (en `vigentes` / `mensuales`). Un récord *batido* es un evento que superó al
  anterior (en `eventos` y en los `totales`).
- **La serie debe estar "madura"**: al estrenarse una serie casi todo es "récord"
  por el simple avance estacional, así que un evento solo cuenta como batido si la
  estación lleva **≥1 año con datos** desde que empezó (o desde que se reanudó tras
  un hueco largo). Mientras no es madura, su `valorAnterior` es `null`. El valor
  **sí** sigue valiendo como récord vigente: si el máximo histórico se fijó antes
  de la madurez y nunca se superó, sigue vigente. (Esto evita "saltos" ficticios
  al comparar un récord nuevo contra una base rancia fijada antes de un hueco.)
- **Provisional**: mientras AEMET publica el diario definitivo se reconstruyen
  récords provisionales desde la observación horaria. Van marcados
  `provisional: true` en el mapa (`stations.json`), se reemplazan al llegar el
  definitivo y se **excluyen de toda la página de datos**: ni los agregados de
  `stats.json` ni las clasificaciones de `rankings.json` (incluida la de récords
  recientes) los tienen en cuenta.

## Cómo consumirlo

Los ficheros se sirven por HTTP desde `resolve/main` (no vía `load_dataset`):

```
https://huggingface.co/datasets/adrimaqueda/records-aemet/resolve/main/<fichero>
```

```js
const BASE = "https://huggingface.co/datasets/adrimaqueda/records-aemet/resolve/main";
const estaciones = await (await fetch(`${BASE}/stations.json`)).json();
const ficha      = await (await fetch(`${BASE}/stations/0009X.json`)).json();
const stats      = await (await fetch(`${BASE}/stats.json`)).json();
const rankings   = await (await fetch(`${BASE}/rankings.json`)).json();
```

> Sugerencia de caché: estos ficheros se sirven con caché por ETag/CDN. Para
> forzar frescura tras una actualización, cachea contra `stats.generadoEn`
> (p. ej. añadiéndolo como query param a las peticiones).

## Actualización

Se regenera **dos veces al día** (09:00 y 21:00, hora peninsular) desde un cron.
Cada pasada descarga lo nuevo de AEMET, recalcula los récords y republica estos
JSONs (incluida esta tarjeta). El campo `stats.generadoEn` indica el momento
exacto de la última generación.

*Tarjeta generada automáticamente el @@GENERADO@@.*

## Fuente, atribución y licencia

- **Fuente primaria**: [AEMET OpenData](https://opendata.aemet.es/) — climatologías
  diarias. Histórico inicial cargado vía el dataset [`datania/aemet`](https://huggingface.co/datasets/datania/aemet).
- **Atribución**: © AEMET. Elaboración propia a partir de datos de AEMET.
- **Licencia**: uso sujeto al [aviso legal de AEMET](https://www.aemet.es/es/nota_legal)
  sobre reutilización de sus datos (atribución y no tergiversación). Revisa esos
  términos antes de reutilizar.

## Limitaciones

- Solo **temperatura** (TMAX/TMIN); no incluye precipitación, viento, etc.
- Histórico desde **@@ANIO_MIN@@**; antes la cobertura es anecdótica.
- `stations.json` y las fichas incluyen solo estaciones **activas** (≥180 días
  reportados en los últimos 12 meses); `stats.json` cubre el histórico completo.
- Los récords se calculan **por estación**, no son récords provinciales ni
  nacionales homologados por AEMET.
"""


def _figures(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Cifras vivas para la tarjeta, sacadas de las tablas ya calculadas."""
    activas = con.execute(
        "SELECT COUNT(*) FROM station_coverage WHERE activa"
    ).fetchone()[0]
    historico = con.execute("SELECT COUNT(*) FROM station_coverage").fetchone()[0]
    anio_min, anio_max = con.execute(
        "SELECT MIN(EXTRACT(YEAR FROM fecha))::INTEGER, "
        "       MAX(EXTRACT(YEAR FROM fecha))::INTEGER "
        "FROM observations WHERE NOT COALESCE(provisional, FALSE)"
    ).fetchone()
    hasta = con.execute("SELECT MAX(datos_hasta) FROM station_coverage").fetchone()[0]
    batidos = con.execute(
        "SELECT COUNT(*) FROM record_events WHERE valor_anterior IS NOT NULL"
    ).fetchone()[0]
    # Separador de millares estilo español (1.234.567).
    batidos_es = f"{batidos:,}".replace(",", ".")
    return {
        "@@ACTIVAS@@": str(activas),
        "@@HISTORICO@@": str(historico),
        "@@ANIO_MIN@@": str(anio_min),
        "@@ANIO_MAX@@": str(anio_max),
        "@@HASTA@@": hasta.isoformat() if hasta else "—",
        "@@BATIDOS@@": batidos_es,
        "@@PROVINCIAS@@": str(len(PROVINCIA_NAMES)),
        # stations.json + stats.json + rankings.json + una ficha por estación activa.
        "@@NFICHEROS@@": str(activas + 3),
        "@@GENERADO@@": datetime.now().astimezone().isoformat(timespec="seconds"),
    }


def render(con: duckdb.DuckDBPyConnection) -> str:
    text = _TEMPLATE
    for token, value in _figures(con).items():
        text = text.replace(token, value)
    return text


def write(con: duckdb.DuckDBPyConnection, out_dir: Path) -> Path:
    path = out_dir / "README.md"
    path.write_text(render(con), encoding="utf-8")
    return path
