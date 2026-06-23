# Records AEMET · pipeline

Pipeline de datos de AEMET: descarga observaciones diarias, calcula los récords
de temperatura por estación y publica en HuggingFace los JSONs que consume la
app web. Es una **elaboración propia**, no un producto oficial de AEMET.

Flujo de datos:

```
AEMET OpenData ─┐
                ├─▶ DuckDB (histórico + incremental) ─▶ récords ─▶ JSONs ─▶ HuggingFace ─▶ app web
datania/aemet ──┘
```

Dataset público resultante: [`adrimaqueda/records-aemet`](https://huggingface.co/datasets/adrimaqueda/records-aemet).

## Requisitos

- Python >= 3.14
- [uv](https://docs.astral.sh/uv/)
- Variables de entorno (ver `.env.example`):
  - `AEMET_API_KEY` — para el fetcher incremental
  - `EXTREMOS_HF_REPO` — dataset **público** con los JSONs de la app (ej. `adrimaqueda/records-aemet`)
  - `EXTREMOS_HF_DB_REPO` — dataset **privado** para el backup de la DuckDB (ej. `adrimaqueda/records-aemet-db`)
  - `HF_TOKEN` — token con permisos **write** en ambos repos
    (https://huggingface.co/settings/tokens). El de solo lectura sirve para el
    backfill desde `datania`, pero **no** para publicar ni respaldar.

## Uso

```bash
cp .env.example .env         # y rellena AEMET_API_KEY, HF_TOKEN, etc.
uv sync                      # instala dependencias
uv run extremos-backfill     # one-shot: histórico desde HF datania
uv run extremos-fetch        # incremental desde API AEMET
uv run extremos-provisional  # récords provisionales desde el horario en tiempo real
uv run extremos-records      # recalcula récords
uv run extremos-stats        # genera stats.json (agregados de la página /datos)
uv run extremos-rankings     # genera rankings.json (clasificaciones de la página /datos)
uv run extremos-export       # genera los JSONs en outputs/
uv run extremos-publish      # sube outputs/ al dataset HF de la app
uv run extremos-backup       # respalda la DuckDB al dataset HF privado
uv run extremos-daily        # fetch + provisional + records + stats + rankings + export + publish
```

## Dónde viven los datos

| Qué | Tamaño | Dónde |
|---|---|---|
| JSONs de la app (`stations.json`, `stats.json`, `rankings.json`, `stations/*.json`) | ~28 MB | dataset HF **público** `EXTREMOS_HF_REPO`, servido por `resolve/main` |
| `aemet.duckdb` (base de trabajo) | ~342 MB | **solo en la Pi** (gitignored); reconstruible |
| Backup de la DuckDB (parquet ZSTD de tablas fuente) | ~75 MB | dataset HF **privado** `EXTREMOS_HF_DB_REPO` |

La app lee de HF en producción vía `VITE_DATA_BASE_URL` →
`https://huggingface.co/datasets/<EXTREMOS_HF_REPO>/resolve/main`.

## Backup y bootstrap de la Pi

El backup **no** sube los 342 MB del `.duckdb`. Exporta solo las tablas fuente
(`observations`, `stations`, `backfill_progress`) a Parquet ZSTD (~75 MB); las
derivadas (`record_events`, `station_coverage`) se recalculan con `records`.

```bash
uv run extremos-backup            # exporta a parquet y sube a EXTREMOS_HF_DB_REPO
uv run extremos-backup --restore  # descarga el backup y reconstruye la DB local
```

**Montar una Pi nueva** sin re-backfill de horas:

```bash
uv sync
uv run extremos-backup --restore  # baja el parquet y reconstruye la DB
uv run extremos-daily             # rellena el hueco, recalcula, exporta y publica
```

(Alternativa desde cero, varias horas: `uv run extremos-backfill`.)

### Récords provisionales

AEMET publica el diario climatológico con varios días de retraso (~4 según su
FAQ oficial). Para no dejar el mapa "congelado" esos días, `extremos-provisional` descarga la
observación **horaria** de las últimas 24 h (`/observacion/convencional/todas`),
la agrega a `tmax`/`tmin` por día (hora local de Madrid) y la inserta en
`observations` con `provisional = TRUE`. Esas filas:

- nunca pisan un dato definitivo (anti-join por PK `indicativo, fecha`);
- son reemplazadas por el diario definitivo cuando llega (con `provisional = FALSE`);
- aparecen en el **mapa** (`stations.json` / `stations/*.json`) marcadas
  `provisional`, que la app pinta distinto (borde blanco y número con "~");
- pero quedan **fuera de la página `/datos`**: tanto los agregados (`stats.json`)
  como las clasificaciones (`rankings.json`) usan solo definitivos. Esto incluye
  la lista de **"récords recientes"**, que antes sí los mostraba: muchos no se
  confirmaban (AEMET no publica el definitivo o lo corrige a la baja) y ensuciaban
  el panel con récords que luego no cuajaban.

**Conciliación** (cuando llega el definitivo, gracias al recálculo total de
`records`):

- el provisional **se cumplió** → la fila pasa a definitiva con la temperatura
  real y el récord queda confirmado (`provisional = FALSE`);
- **no se cumplió** → la fila definitiva (más baja) sustituye a la provisional y
  al recalcular el récord ya no se genera: desaparece;
- el definitivo **nunca llega** (la estación no reportó) → el provisional queda
  huérfano y `extremos-provisional` lo **purga** pasados `PROVISIONAL_MAX_AGE_DAYS`
  (15 por defecto).

Para que esto funcione, la ventana incremental de `fetch` se calcula solo sobre
fechas **definitivas** (ignora los provisionales), de modo que siempre vuelve a
bajar el diario del hueco y reemplaza a los provisionales.

Como la API horaria sólo cubre 24 h, cada pasada añade el día (parcial) más
reciente; el hueco de varios días se cubre de forma incremental ejecutando el cron.

## Programación (cron)

El ciclo de actualización (`extremos-daily`) se ejecuta **dos veces al día, a las
09:00 y a las 21:00** (hora local), vía `scripts/daily.sh`. El backup semanal
(`scripts/weekly.sh`) corre los domingos. Crontab en la Pi:

```cron
# Actualización: 09:00 y 21:00 todos los días
0 9,21 * * * ~/records-aemet-pipeline/scripts/daily.sh  >> ~/records-aemet-pipeline/daily.log  2>&1
# Backup de la DuckDB a HF privado: domingos a las 04:00
0 4 * * 0   ~/records-aemet-pipeline/scripts/weekly.sh >> ~/records-aemet-pipeline/weekly.log 2>&1
```

AEMET no publica a una hora fija (la periodicidad declarada es "continuamente"),
así que para el dato **definitivo** la hora es indiferente. Las dos pasadas
existen sobre todo por el **provisional**: la API horaria sólo da 24 h, de modo
que correr a las 09:00 y 21:00 hace que cada día natural quede bien cubierto entre
pasadas, y los extremos de un mismo día se **acumulan** (merge, no reemplazo), con
lo que el `tmax` de la tarde y el `tmin` de la madrugada se capturan ambos.

## Estructura

```
records-aemet-pipeline/
├── pyproject.toml
├── src/extremos/
│   ├── config.py       # rutas y constantes
│   ├── db.py           # conexión DuckDB + esquema
│   ├── backfill.py     # carga inicial desde HF
│   ├── fetch.py        # incremental contra AEMET (diario)
│   ├── provisional.py  # récords provisionales desde el horario en tiempo real
│   ├── records.py      # cómputo de récords (SQL)
│   ├── stats.py        # agregados para la página /datos (stats.json)
│   ├── rankings.py     # clasificaciones para la página /datos (rankings.json)
│   ├── export.py       # genera los JSONs en outputs/
│   ├── dataset_card.py # genera la tarjeta (README) del dataset HF público
│   ├── publish.py      # sube outputs/ al dataset HF público
│   ├── backup.py       # backup/restore de la DuckDB en HF privado
│   └── daily.py        # orquestador del cron
├── scripts/
│   ├── daily.sh        # cron de actualización (2×/día)
│   └── weekly.sh       # cron de backup (semanal)
└── data/aemet.duckdb   # (gitignored) base local
```

## Fuente y licencia de los datos

Los datos proceden de [AEMET OpenData](https://opendata.aemet.es/) (climatologías
diarias); el histórico inicial se carga vía el dataset
[`datania/aemet`](https://huggingface.co/datasets/datania/aemet). Este proyecto es
una **elaboración propia** (récords calculados a partir de esas observaciones), no
un producto oficial de AEMET. El uso de los datos está sujeto al
[aviso legal de AEMET](https://www.aemet.es/es/nota_legal) (atribución y no
tergiversación): © AEMET.
