#!/usr/bin/env bash
# Cron de actualización de la Raspberry Pi: dos veces al día, 09:00 y 21:00
# (hora local). Crontab (`crontab -e`):
#
#   0 9,21 * * * /usr/local/bin/cron-alert "Pipeline AEMET (daily)" ~/records-aemet-pipeline/scripts/daily.sh >> ~/records-aemet-pipeline/daily.log 2>&1
#
# Correr 2×/día mantiene fresco el provisional (la pasada de la tarde ya ve el
# pico de tmax del día); los extremos de cada día se acumulan entre pasadas
# (merge en provisional.py), no se reemplazan.
exec "$(dirname "$0")/run.sh" extremos-daily
