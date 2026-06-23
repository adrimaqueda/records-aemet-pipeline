"""Normalización y nombre presentable de provincias.

Las provincias vienen del maestro de AEMET en MAYÚSCULAS y con dos casos en los
que la misma provincia aparece bajo nombres distintos:
  - "BALEARES" / "ILLES BALEARS"            → mismo identificador
  - "SANTA CRUZ DE TENERIFE" / "STA. CRUZ…" → mismo identificador

Mantenemos:
  - PROVINCIA_NORM:  nombre AEMET (mayúsculas) → id estable kebab-case.
  - PROVINCIA_NAMES: id → nombre presentable para la app.
"""
from __future__ import annotations

# id estable → nombre presentable.
PROVINCIA_NAMES: dict[str, str] = {
    "a-coruna": "A Coruña",
    "albacete": "Albacete",
    "alicante": "Alicante",
    "almeria": "Almería",
    "araba-alava": "Araba/Álava",
    "asturias": "Asturias",
    "avila": "Ávila",
    "badajoz": "Badajoz",
    "baleares": "Illes Balears",
    "barcelona": "Barcelona",
    "bizkaia": "Bizkaia",
    "burgos": "Burgos",
    "caceres": "Cáceres",
    "cadiz": "Cádiz",
    "cantabria": "Cantabria",
    "castellon": "Castellón",
    "ceuta": "Ceuta",
    "ciudad-real": "Ciudad Real",
    "cordoba": "Córdoba",
    "cuenca": "Cuenca",
    "gipuzkoa": "Gipuzkoa",
    "girona": "Girona",
    "granada": "Granada",
    "guadalajara": "Guadalajara",
    "huelva": "Huelva",
    "huesca": "Huesca",
    "jaen": "Jaén",
    "la-rioja": "La Rioja",
    "las-palmas": "Las Palmas",
    "leon": "León",
    "lleida": "Lleida",
    "lugo": "Lugo",
    "madrid": "Madrid",
    "malaga": "Málaga",
    "melilla": "Melilla",
    "murcia": "Murcia",
    "navarra": "Navarra",
    "ourense": "Ourense",
    "palencia": "Palencia",
    "pontevedra": "Pontevedra",
    "salamanca": "Salamanca",
    "santa-cruz-de-tenerife": "Santa Cruz de Tenerife",
    "segovia": "Segovia",
    "sevilla": "Sevilla",
    "soria": "Soria",
    "tarragona": "Tarragona",
    "teruel": "Teruel",
    "toledo": "Toledo",
    "valencia": "Valencia",
    "valladolid": "Valladolid",
    "zamora": "Zamora",
    "zaragoza": "Zaragoza",
}

# Nombre tal cual aparece en AEMET → id estable.
PROVINCIA_NORM: dict[str, str] = {
    "A CORUÑA": "a-coruna",
    "ALBACETE": "albacete",
    "ALICANTE": "alicante",
    "ALMERIA": "almeria",
    "ARABA/ALAVA": "araba-alava",
    "ASTURIAS": "asturias",
    "AVILA": "avila",
    "BADAJOZ": "badajoz",
    # Dos nombres para la misma provincia.
    "BALEARES": "baleares",
    "ILLES BALEARS": "baleares",
    "BARCELONA": "barcelona",
    "BIZKAIA": "bizkaia",
    "BURGOS": "burgos",
    "CACERES": "caceres",
    "CADIZ": "cadiz",
    "CANTABRIA": "cantabria",
    "CASTELLON": "castellon",
    "CEUTA": "ceuta",
    "CIUDAD REAL": "ciudad-real",
    "CORDOBA": "cordoba",
    "CUENCA": "cuenca",
    "GIPUZKOA": "gipuzkoa",
    "GIRONA": "girona",
    "GRANADA": "granada",
    "GUADALAJARA": "guadalajara",
    "HUELVA": "huelva",
    "HUESCA": "huesca",
    "JAEN": "jaen",
    "LA RIOJA": "la-rioja",
    "LAS PALMAS": "las-palmas",
    "LEON": "leon",
    "LLEIDA": "lleida",
    "LUGO": "lugo",
    "MADRID": "madrid",
    "MALAGA": "malaga",
    "MELILLA": "melilla",
    "MURCIA": "murcia",
    "NAVARRA": "navarra",
    "OURENSE": "ourense",
    "PALENCIA": "palencia",
    "PONTEVEDRA": "pontevedra",
    "SALAMANCA": "salamanca",
    # Dos nombres para la misma provincia.
    "SANTA CRUZ DE TENERIFE": "santa-cruz-de-tenerife",
    "STA. CRUZ DE TENERIFE": "santa-cruz-de-tenerife",
    "SEGOVIA": "segovia",
    "SEVILLA": "sevilla",
    "SORIA": "soria",
    "TARRAGONA": "tarragona",
    "TERUEL": "teruel",
    "TOLEDO": "toledo",
    "VALENCIA": "valencia",
    "VALLADOLID": "valladolid",
    "ZAMORA": "zamora",
    "ZARAGOZA": "zaragoza",
}


def rows_for_duckdb() -> list[tuple[str, str]]:
    """Pares (provincia_aemet, provincia_id) listos para un VALUES en DuckDB."""
    return list(PROVINCIA_NORM.items())
