import re
import unicodedata
from typing import Iterable

import pandas as pd
import requests

from src.database.star_schema import (
    build_engine_from_env,
    initialize_star_schema,
    load_dataframe_to_star_schema,
)

YEAR_RESOURCE_MAP = {
    2023: "34f07971-552d-4589-9e8c-57224213813a",
    2022: "0e3505c2-f19b-465a-935f-40f4bf884784",
    2021: "5e8903c7-4348-4364-8149-6f977c3856d3",
    2020: "85e82c89-6019-4809-a15e-a60d6f461e72",
    2019: "0c609100-2f95-4673-8902-6e279b940026",
    2018: "2990616b-b461-4876-90f7-5264bc80738e",
    2017: "28e235f9-250e-434a-9b43-41c3e3871929",
    2016: "524c5520-cb96-4a30-819a-43186835a646",
    2015: "0059c362-e19c-4395-888f-7c151e36f903",
    2014: "9565e31d-b8da-430c-b26a-937b420f1882",
    2013: "277e9973-1959-450f-90e9-b57223b37936",
    2012: "69e71569-87c2-4911-945f-4613b53c151c",
    2011: "c380e928-1113-4315-849f-7f722081d11b",
    2010: "b14f6b45-1393-4e4b-bb1a-f7793d56b005",
}

LEGACY_BASE_URL = (
    "http://datos.energia.gob.ar/dataset/"
    "c846e79c-026c-4040-897f-1ad3543b407c/resource/"
    "{resource_id}/download/produccion-de-pozos-de-gas-y-petroleo-{year}.csv"
)
CKAN_PACKAGE_SHOW_URL = (
    "http://datos.energia.gob.ar/api/3/action/"
    "package_show?id=produccion-de-petroleo-y-gas-por-pozo"
)
MIN_YEAR = 2015
MAX_YEAR = 2026

COMPANY_NORMALIZATION_MAP = {
    "YPF SA": "YPF",
    "YPF S A": "YPF",
    "Y P F": "YPF",
    "PAN AMERICAN ENERGY SL": "PAN AMERICAN ENERGY",
    "TOTAL AUSTRAL SA": "TOTAL AUSTRAL",
}

DATE_COLUMN_CANDIDATES = [
    "fecha",
    "fecha_produccion",
    "fecha_medicion",
    "mes",
    "periodo",
]

COMPANY_COLUMN_CANDIDATES = [
    "empresa",
    "operador",
    "empresa_operadora",
    "concesionario",
]

YACIMIENTO_COLUMN_CANDIDATES = [
    "yacimiento",
    "campo",
    "nombre_yacimiento",
]

LAT_COLUMN_CANDIDATES = [
    "latitud",
    "latitude",
    "coord_y",
    "coordenada_y",
    "coordenada_lat",
    "y",
]

LON_COLUMN_CANDIDATES = [
    "longitud",
    "longitude",
    "coord_x",
    "coordenada_x",
    "coordenada_lon",
    "x",
]


def build_url(year: int, resource_id: str) -> str:
    return LEGACY_BASE_URL.format(resource_id=resource_id, year=year)


def _resource_contains_production_wells(resource: dict) -> bool:
    name = str(resource.get("name", "")).lower()
    description = str(resource.get("description", "")).lower()
    candidate = f"{name} {description}"
    return "pozo" in candidate and "produ" in candidate


def _extract_year_from_resource(resource: dict) -> int | None:
    candidate_text = " ".join(
        [
            str(resource.get("name", "")),
            str(resource.get("description", "")),
            str(resource.get("url", "")),
            str(resource.get("id", "")),
        ]
    )
    years = re.findall(r"(20[1-2][0-9])", candidate_text)
    for year_text in years:
        year = int(year_text)
        if MIN_YEAR <= year <= MAX_YEAR:
            return year
    return None


def _is_csv_resource(resource: dict) -> bool:
    format_value = str(resource.get("format", "")).lower()
    url = str(resource.get("url", "")).lower()
    return format_value == "csv" or ".csv" in url


def get_updated_urls() -> dict[int, str]:
    response = requests.get(CKAN_PACKAGE_SHOW_URL, timeout=30)
    response.raise_for_status()
    payload = response.json()

    if not payload.get("success"):
        raise RuntimeError("La API CKAN respondio success=false")

    resources = payload.get("result", {}).get("resources", [])
    year_url_map: dict[int, str] = {}

    for resource in resources:
        if not isinstance(resource, dict):
            continue
        if not _is_csv_resource(resource):
            continue
        if not _resource_contains_production_wells(resource):
            continue

        year = _extract_year_from_resource(resource)
        url = resource.get("url")
        if year is None or not url:
            continue

        try:
            head_response = requests.head(url, allow_redirects=True, timeout=20)
            if head_response.status_code == 200:
                year_url_map[year] = url
        except requests.RequestException:
            continue

    if not year_url_map:
        raise RuntimeError("No se encontraron URLs validas en CKAN para produccion por pozo")

    return dict(sorted(year_url_map.items(), reverse=True))


def to_snake_case(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(text))
    without_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    snake = re.sub(r"[^a-zA-Z0-9]+", "_", without_accents).strip("_").lower()
    return snake


def normalize_company_name(value: object) -> object:
    if value is None or pd.isna(value):
        return value

    text = str(value).strip()
    if not text:
        return text

    upper = text.upper().replace(".", "").replace("  ", " ").strip()
    return COMPANY_NORMALIZATION_MAP.get(upper, upper)


def get_first_existing_column(columns: Iterable[str], candidates: list[str]) -> str | None:
    available = set(columns)
    for candidate in candidates:
        if candidate in available:
            return candidate
    return None


def read_year_csv(url: str) -> pd.DataFrame:
    separators = [",", ";", "\t"]
    encodings = ["utf-8", "latin-1"]
    last_error: Exception | None = None

    for sep in separators:
        for encoding in encodings:
            try:
                return pd.read_csv(url, sep=sep, low_memory=False, encoding=encoding)
            except Exception as exc:
                last_error = exc

    if last_error is None:
        raise RuntimeError("No fue posible leer el CSV")
    raise last_error


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [to_snake_case(col) for col in df.columns]

    date_col = get_first_existing_column(df.columns, DATE_COLUMN_CANDIDATES)
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)

    company_col = get_first_existing_column(df.columns, COMPANY_COLUMN_CANDIDATES)
    if company_col:
        df[company_col] = df[company_col].map(normalize_company_name)

    yacimiento_col = get_first_existing_column(df.columns, YACIMIENTO_COLUMN_CANDIDATES)
    lat_col = get_first_existing_column(df.columns, LAT_COLUMN_CANDIDATES)
    lon_col = get_first_existing_column(df.columns, LON_COLUMN_CANDIDATES)

    if yacimiento_col and lat_col:
        df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
        df[lat_col] = df[lat_col].fillna(df.groupby(yacimiento_col)[lat_col].transform("mean"))

    if yacimiento_col and lon_col:
        df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
        df[lon_col] = df[lon_col].fillna(df.groupby(yacimiento_col)[lon_col].transform("mean"))

    return df


def run_extraction_and_load() -> int:
    engine = build_engine_from_env()
    initialize_star_schema(engine)

    total_rows = 0
    years_loaded = 0
    url_map: dict[int, str] = {}

    try:
        url_map = get_updated_urls()
        print(f"Se obtuvieron {len(url_map)} URLs actualizadas desde CKAN")
    except Exception as exc:
        print(f"No se pudieron obtener URLs actualizadas desde CKAN: {exc}")
        print("Usando diccionario legacy YEAR_RESOURCE_MAP como fallback")
        url_map = {
            year: build_url(year, resource_id)
            for year, resource_id in sorted(YEAR_RESOURCE_MAP.items(), reverse=True)
            if MIN_YEAR <= year <= MAX_YEAR
        }

    for year, url in sorted(url_map.items(), reverse=True):
        try:
            print(f"Descargando anio {year} desde {url} ...")
            df_raw = read_year_csv(url)
            df_clean = clean_dataframe(df_raw)
            loaded_rows = load_dataframe_to_star_schema(df_clean, engine)

            total_rows += loaded_rows
            years_loaded += 1
            print(f"Cargado anio {year} con exito. Filas fact: {loaded_rows}")
        except Exception as exc:
            print(f"Error procesando anio {year}: {exc}")

    engine.dispose()
    print(f"Proceso finalizado. Anios cargados: {years_loaded} | Filas totales: {total_rows}")
    return total_rows


if __name__ == "__main__":
    run_extraction_and_load()
