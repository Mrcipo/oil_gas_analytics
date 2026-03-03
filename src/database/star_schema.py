from __future__ import annotations

import os
from uuid import uuid4

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

STAR_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS dim_empresa (
  id_empresa BIGSERIAL PRIMARY KEY,
  nombre_empresa TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_geografia (
  id_geo BIGSERIAL PRIMARY KEY,
  cuenca TEXT NOT NULL,
  provincia TEXT NOT NULL,
  yacimiento TEXT NOT NULL,
  CONSTRAINT uq_dim_geografia UNIQUE (cuenca, provincia, yacimiento)
);

CREATE TABLE IF NOT EXISTS dim_pozo (
  id_pozo BIGSERIAL PRIMARY KEY,
  nombre_pozo TEXT NOT NULL,
  tipo_reservorio TEXT NOT NULL,
  profundidad NUMERIC(12, 2),
  CONSTRAINT uq_dim_pozo UNIQUE (nombre_pozo, tipo_reservorio, profundidad)
);

CREATE TABLE IF NOT EXISTS dim_tiempo (
  fecha DATE PRIMARY KEY,
  anio SMALLINT NOT NULL,
  mes SMALLINT NOT NULL,
  trimestre SMALLINT NOT NULL,
  semestre SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_produccion (
  id_fact BIGSERIAL PRIMARY KEY,
  fecha DATE NOT NULL,
  id_empresa BIGINT NOT NULL,
  id_geo BIGINT NOT NULL,
  id_pozo BIGINT NOT NULL,
  prod_pet NUMERIC(18, 4) NOT NULL DEFAULT 0,
  prod_gas NUMERIC(18, 4) NOT NULL DEFAULT 0,
  prod_agua NUMERIC(18, 4) NOT NULL DEFAULT 0,
  iny_pet NUMERIC(18, 4) NOT NULL DEFAULT 0,
  iny_gas NUMERIC(18, 4) NOT NULL DEFAULT 0,
  iny_agua NUMERIC(18, 4) NOT NULL DEFAULT 0,
  tef NUMERIC(18, 4) NOT NULL DEFAULT 0,
  CONSTRAINT fk_fact_tiempo FOREIGN KEY (fecha) REFERENCES dim_tiempo(fecha),
  CONSTRAINT fk_fact_empresa FOREIGN KEY (id_empresa) REFERENCES dim_empresa(id_empresa),
  CONSTRAINT fk_fact_geo FOREIGN KEY (id_geo) REFERENCES dim_geografia(id_geo),
  CONSTRAINT fk_fact_pozo FOREIGN KEY (id_pozo) REFERENCES dim_pozo(id_pozo),
  CONSTRAINT uq_fact_grano UNIQUE (fecha, id_empresa, id_geo, id_pozo)
);

CREATE INDEX IF NOT EXISTS idx_fact_empresa ON fact_produccion(id_empresa);
CREATE INDEX IF NOT EXISTS idx_fact_geo ON fact_produccion(id_geo);
CREATE INDEX IF NOT EXISTS idx_fact_pozo ON fact_produccion(id_pozo);
CREATE INDEX IF NOT EXISTS idx_fact_fecha ON fact_produccion(fecha);
"""

COLUMN_CANDIDATES = {
    "fecha": ["fecha", "fecha_produccion", "fecha_medicion", "mes", "periodo"],
    "empresa": ["empresa", "operador", "empresa_operadora", "concesionario"],
    "cuenca": ["cuenca"],
    "provincia": ["provincia"],
    "yacimiento": ["yacimiento", "campo", "nombre_yacimiento"],
    "nombre_pozo": ["pozo", "nombre_pozo", "idpozo", "id_pozo", "sigla"],
    "tipo_reservorio": ["tipo_reservorio", "tipo_de_recurso", "tipo_recurso"],
    "profundidad": ["profundidad", "prof_total"],
    "prod_pet": ["prod_pet", "petroleo", "petroleo_m3", "prod_pet_m3"],
    "prod_gas": ["prod_gas", "gas", "gas_m3", "prod_gas_m3"],
    "prod_agua": ["prod_agua", "agua", "agua_m3", "prod_agua_m3"],
    "iny_pet": ["iny_pet", "iny_pet_m3", "inyeccion_petroleo", "petroleo_iny"],
    "iny_gas": ["iny_gas", "iny_gas_m3", "inyeccion_gas", "gas_iny"],
    "iny_agua": ["iny_agua", "iny_agua_m3", "inyeccion_agua", "agua_iny"],
    "tef": ["tef", "tiempo_efectivo", "factor_efectivo", "dias_efectivos"],
}


def build_engine_from_env() -> Engine:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("La variable DATABASE_URL es obligatoria")
    return create_engine(database_url)


def initialize_star_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(STAR_SCHEMA_DDL))


def _pick_column(df: pd.DataFrame, field: str) -> str | None:
    for candidate in COLUMN_CANDIDATES[field]:
        if candidate in df.columns:
            return candidate
    return None


def _normalize_text(series: pd.Series, default_value: str) -> pd.Series:
    return (
        series.fillna(default_value)
        .astype(str)
        .str.strip()
        .replace("", default_value)
        .fillna(default_value)
    )


def _build_date_series(df: pd.DataFrame) -> pd.Series:
    fecha_col = _pick_column(df, "fecha")
    raw_fecha = df[fecha_col] if fecha_col else pd.Series(index=df.index, dtype="object")
    parsed_fecha = pd.to_datetime(raw_fecha, errors="coerce", dayfirst=True)

    raw_numeric = pd.to_numeric(raw_fecha, errors="coerce")
    yyyymm_mask = raw_numeric.between(190001, 210012)
    if yyyymm_mask.any():
        yyyymm_values = (
            raw_numeric.where(yyyymm_mask)
            .round()
            .astype("Int64")
            .astype(str)
            .str.zfill(6)
        )
        parsed_yyyymm = pd.to_datetime(yyyymm_values, format="%Y%m", errors="coerce")
        parsed_fecha = parsed_fecha.where(~yyyymm_mask, parsed_yyyymm)

    if {"anio", "mes"}.issubset(df.columns):
        anio = pd.to_numeric(df["anio"], errors="coerce")
        mes = pd.to_numeric(df["mes"], errors="coerce")
        anio_mes_mask = anio.between(1900, 2100) & mes.between(1, 12)
        if anio_mes_mask.any():
            derived_fecha = pd.to_datetime(
                {
                    "year": anio.where(anio_mes_mask),
                    "month": mes.where(anio_mes_mask),
                    "day": 1,
                },
                errors="coerce",
            )
            # Priorizar anio/mes cuando existe para evitar parseos ambiguos (ej. 1970)
            parsed_fecha = derived_fecha.combine_first(parsed_fecha)

    return parsed_fecha.dt.date


def prepare_fact_frame(df: pd.DataFrame) -> pd.DataFrame:
    prepared = pd.DataFrame(index=df.index)
    prepared["fecha"] = _build_date_series(df)

    empresa_col = _pick_column(df, "empresa")
    cuenca_col = _pick_column(df, "cuenca")
    provincia_col = _pick_column(df, "provincia")
    yacimiento_col = _pick_column(df, "yacimiento")
    nombre_pozo_col = _pick_column(df, "nombre_pozo")
    tipo_reservorio_col = _pick_column(df, "tipo_reservorio")
    profundidad_col = _pick_column(df, "profundidad")

    prepared["nombre_empresa"] = _normalize_text(
        df[empresa_col] if empresa_col else pd.Series(index=df.index, dtype="object"),
        "NO INFORMADA",
    )
    prepared["cuenca"] = _normalize_text(
        df[cuenca_col] if cuenca_col else pd.Series(index=df.index, dtype="object"),
        "NO INFORMADA",
    )
    prepared["provincia"] = _normalize_text(
        df[provincia_col] if provincia_col else pd.Series(index=df.index, dtype="object"),
        "NO INFORMADA",
    )
    prepared["yacimiento"] = _normalize_text(
        df[yacimiento_col] if yacimiento_col else pd.Series(index=df.index, dtype="object"),
        "NO INFORMADO",
    )
    prepared["nombre_pozo"] = _normalize_text(
        df[nombre_pozo_col] if nombre_pozo_col else pd.Series(index=df.index, dtype="object"),
        "POZO_DESCONOCIDO",
    )
    prepared["tipo_reservorio"] = _normalize_text(
        (
            df[tipo_reservorio_col]
            if tipo_reservorio_col
            else pd.Series(index=df.index, dtype="object")
        ),
        "NO INFORMADO",
    )
    prepared["profundidad"] = pd.to_numeric(
        df[profundidad_col] if profundidad_col else pd.Series(index=df.index, dtype="float"),
        errors="coerce",
    )

    for metric in ["prod_pet", "prod_gas", "prod_agua", "iny_pet", "iny_gas", "iny_agua", "tef"]:
        metric_col = _pick_column(df, metric)
        prepared[metric] = pd.to_numeric(
            df[metric_col] if metric_col else pd.Series(index=df.index, dtype="float"),
            errors="coerce",
        ).fillna(0.0)

    prepared = prepared.dropna(subset=["fecha"])
    return prepared


def load_dataframe_to_star_schema(df: pd.DataFrame, engine: Engine) -> int:
    fact_frame = prepare_fact_frame(df)
    if fact_frame.empty:
        return 0

    staging_table = f"stg_produccion_{uuid4().hex[:8]}"

    fact_frame.to_sql(
        name=staging_table,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )

    with engine.begin() as conn:
        conn.execute(text(STAR_SCHEMA_DDL))

        conn.execute(
            text(
                f"""
                INSERT INTO dim_empresa (nombre_empresa)
                SELECT DISTINCT s.nombre_empresa
                FROM {staging_table} s
                ON CONFLICT (nombre_empresa) DO NOTHING;
                """
            )
        )

        conn.execute(
            text(
                f"""
                INSERT INTO dim_geografia (cuenca, provincia, yacimiento)
                SELECT DISTINCT s.cuenca, s.provincia, s.yacimiento
                FROM {staging_table} s
                ON CONFLICT (cuenca, provincia, yacimiento) DO NOTHING;
                """
            )
        )

        conn.execute(
            text(
                f"""
                INSERT INTO dim_pozo (nombre_pozo, tipo_reservorio, profundidad)
                SELECT DISTINCT s.nombre_pozo, s.tipo_reservorio, s.profundidad
                FROM {staging_table} s
                ON CONFLICT (nombre_pozo, tipo_reservorio, profundidad) DO NOTHING;
                """
            )
        )

        conn.execute(
            text(
                f"""
                INSERT INTO dim_tiempo (fecha, anio, mes, trimestre, semestre)
                SELECT DISTINCT
                    s.fecha,
                    EXTRACT(YEAR FROM s.fecha)::SMALLINT,
                    EXTRACT(MONTH FROM s.fecha)::SMALLINT,
                    EXTRACT(QUARTER FROM s.fecha)::SMALLINT,
                    CASE WHEN EXTRACT(MONTH FROM s.fecha) <= 6 THEN 1 ELSE 2 END::SMALLINT
                FROM {staging_table} s
                ON CONFLICT (fecha) DO NOTHING;
                """
            )
        )

        result = conn.execute(
            text(
                f"""
                WITH fact_agg AS (
                    SELECT
                        s.fecha,
                        de.id_empresa,
                        dg.id_geo,
                        dp.id_pozo,
                        SUM(s.prod_pet) AS prod_pet,
                        SUM(s.prod_gas) AS prod_gas,
                        SUM(s.prod_agua) AS prod_agua,
                        SUM(s.iny_pet) AS iny_pet,
                        SUM(s.iny_gas) AS iny_gas,
                        SUM(s.iny_agua) AS iny_agua,
                        SUM(s.tef) AS tef
                    FROM {staging_table} s
                    JOIN dim_empresa de
                      ON de.nombre_empresa = s.nombre_empresa
                    JOIN dim_geografia dg
                      ON dg.cuenca = s.cuenca
                     AND dg.provincia = s.provincia
                     AND dg.yacimiento = s.yacimiento
                    JOIN dim_pozo dp
                      ON dp.nombre_pozo = s.nombre_pozo
                     AND dp.tipo_reservorio = s.tipo_reservorio
                     AND dp.profundidad IS NOT DISTINCT FROM s.profundidad
                    GROUP BY s.fecha, de.id_empresa, dg.id_geo, dp.id_pozo
                )
                INSERT INTO fact_produccion (
                    fecha, id_empresa, id_geo, id_pozo,
                    prod_pet, prod_gas, prod_agua,
                    iny_pet, iny_gas, iny_agua, tef
                )
                SELECT
                    fa.fecha,
                    fa.id_empresa,
                    fa.id_geo,
                    fa.id_pozo,
                    fa.prod_pet,
                    fa.prod_gas,
                    fa.prod_agua,
                    fa.iny_pet,
                    fa.iny_gas,
                    fa.iny_agua,
                    fa.tef
                FROM fact_agg fa
                ON CONFLICT (fecha, id_empresa, id_geo, id_pozo)
                DO UPDATE SET
                    prod_pet = EXCLUDED.prod_pet,
                    prod_gas = EXCLUDED.prod_gas,
                    prod_agua = EXCLUDED.prod_agua,
                    iny_pet = EXCLUDED.iny_pet,
                    iny_gas = EXCLUDED.iny_gas,
                    iny_agua = EXCLUDED.iny_agua,
                    tef = EXCLUDED.tef;
                """
            )
        )

        conn.execute(text(f"DROP TABLE IF EXISTS {staging_table};"))

    return result.rowcount if result.rowcount and result.rowcount > 0 else len(fact_frame)
