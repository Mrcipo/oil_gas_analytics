from __future__ import annotations

import os
from typing import Iterable

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def compute_zero_streak(series: pd.Series) -> pd.Series:
    streak = 0
    values: list[int] = []
    for value in series.fillna(0):
        if value == 0:
            streak += 1
        else:
            streak = 0
        values.append(streak)
    return pd.Series(values, index=series.index)


def build_engine_from_env() -> Engine:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        database = os.getenv("POSTGRES_DB")
        host = os.getenv("POSTGRES_HOST", "db")
        port = os.getenv("POSTGRES_PORT", "5432")

        if user and password and database:
            database_url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
        else:
            raise RuntimeError(
                "No hay DATABASE_URL y faltan variables POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_DB"
            )
    return create_engine(database_url)


def load_monthly_well_production(
    engine: Engine,
    start_date: str = "2015-01-01",
    end_date: str = "2027-01-01",
    target_col: str = "prod_pet",
    history_months: int = 6,
    keep_only_window: bool = False,
    top_n_pozos: int | None = None,
) -> pd.DataFrame:
    if target_col not in {"prod_pet", "prod_gas", "prod_agua"}:
        raise ValueError("target_col debe ser uno de: prod_pet, prod_gas, prod_agua")

    start_ts = pd.Timestamp(start_date)
    query_start = (start_ts - pd.DateOffset(months=history_months)).date().isoformat()

    if top_n_pozos is not None and top_n_pozos <= 0:
        raise ValueError("top_n_pozos debe ser mayor que 0")

    if top_n_pozos is None:
        query = f"""
        SELECT
            f.id_pozo,
            f.fecha::date AS fecha,
            f.{target_col} AS target,
            p.profundidad,
            p.tipo_reservorio,
            fp.primera_fecha::date AS primera_fecha_pozo
        FROM fact_produccion f
        JOIN dim_pozo p ON p.id_pozo = f.id_pozo
        JOIN (
            SELECT id_pozo, MIN(fecha)::date AS primera_fecha
            FROM fact_produccion
            GROUP BY id_pozo
        ) fp ON fp.id_pozo = f.id_pozo
        WHERE f.fecha >= CAST(%(query_start)s AS DATE)
          AND f.fecha < CAST(%(end_date)s AS DATE)
        ORDER BY f.id_pozo, f.fecha;
        """
    else:
        # Reduce memoria en notebook: limitar pozos top en SQL antes de traer filas.
        query = f"""
        WITH top_pozos AS (
            SELECT
                id_pozo
            FROM fact_produccion
            WHERE fecha >= CAST(%(start_date)s AS DATE)
              AND fecha < CAST(%(end_date)s AS DATE)
            GROUP BY id_pozo
            ORDER BY SUM({target_col}) DESC
            LIMIT %(top_n_pozos)s
        )
        SELECT
            f.id_pozo,
            f.fecha::date AS fecha,
            f.{target_col} AS target,
            p.profundidad,
            p.tipo_reservorio,
            fp.primera_fecha::date AS primera_fecha_pozo
        FROM fact_produccion f
        JOIN top_pozos tp ON tp.id_pozo = f.id_pozo
        JOIN dim_pozo p ON p.id_pozo = f.id_pozo
        JOIN (
            SELECT id_pozo, MIN(fecha)::date AS primera_fecha
            FROM fact_produccion
            GROUP BY id_pozo
        ) fp ON fp.id_pozo = f.id_pozo
        WHERE f.fecha >= CAST(%(query_start)s AS DATE)
          AND f.fecha < CAST(%(end_date)s AS DATE)
        ORDER BY f.id_pozo, f.fecha;
        """

    df = pd.read_sql(
        query,
        engine,
        params={
            "query_start": query_start,
            "start_date": start_date,
            "end_date": end_date,
            "top_n_pozos": top_n_pozos,
        },
    )
    df["fecha"] = pd.to_datetime(df["fecha"])
    df["primera_fecha_pozo"] = pd.to_datetime(df["primera_fecha_pozo"])
    df["in_window"] = (df["fecha"] >= pd.Timestamp(start_date)) & (
        df["fecha"] < pd.Timestamp(end_date)
    )
    df = df.sort_values(["id_pozo", "fecha"]).reset_index(drop=True)
    if keep_only_window:
        df = df[df["in_window"]].copy()
    return df


def _add_lag_features(
    df: pd.DataFrame,
    value_col: str,
    group_col: str,
    lags: Iterable[int],
) -> pd.DataFrame:
    out = df.copy()
    grouped = out.groupby(group_col)[value_col]
    for lag in lags:
        out[f"{value_col}_lag_{lag}"] = grouped.shift(lag)
    return out


def _add_rolling_features(
    df: pd.DataFrame,
    value_col: str,
    group_col: str,
    window: int = 6,
) -> pd.DataFrame:
    out = df.copy()
    grouped = out.groupby(group_col)[value_col]
    # shift(1) evita leakage al calcular ventanas para prediccion en t.
    out[f"{value_col}_roll_mean_{window}"] = grouped.transform(
        lambda s: s.shift(1).rolling(window, min_periods=3).mean()
    )
    out[f"{value_col}_roll_std_{window}"] = grouped.transform(
        lambda s: s.shift(1).rolling(window, min_periods=3).std()
    )
    return out


def _add_zero_streak_feature(
    df: pd.DataFrame,
    value_col: str,
    group_col: str,
) -> pd.DataFrame:
    out = df.copy()
    # Cuenta meses consecutivos recientes con produccion cero por pozo.
    out["streak_ceros"] = out.groupby(group_col, group_keys=False)[value_col].apply(
        compute_zero_streak
    )
    return out


def _add_well_age_feature(
    df: pd.DataFrame,
    group_col: str = "id_pozo",
    date_col: str = "fecha",
) -> pd.DataFrame:
    out = df.copy()
    if "primera_fecha_pozo" in out.columns:
        first_prod_date = pd.to_datetime(out["primera_fecha_pozo"])
    else:
        first_prod_date = out.groupby(group_col)[date_col].transform("min")
    out["edad_pozo_meses"] = (
        (out[date_col].dt.year - first_prod_date.dt.year) * 12
        + (out[date_col].dt.month - first_prod_date.dt.month)
    )
    return out


def _add_static_ohe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["tipo_reservorio"] = out["tipo_reservorio"].fillna("NO_INFORMADO")
    out = pd.get_dummies(out, columns=["tipo_reservorio"], prefix="res", dummy_na=False)
    return out


def _downcast_numeric(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    float_cols = out.select_dtypes(include=["float64"]).columns
    int_cols = out.select_dtypes(include=["int64"]).columns
    if len(float_cols) > 0:
        out[float_cols] = out[float_cols].astype("float32")
    if len(int_cols) > 0:
        out[int_cols] = out[int_cols].astype("int32")
    return out


def build_feature_dataset(
    df: pd.DataFrame,
    value_col: str = "target",
    group_col: str = "id_pozo",
    lags: tuple[int, int, int] = (1, 3, 6),
    rolling_window: int = 6,
    downcast: bool = True,
) -> pd.DataFrame:
    required_cols = {group_col, "fecha", value_col, "profundidad", "tipo_reservorio"}
    missing = required_cols.difference(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {sorted(missing)}")

    out = df.copy()
    out = out.sort_values([group_col, "fecha"]).reset_index(drop=True)
    out = _add_lag_features(out, value_col=value_col, group_col=group_col, lags=lags)
    out = _add_rolling_features(
        out, value_col=value_col, group_col=group_col, window=rolling_window
    )
    out = _add_zero_streak_feature(out, value_col=value_col, group_col=group_col)
    out = _add_well_age_feature(out, group_col=group_col, date_col="fecha")
    out = _add_static_ohe(out)
    if downcast:
        out = _downcast_numeric(out)
    return out


if __name__ == "__main__":
    engine = build_engine_from_env()
    base_df = load_monthly_well_production(engine, target_col="prod_pet")
    feature_df = build_feature_dataset(base_df)
    print(feature_df.head())
    print(f"Dataset de features generado: {feature_df.shape[0]} filas x {feature_df.shape[1]} columnas")
