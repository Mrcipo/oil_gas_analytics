import os
from datetime import date
from typing import Any

import pandas as pd
import psycopg
from fastapi import FastAPI, HTTPException
from xgboost import XGBRegressor

app = FastAPI(title="oil_gas_analytics API", version="0.2.0")
model: XGBRegressor | None = None
model_features: list[str] = []


def _dsn() -> str:
    host = os.getenv("POSTGRES_HOST", "db")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "oil_user")
    password = os.getenv("POSTGRES_PASSWORD", "oil_pass")
    database = os.getenv("POSTGRES_DB", "oil_gas")
    return f"host={host} port={port} user={user} password={password} dbname={database}"


def _to_float(value: Any) -> float:
    return float(value) if value is not None else 0.0


@app.on_event("startup")
def load_model() -> None:
    global model, model_features
    model_path = os.getenv("MODEL_PATH", "/app/models/xgboost_declinacion_v1.json")
    if not os.path.exists(model_path):
        model = None
        model_features = []
        return

    loaded = XGBRegressor()
    loaded.load_model(model_path)
    booster = loaded.get_booster()
    model = loaded
    model_features = list(booster.feature_names or [])


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "model_loaded": model is not None,
        "model_features": len(model_features),
    }


@app.get("/runs")
def list_runs(limit: int = 20) -> list[dict[str, Any]]:
    try:
        with psycopg.connect(_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, records_loaded, status, created_at
                    FROM etl_runs
                    ORDER BY id DESC
                    LIMIT %s;
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "records_loaded": r[1],
                "status": r[2],
                "created_at": r[3].isoformat() if r[3] else None,
            }
            for r in rows
        ]
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB no disponible: {exc}") from exc


@app.get("/metrics/{cuenca}")
def metrics_by_basin(cuenca: str) -> dict[str, Any]:
    try:
        with psycopg.connect(_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        anio,
                        prod_pet_acum,
                        pozos_activos,
                        ratio_rentabilidad
                    FROM vw_rentabilidad_cuenca_anual
                    WHERE LOWER(cuenca) = LOWER(%s)
                    ORDER BY anio;
                    """,
                    (cuenca,),
                )
                rent_rows = cur.fetchall()

                cur.execute(
                    """
                    SELECT
                        anio,
                        AVG(water_cut_pct) AS water_cut_promedio
                    FROM vw_water_cut_mensual_pozo
                    WHERE LOWER(cuenca) = LOWER(%s)
                    GROUP BY anio
                    ORDER BY anio;
                    """,
                    (cuenca,),
                )
                water_cut_rows = cur.fetchall()

                cur.execute(
                    """
                    SELECT
                        anio,
                        mes,
                        yacimiento,
                        iny_agua_total,
                        prod_pet_total,
                        ratio_inyeccion_vs_petroleo
                    FROM vw_recuperacion_secundaria_mensual
                    WHERE LOWER(cuenca) = LOWER(%s)
                    ORDER BY anio, mes;
                    """,
                    (cuenca,),
                )
                rec_rows = cur.fetchall()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Error consultando metricas: {exc}") from exc

    if not rent_rows and not water_cut_rows and not rec_rows:
        raise HTTPException(
            status_code=404, detail=f"No hay metricas para cuenca '{cuenca}'."
        )

    return {
        "cuenca": cuenca,
        "rentabilidad_anual": [
            {
                "anio": int(r[0]),
                "prod_pet_acum": _to_float(r[1]),
                "pozos_activos": int(r[2]),
                "ratio_rentabilidad": _to_float(r[3]),
            }
            for r in rent_rows
        ],
        "water_cut_anual": [
            {"anio": int(r[0]), "water_cut_promedio": _to_float(r[1])}
            for r in water_cut_rows
        ],
        "recuperacion_secundaria_mensual": [
            {
                "anio": int(r[0]),
                "mes": int(r[1]),
                "yacimiento": r[2],
                "iny_agua_total": _to_float(r[3]),
                "prod_pet_total": _to_float(r[4]),
                "ratio_inyeccion_vs_petroleo": _to_float(r[5]),
            }
            for r in rec_rows
        ],
    }


def _build_feature_row(pozo_id: int) -> tuple[pd.DataFrame, dict[str, Any]]:
    with psycopg.connect(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id_pozo, profundidad, tipo_reservorio
                FROM dim_pozo
                WHERE id_pozo = %s;
                """,
                (pozo_id,),
            )
            pozo = cur.fetchone()
            if not pozo:
                raise HTTPException(status_code=404, detail=f"Pozo {pozo_id} no existe.")

            cur.execute(
                """
                SELECT fecha, prod_pet
                FROM fact_produccion
                WHERE id_pozo = %s
                ORDER BY fecha DESC
                LIMIT 6;
                """,
                (pozo_id,),
            )
            rows = cur.fetchall()
            if len(rows) < 3:
                raise HTTPException(
                    status_code=422,
                    detail=f"Pozo {pozo_id} sin historia minima (>=3 meses) para prediccion.",
                )

            cur.execute(
                """
                SELECT MIN(fecha), MAX(fecha)
                FROM fact_produccion
                WHERE id_pozo = %s;
                """,
                (pozo_id,),
            )
            first_last = cur.fetchone()

    history = list(reversed(rows))
    series = [float(r[1]) for r in history]
    first_date = first_last[0]
    last_date = first_last[1]

    if not first_date or not last_date:
        raise HTTPException(status_code=422, detail=f"Pozo {pozo_id} sin fechas validas.")

    next_month_ts = pd.Timestamp(last_date) + pd.offsets.MonthBegin(1)
    age_months = (
        (next_month_ts.year - first_date.year) * 12
        + (next_month_ts.month - first_date.month)
    )

    lag_1 = series[-1] if len(series) >= 1 else 0.0
    lag_3 = series[-3] if len(series) >= 3 else 0.0
    lag_6 = series[-6] if len(series) >= 6 else 0.0
    roll_window = series[-6:]
    roll_mean = float(pd.Series(roll_window).mean())
    roll_std = float(pd.Series(roll_window).std(ddof=1)) if len(roll_window) >= 3 else 0.0
    if pd.isna(roll_std):
        roll_std = 0.0

    feature_values: dict[str, Any] = {
        "profundidad": _to_float(pozo[1]),
        "edad_pozo_meses": int(age_months),
        "target_lag_1": lag_1,
        "target_lag_3": lag_3,
        "target_lag_6": lag_6,
        "target_roll_mean_6": roll_mean,
        "target_roll_std_6": roll_std,
    }

    tipo_reservorio = str(pozo[2] or "NO_INFORMADO").upper()
    for name in model_features:
        if name.startswith("res_"):
            feature_values[name] = 1 if name == f"res_{tipo_reservorio}" else 0

    if not model_features:
        raise HTTPException(
            status_code=503,
            detail="Modelo cargado sin feature_names; reentrena/exporta con nombres de features.",
        )

    feature_values = {name: feature_values.get(name, 0) for name in model_features}
    feature_df = pd.DataFrame([feature_values], columns=model_features)
    metadata = {
        "id_pozo": pozo_id,
        "fecha_ultima_observada": last_date.isoformat(),
        "fecha_prediccion": next_month_ts.date().isoformat(),
        "tipo_reservorio": pozo[2],
        "profundidad": _to_float(pozo[1]),
        "historia_meses_utilizada": len(series),
    }
    return feature_df, metadata


@app.get("/predict/{pozo_id}")
def predict_well(pozo_id: int) -> dict[str, Any]:
    if model is None:
        raise HTTPException(
            status_code=503,
            detail="Modelo no cargado. Verifica MODEL_PATH y reinicia api_service.",
        )

    try:
        feature_df, metadata = _build_feature_row(pozo_id)
        pred = float(model.predict(feature_df)[0])
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error de prediccion: {exc}") from exc

    return {
        "pozo_id": pozo_id,
        "fecha_prediccion": metadata["fecha_prediccion"],
        "pred_prod_pet_bbl": round(max(pred, 0.0), 4),
        "metadata": metadata,
        "features": feature_df.to_dict(orient="records")[0],
    }
