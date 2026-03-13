from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text
from xgboost import XGBRegressor

from src.features.temporal_features import (
    build_engine_from_env,
    build_feature_dataset,
    load_monthly_well_production,
)


st.set_page_config(page_title="Oil & Gas Monitoring", layout="wide")

st.markdown(
    """
    <style>
      .block-container {
        padding-top: 4.5rem;
        padding-bottom: 1.5rem;
      }
      .stMetric {
        background: #f3f5f7;
        border-radius: 10px;
        padding: 0.8rem;
        border: 1px solid #d3dbe3;
      }
      .stMetric label,
      .stMetric div,
      .stMetric p,
      [data-testid="stMetricValue"],
      [data-testid="stMetricLabel"] {
        color: #1f2a33 !important;
      }
      .main-title {
        font-size: 1.8rem;
        font-weight: 700;
        color: #1f2a33;
        line-height: 1.25;
        margin-top: 0.25rem;
        margin-bottom: 1rem;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown("<div class='main-title'>Oil & Gas Monitoring Dashboard (2015-2026)</div>", unsafe_allow_html=True)


VIEW_QUERIES: dict[str, str] = {
    "vw_rentabilidad_cuenca_anual": """
        SELECT anio, cuenca, prod_pet_acum, pozos_activos, ratio_rentabilidad
        FROM vw_rentabilidad_cuenca_anual
    """,
    "vw_uptime_mensual_empresa_yacimiento": """
        SELECT anio, mes, nombre_empresa, yacimiento, uptime_pct
        FROM vw_uptime_mensual_empresa_yacimiento
    """,
    "vw_recuperacion_secundaria_mensual": """
        SELECT anio, mes, cuenca, yacimiento, iny_agua_total, prod_pet_total, ratio_inyeccion_vs_petroleo
        FROM vw_recuperacion_secundaria_mensual
    """,
    "vw_water_cut_mensual_pozo": """
        SELECT anio, mes, cuenca, AVG(water_cut_pct) AS water_cut_pct_prom
        FROM vw_water_cut_mensual_pozo
        GROUP BY anio, mes, cuenca
    """,
    "vw_gor_empresa_anual": """
        SELECT anio, nombre_empresa, gor, orientacion
        FROM vw_gor_empresa_anual
    """,
    "vw_pareto_pozos_cuenca_detalle": """
        SELECT cuenca, id_pozo, nombre_pozo, prod_pet_total_pozo, pct_pozos, pct_prod_acumulada, total_pozos_muestra_cuenca
        FROM vw_pareto_pozos_cuenca_detalle
    """,
    "vw_pareto_pozos_cuenca_resumen": """
        SELECT *
        FROM vw_pareto_pozos_cuenca_resumen
    """,
}


@st.cache_resource
def get_engine():
    return build_engine_from_env()


@st.cache_resource
def get_model() -> tuple[XGBRegressor, list[str]]:
    env_model_path = os.getenv("MODEL_PATH")
    candidates: list[Path] = []
    if env_model_path:
        candidates.append(Path(env_model_path))
    candidates.extend(
        [
            Path("models/xgboost_declinacion_v1.json"),
            Path("notebooks/models/xgboost_declinacion_v1.json"),
        ]
    )
    model_path = next((p for p in candidates if p.exists() and p.is_file()), None)
    if model_path is None:
        raise FileNotFoundError("No se encontro el modelo xgboost_declinacion_v1.json")

    model = XGBRegressor()
    model.load_model(str(model_path))
    features = list(model.get_booster().feature_names or [])
    return model, features


@st.cache_data(ttl=1800, show_spinner=False)
def load_business_views() -> dict[str, pd.DataFrame]:
    engine = get_engine()
    return {name: pd.read_sql(query, engine) for name, query in VIEW_QUERIES.items()}


@st.cache_data(ttl=1800, show_spinner=False)
def load_inventory_base() -> pd.DataFrame:
    engine = get_engine()
    query = """
    WITH latest_by_well AS (
      SELECT id_pozo, MAX(fecha) AS fecha
      FROM fact_produccion
      GROUP BY id_pozo
    )
    SELECT
      f.id_pozo,
      p.nombre_pozo,
      e.nombre_empresa,
      g.cuenca,
      g.yacimiento,
      p.profundidad,
      p.tipo_reservorio,
      f.fecha,
      f.prod_pet
    FROM latest_by_well l
    JOIN fact_produccion f ON f.id_pozo = l.id_pozo AND f.fecha = l.fecha
    JOIN dim_pozo p ON p.id_pozo = f.id_pozo
    JOIN dim_empresa e ON e.id_empresa = f.id_empresa
    JOIN dim_geografia g ON g.id_geo = f.id_geo
    ORDER BY f.id_pozo;
    """
    df = pd.read_sql(query, engine)
    df["fecha"] = pd.to_datetime(df["fecha"])
    return df


@st.cache_data(ttl=1800, show_spinner=False)
def load_latest_features_and_predictions() -> pd.DataFrame:
    engine = get_engine()
    model, model_features = get_model()

    base_df = load_monthly_well_production(
        engine=engine,
        start_date="2015-01-01",
        end_date="2027-01-01",
        target_col="prod_pet",
        history_months=6,
        keep_only_window=False,
        top_n_pozos=2000,
    )
    feat_df = build_feature_dataset(base_df, value_col="target", downcast=True)
    feat_df = feat_df[feat_df["in_window"]].copy()
    feat_df = feat_df.dropna(subset=["target_lag_6", "target_roll_mean_6"]).copy()
    feat_df = feat_df.sort_values(["id_pozo", "fecha"])
    latest = feat_df.groupby("id_pozo", as_index=False).tail(1).copy()

    X = latest.reindex(columns=model_features, fill_value=0)
    latest["pred_prod_pet"] = model.predict(X).astype(float)
    return latest


def estimate_months_to_closure(prod_pred: float, threshold: float, lag1: float, lag3: float) -> float:
    if prod_pred <= threshold:
        return 0.0
    decline_rate = max((lag3 - lag1) / 2.0, 0.001)
    months = (prod_pred - threshold) / decline_rate
    return float(np.clip(months, 0, 24))


def classify_status(months_to_closure: float) -> str:
    if months_to_closure < 3:
        return "Rojo"
    if months_to_closure < 6:
        return "Amarillo"
    return "Verde"


def build_styled_inventory(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    def color_row(row: pd.Series) -> list[str]:
        color_map = {
            "Rojo": "background-color: #ffd7d7;",
            "Amarillo": "background-color: #fff3cd;",
            "Verde": "background-color: #d7f5df;",
        }
        style = color_map.get(row["Estado"], "")
        return [style if c == "Estado" else "" for c in row.index]

    return df.style.apply(color_row, axis=1)


MAX_STYLED_CELLS = 250_000


def load_well_series(pozo_id: int) -> pd.DataFrame:
    engine = get_engine()
    model, model_features = get_model()
    query = text(
        """
        SELECT
          f.id_pozo,
          f.fecha::date AS fecha,
          f.prod_pet AS target,
          p.profundidad,
          p.tipo_reservorio,
          MIN(f.fecha) OVER (PARTITION BY f.id_pozo)::date AS primera_fecha_pozo
        FROM fact_produccion f
        JOIN dim_pozo p ON p.id_pozo = f.id_pozo
        WHERE f.id_pozo = :pozo_id
          AND f.fecha >= DATE '2015-01-01'
          AND f.fecha < DATE '2027-01-01'
        ORDER BY f.fecha;
        """
    )
    well_df = pd.read_sql(query, engine, params={"pozo_id": pozo_id})
    well_df["fecha"] = pd.to_datetime(well_df["fecha"])
    well_df["primera_fecha_pozo"] = pd.to_datetime(well_df["primera_fecha_pozo"])

    feat_df = build_feature_dataset(well_df, value_col="target", downcast=False)
    X = feat_df.reindex(columns=model_features, fill_value=0)
    valid = feat_df["target_lag_6"].notna()
    feat_df["pred_target"] = np.nan
    feat_df.loc[valid, "pred_target"] = model.predict(X.loc[valid]).astype(float)
    return feat_df


# Sidebar: parametros economicos
st.sidebar.header("Parametros Economicos")
precio_bbl = st.sidebar.slider("Precio del Barril (USD)", min_value=35, max_value=130, value=75, step=1)
opex_fijo = st.sidebar.slider("OPEX Fijo Mensual (USD)", min_value=5000, max_value=50000, value=15000, step=500)

with st.spinner("Calculando predicciones y límites económicos..."):
    views = load_business_views()
    inventory_base = load_inventory_base()
    latest_pred = load_latest_features_and_predictions()

threshold_bbl = opex_fijo / max(precio_bbl, 1)

inventory = inventory_base.merge(
    latest_pred[
        ["id_pozo", "pred_prod_pet", "target_lag_1", "target_lag_3", "target_lag_6"]
    ],
    on="id_pozo",
    how="left",
)
# Mantener semantica: NaN en prediccion = "sin prediccion", no convertir a cero.
inventory["pred_prod_pet_raw"] = pd.to_numeric(inventory["pred_prod_pet"], errors="coerce")
inventory["estado_prediccion"] = np.where(
    inventory["pred_prod_pet_raw"].notna(),
    "Prediccion disponible",
    "Sin prediccion disponible",
)
inventory["pred_prod_pet_display"] = np.where(
    inventory["pred_prod_pet_raw"].notna(),
    inventory["pred_prod_pet_raw"].round(4).astype(str),
    "Sin prediccion",
)

mask_pred = inventory["pred_prod_pet_raw"].notna()

# Regla de negocio: pozo sin produccion reciente -> prediccion no confiable en UI.
sin_produccion_reciente = (
    inventory["prod_pet"].fillna(0).eq(0)
    & inventory["target_lag_1"].fillna(0).eq(0)
    & inventory["target_lag_3"].fillna(0).eq(0)
)
inventory["prediccion_confiable"] = np.where(mask_pred & sin_produccion_reciente, False, True)
inventory["motivo_prediccion"] = np.where(
    mask_pred & sin_produccion_reciente,
    "Pozo sin produccion reciente. Prediccion no confiable.",
    "",
)
inventory["mostrar_prediccion_en_ui"] = mask_pred & inventory["prediccion_confiable"]

inventory["margen_estimado_usd"] = np.where(
    mask_pred,
    inventory["pred_prod_pet_raw"] * precio_bbl - opex_fijo,
    np.nan,
)

# Solo calcular meses al cierre cuando existe prediccion.
inventory["meses_al_cierre"] = np.nan
inventory.loc[mask_pred, "meses_al_cierre"] = inventory.loc[mask_pred].apply(
    lambda r: estimate_months_to_closure(
        prod_pred=float(r["pred_prod_pet_raw"]),
        threshold=threshold_bbl,
        lag1=float(r.get("target_lag_1", 0) or 0),
        lag3=float(r.get("target_lag_3", 0) or 0),
    ),
    axis=1,
)
inventory["Estado"] = np.where(
    mask_pred,
    inventory["meses_al_cierre"].map(classify_status),
    "Sin prediccion",
)

# KPIs
kpi_col1, kpi_col2, kpi_col3 = st.columns(3)

latest_month = inventory["fecha"].max()
prod_total_mensual = inventory.loc[inventory["fecha"] == latest_month, "prod_pet"].sum()

uptime_df = views["vw_uptime_mensual_empresa_yacimiento"].copy()
uptime_latest = uptime_df[uptime_df["anio"] * 100 + uptime_df["mes"] == (uptime_df["anio"] * 100 + uptime_df["mes"]).max()]
uptime_promedio = float(uptime_latest["uptime_pct"].mean()) if not uptime_latest.empty else 0.0

pozos_alerta = int(
    ((inventory["pred_prod_pet_raw"] * precio_bbl < opex_fijo) & mask_pred).sum()
)

kpi_col1.metric("Produccion Total Mensual", f"{prod_total_mensual:,.0f} bbl")
kpi_col2.metric("Uptime Promedio Sistema", f"{uptime_promedio:,.2f}%")
kpi_col3.metric("Pozos en Alerta", f"{pozos_alerta:,}")

st.divider()

# Inventario de pozos
st.subheader("Inventario de Pozos")
flt_col1, flt_col2 = st.columns(2)
cuencas = ["Todas"] + sorted(inventory["cuenca"].dropna().unique().tolist())
empresas = ["Todas"] + sorted(inventory["nombre_empresa"].dropna().unique().tolist())
cuenca_sel = flt_col1.selectbox("Filtrar por Cuenca", cuencas)
empresa_sel = flt_col2.selectbox("Filtrar por Empresa", empresas)

inv_view = inventory.copy()
if cuenca_sel != "Todas":
    inv_view = inv_view[inv_view["cuenca"] == cuenca_sel]
if empresa_sel != "Todas":
    inv_view = inv_view[inv_view["nombre_empresa"] == empresa_sel]

inv_show = inv_view[
    [
        "id_pozo",
        "nombre_pozo",
        "nombre_empresa",
        "cuenca",
        "yacimiento",
        "prod_pet",
        "pred_prod_pet_display",
        "estado_prediccion",
        "prediccion_confiable",
        "margen_estimado_usd",
        "meses_al_cierre",
        "Estado",
    ]
].sort_values(["Estado", "margen_estimado_usd"])

inv_show = inv_show.rename(
    columns={
        "pred_prod_pet_display": "pred_prod_pet",
    }
)

rows_col1, rows_col2 = st.columns([1, 3])
rows_opt = rows_col1.selectbox("Filas a mostrar", [500, 1000, 5000, 10000, "Todas"], index=2)
inv_show_display = inv_show if rows_opt == "Todas" else inv_show.head(int(rows_opt))
st.dataframe(
    inv_show_display,
    use_container_width=True,
    height=420,
)

st.divider()

# Deep dive pozo
st.subheader("Analisis Detallado por Pozo")
pozo_options = inv_view["id_pozo"].dropna().astype(int).unique().tolist()
if not pozo_options:
    st.info("No hay pozos para los filtros seleccionados.")
    st.stop()

pozo_id_sel = st.selectbox("Seleccionar id_pozo", sorted(pozo_options))
pozo_info = inventory[inventory["id_pozo"] == pozo_id_sel].iloc[0]

tech_col1, tech_col2, tech_col3 = st.columns(3)
tech_col1.metric("Profundidad", f"{float(pozo_info['profundidad'] or 0):,.0f} m")
tech_col2.metric("Reservorio", str(pozo_info["tipo_reservorio"]))
tech_col3.metric("Yacimiento", str(pozo_info["yacimiento"]))

st.write(f"**Estado de prediccion:** {pozo_info['estado_prediccion']}")
if bool(pozo_info.get("prediccion_confiable", True)) is False:
    st.warning("Pozo sin produccion reciente. Prediccion no confiable.")

well_series = load_well_series(int(pozo_id_sel)).sort_values("fecha")
plot_df = well_series[["fecha", "target", "pred_target"]].copy().tail(12)

# Fuente de verdad semantica: estado de prediccion del inventario.
estado_pred = str(pozo_info["estado_prediccion"])
mostrar_pred_ui = bool(pozo_info.get("mostrar_prediccion_en_ui", False))
if estado_pred == "Sin prediccion disponible" or not mostrar_pred_ui:
    # Evita que residuos de pred_target dibujen una serie no valida.
    plot_df["pred_target"] = np.nan

has_real = plot_df["target"].notna().any()
has_pred = (
    plot_df["pred_target"].notna().any()
    and estado_pred == "Prediccion disponible"
    and mostrar_pred_ui
)

show_chart = True
if not has_real and not has_pred:
    st.info("No hay datos suficientes para mostrar el grafico de este pozo.")
    show_chart = False

if not has_real and has_pred:
    st.warning(
        "Inconsistencia detectada: hay prediccion pero no hay datos reales. "
        "No se muestra grafico para este pozo."
    )
    show_chart = False

if show_chart:
    fig = go.Figure()
    if has_real:
        fig.add_trace(
            go.Scatter(
                x=plot_df["fecha"],
                y=plot_df["target"],
                mode="lines+markers",
                name="Real",
                line=dict(color="#1f77b4", width=3),
            )
        )

    if has_pred:
        fig.add_trace(
            go.Scatter(
                x=plot_df["fecha"],
                y=plot_df["pred_target"],
                mode="lines+markers",
                name="Predicha",
                line=dict(color="#ff7f0e", width=3, dash="dash"),
            )
        )
    else:
        st.info("Sin prediccion disponible para este pozo. Se muestra solo serie real.")

    fig.update_layout(
        title=f"Curva de Declinacion Real vs Predicha - Pozo {pozo_id_sel} (ultimos 12 meses)",
        xaxis_title="Fecha",
        yaxis_title="Produccion de petroleo (bbl)",
        template="plotly_white",
        height=470,
        legend=dict(orientation="h", y=1.05, x=0.01),
        margin=dict(l=30, r=30, t=70, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)

st.caption(
    "Datos de negocio consultados desde las 7 vistas SQL del esquema estrella. "
    "Filtro temporal operativo: 2015-2026."
)
