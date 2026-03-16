# Oil_gas_analytics

Proyecto end-to-end de Data Engineering, Analytics y Forecasting para produccion de petroleo y gas en Argentina, completamente dockerizado.

Combina:
- pipeline ETL reproducible
- data warehouse en PostgreSQL con esquema estrella
- metricas de negocio para BI
- modelo de forecasting por pozo con XGBoost
- API en FastAPI
- dashboard operativo en Streamlit
- notebooks analiticos y de modelado

## Objetivo

Construir una solucion portfolio-ready para monitoreo y analisis de produccion por pozo, con foco en:
- ingesta y normalizacion de datos publicos de energia
- modelado analitico en estrella
- explotacion via vistas SQL
- forecasting condicional por estado operativo del pozo
- exposicion de resultados por API y dashboard

## Arquitectura

Flujo principal:
1. ETL por anio desde el portal de datos de Energia.
2. Limpieza y estandarizacion en Python.
3. Carga incremental en PostgreSQL.
4. Modelado en esquema estrella.
5. Exposicion de vistas de negocio.
6. Scoring de forecasting por pozo.
7. Consumo desde FastAPI, Streamlit y notebooks.

Servicios Docker:
- `db`: PostgreSQL 15 en `localhost:5437`
- `api_service`: API FastAPI en `localhost:8000`
- `notebook`: JupyterLab en `localhost:8888`
- `streamlit`: dashboard operativo en `localhost:8501`
- `etl_service`: pipeline ETL bajo perfil `etl` para ejecucion explicita

## Stack Tecnologico

- `Python`
- `pandas`
- `SQLAlchemy`
- `psycopg`
- `PostgreSQL`
- `FastAPI`
- `Streamlit`
- `XGBoost`
- `scikit-learn`
- `pytest`
- `Docker Compose`
- `JupyterLab`

## Estructura del Proyecto

- `api/`: backend FastAPI
- `config/`: variables de entorno
- `data/`: datos raw, processed y external
- `docker/`: Dockerfiles auxiliares
- `notebooks/`: analisis, dashboard ejecutivo y forecasting
- `sql/`: inicializacion DDL y vistas de negocio
- `src/`: ETL, features, dominio y validaciones
- `tests/`: tests unitarios con pytest
- `app.py`: dashboard Streamlit
- `docker-compose.yml`: orquestacion del stack

## Puesta en Marcha

1. Crear el archivo de entorno:

```bash
cp config/.env.example config/.env
```

2. Levantar servicios principales:

```bash
docker compose up -d --build db api_service notebook streamlit
```

Servicios disponibles:
- API: `http://localhost:8000`
- Streamlit: `http://localhost:8501`
- JupyterLab: `http://localhost:8888`
- PostgreSQL: `localhost:5437`

## ETL

El ETL no arranca por defecto. Esta configurado bajo `profile` para evitar recargas accidentales del pipeline.

Ejecutar ETL manualmente:

```bash
docker compose --profile etl up etl_service
```

Ejecutar ETL en background:

```bash
docker compose --profile etl up -d etl_service
```

## Modelo de Datos

El proyecto usa un esquema estrella en PostgreSQL con:
- `fact_produccion`
- `dim_empresa`
- `dim_geografia`
- `dim_pozo`
- `dim_tiempo`

Esto permite analisis por:
- cuenca
- provincia
- yacimiento
- empresa
- pozo
- fecha

## Vistas de Negocio

El archivo `sql/business_metrics.sql` crea vistas para consumo analitico y dashboards.

Principales vistas:
- `vw_rentabilidad_cuenca_anual`
- `vw_uptime_mensual_empresa_yacimiento`
- `vw_recuperacion_secundaria_mensual`
- `vw_water_cut_mensual_pozo`
- `vw_gor_empresa_anual`
- `vw_pareto_pozos_cuenca_detalle`
- `vw_pareto_pozos_cuenca_resumen`

## Forecasting por Pozo

El forecasting usa `XGBoost Regressor` y features temporales derivadas de produccion mensual por pozo.

Features principales:
- `target_lag_1`
- `target_lag_3`
- `target_lag_6`
- `target_roll_mean_6`
- `target_roll_std_6`
- `edad_pozo_meses`
- `streak_ceros`
- variables one-hot de reservorio

### Mejora conceptual implementada

El pipeline fue refactorizado para separar dos problemas distintos:
1. `estado_operativo`: determinar si el pozo sigue activo
2. `forecast productivo`: estimar cuanto produciria solo si sigue activo

Salidas clave:
- `pred_prod_pet_modelo`: salida cruda del modelo
- `estado_operativo`: `Activo` / `Inactivo`
- `pred_prod_pet_final`: forecast final usable en UI
- `prediccion_confiable`: bandera de confiabilidad

Regla operativa actual:
- si `prod_pet == 0`
- y `target_lag_1 == 0`
- y `target_lag_3 == 0`
entonces el pozo se considera `Inactivo`

En esos casos:
- el forecast queda oculto en UI
- la linea predicha no se dibuja en el grafico
- se informa el motivo al usuario

## Dashboard Streamlit

El dashboard en `app.py` permite:
- visualizar KPIs operativos
- explorar inventario de pozos
- filtrar por cuenca y empresa
- revisar estado operativo del pozo
- ver forecast solo cuando aplica
- analizar el detalle de un pozo en un grafico Real vs Predicha

Incluye:
- parametros economicos en sidebar
- estado de prediccion
- estado operativo
- motivo del forecast
- logica para ocultar predicciones no confiables

## API FastAPI

Endpoints disponibles:
- `GET /health`
- `GET /runs`
- `GET /metrics/{cuenca}`
- `GET /predict/{pozo_id}`

Ejemplos:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/metrics/Neuquina
curl http://localhost:8000/predict/132
```

## Notebooks

Notebooks principales:
- `notebooks/02_executive_dashboard.ipynb`
- `notebooks/03_forecasting_declinacion.ipynb`

Cubren:
- metricas ejecutivas
- visualizacion analitica
- feature engineering temporal
- entrenamiento y validacion del modelo
- analisis de importancia de variables

## Tests

El proyecto ya tiene una primera suite unitaria con `pytest`, enfocada en logica critica y reusable.

Archivos:
- `tests/test_temporal_features.py`
- `tests/test_operational_rules.py`
- `tests/test_data_quality.py`

Cobertura inicial:
- generacion correcta de lags
- no mezclar series entre pozos
- calculo de `streak_ceros`
- regla de pozo inactivo
- forecast condicional segun estado operativo
- validacion de negativos en `prod_pet`
- validacion de duplicados por `(id_pozo, fecha)`

Ejecutar tests:

```bash
docker compose exec -T streamlit python -m pytest /workspace/tests -v
```

## Calidad de Datos

Se agrego una validacion reusable para datasets productivos:
- rechazo de `prod_pet < 0`
- rechazo de duplicados en `(id_pozo, fecha)`

Modulo:
- `src/validation/data_quality.py`

## Hallazgos de Negocio

Hallazgos obtenidos hasta ahora:
- la produccion esta concentrada en un subconjunto reducido de pozos
- los lags recientes explican gran parte de la dinamica productiva
- el `water cut` es util para detectar activos maduros
- el `uptime` permite monitorear eficiencia operativa
- separar actividad operativa de forecast productivo mejora consistencia de negocio

## Estado Actual

El proyecto ya permite:
- levantar base y servicios con Docker
- correr ETL manualmente
- consultar metricas via API
- monitorear pozos desde Streamlit
- ejecutar tests unitarios
- trabajar notebooks sobre el mismo entorno reproducible

## Proximos Pasos

- reentrenar el modelo incorporando explicitamente `streak_ceros` y `estado_operativo`
- agregar CI para tests automaticos
- ampliar cobertura de tests a ETL y SQL
- versionar modelos de forecasting
- mejorar performance del dashboard con mas preagregaciones
