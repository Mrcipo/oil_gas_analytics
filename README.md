# oil_gas_analytics

Pipeline end-to-end de Data Engineering + Data Science para producción de petróleo y gas en Argentina, 100% dockerizado.

## 1. Arquitectura

Flujo de datos:
1. Ingesta ETL por año desde el portal oficial de Energía (CKAN/API + CSV).
2. Estandarización y carga incremental en PostgreSQL.
3. Modelado analítico en esquema estrella (`fact_produccion` + dimensiones).
4. Exposición de métricas y predicciones vía FastAPI.
5. Exploración y reporting ejecutivo en notebooks.

Servicios Docker:
- `db`: PostgreSQL 15 (`localhost:5437` para DBeaver/pgAdmin).
- `etl_service`: extracción, limpieza y carga de datos.
- `api_service`: API de métricas y scoring de pozos.
- `notebook`: JupyterLab para análisis y modelado.

## 2. Stack Tecnológico

- `Python`: pandas, SQLAlchemy, psycopg, scikit-learn, XGBoost.
- `PostgreSQL`: data warehouse en esquema estrella + vistas de negocio.
- `FastAPI`: endpoints de consumo para BI y aplicaciones.
- `Docker Compose`: orquestación reproducible sin dependencias locales.
- `Jupyter`: análisis exploratorio, dashboard ejecutivo y forecasting.

## 3. Estructura del Proyecto

- `data/`: `raw`, `processed`, `external`.
- `src/`: ETL, capa de datos, feature engineering y utilidades.
- `sql/`: scripts DDL de inicialización y métricas de negocio.
- `api/`: backend FastAPI.
- `notebooks/`: dashboards y entrenamiento de modelos.
- `config/`: variables de entorno (`.env.example` y `.env` local).
- `tests/`: pruebas.

Para replicar la estructura base:
```bash
./setup_project.sh
```

## 4. Puesta en Marcha

1. Crear `config/.env` a partir de `config/.env.example`.
2. Levantar stack completo:
```bash
docker compose up --build
```

Servicios:
- API: `http://localhost:8000`
- JupyterLab: `http://localhost:8888`
- PostgreSQL host: `localhost:5437`

## 5. Endpoints API

- `GET /health`: estado de la API y del modelo.
- `GET /runs?limit=20`: últimas ejecuciones ETL.
- `GET /metrics/{cuenca}`: métricas de negocio por cuenca desde PostgreSQL.
- `GET /predict/{pozo_id}`: predicción de producción de petróleo del próximo mes para un pozo.

Ejemplos:
```bash
curl http://localhost:8000/metrics/Neuquina
curl http://localhost:8000/predict/132
```

## 6. Modelo de ML

- Algoritmo: `XGBoost Regressor`.
- Features: lags (`t-1`, `t-3`, `t-6`), rolling stats 6 meses, edad del pozo, variables estáticas de reservorio.
- Artefacto: `notebooks/models/xgboost_declinacion_v1.json`.
- El contenedor `api_service` monta el modelo en `/app/models`.

## 7. Hallazgos de Negocio (Resumen)

- La productividad está concentrada en un subconjunto reducido de pozos (patrón Pareto por cuenca).
- `lag_1`, `lag_3`, `lag_6` y la edad del pozo son variables con alto poder explicativo de la declinación.
- El `water cut` creciente identifica activos maduros con posible deterioro de performance.
- El `uptime` mensual permite detectar ineficiencias operativas por empresa y yacimiento.
- La relación inyección/producción aporta señal para evaluar respuesta de recuperación secundaria.

## 8. Roadmap Técnico

- Versionado de modelos (MLflow o registry simple por versión).
- Pruebas automatizadas de calidad de datos (Great Expectations).
- Serving de predicción batch/online con caché de features por pozo.
- CI/CD para build, tests, linters y despliegue.
