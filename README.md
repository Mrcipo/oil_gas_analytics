# oil_gas_analytics

Proyecto base de Data Engineering + ML completamente dockerizado.

## Estructura
- `data/`: `raw`, `processed`, `external`, `db_data`
- `src/`: logica ETL (`extraction`, `transformation`, `database`, `models`)
- `api/`: backend FastAPI
- `sql/`: DDL para inicializacion automatica de Postgres
- `config/`: ejemplo de variables de entorno
- `notebooks/`, `tests/`

## Arranque
```bash
docker-compose up --build
```

Requiere `config/.env` con credenciales de PostgreSQL para los servicios.

Para replicar la estructura de carpetas de este proyecto, ejecuta `./setup_project.sh` en tu terminal.

## Endpoints API
- `GET http://localhost:8000/health`
- `GET http://localhost:8000/runs`
