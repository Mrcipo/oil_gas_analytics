#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${1:-.}"
mkdir -p "${PROJECT_ROOT}"
cd "${PROJECT_ROOT}"

echo "Creando estructura de proyecto en: $(pwd)"

mkdir -p \
  data/raw \
  data/processed \
  data/external \
  data/db_data \
  src/extraction \
  src/transformation \
  src/database \
  src/models \
  api \
  sql \
  notebooks \
  config \
  tests

touch \
  data/raw/.gitkeep \
  data/processed/.gitkeep \
  data/external/.gitkeep \
  data/db_data/.gitkeep \
  notebooks/.gitkeep \
  tests/.gitkeep \
  src/__init__.py \
  src/extraction/__init__.py \
  src/transformation/__init__.py \
  src/database/__init__.py \
  src/models/__init__.py \
  api/__init__.py

cat > requirements.txt <<'EOF'
fastapi
uvicorn[standard]
psycopg[binary]
pandas
sqlalchemy
python-dotenv
pytest
requests
EOF

cat > .gitignore <<'EOF'
__pycache__/
*.py[cod]
*.pyo
*.pyd
.pytest_cache/
.venv/
.env

data/raw/*
!data/raw/.gitkeep
data/processed/*
!data/processed/.gitkeep
data/external/*
!data/external/.gitkeep
data/db_data/*
!data/db_data/.gitkeep
EOF

cat > .dockerignore <<'EOF'
.git
.gitignore
__pycache__/
*.py[cod]
.pytest_cache/
.venv/
.env
data/db_data/
notebooks/
tests/
EOF

cat > config/.env.example <<'EOF'
POSTGRES_USER=oil_user
POSTGRES_PASSWORD=oil_pass
POSTGRES_DB=oil_gas
POSTGRES_HOST=db
POSTGRES_PORT=5432
EOF

cat > docker-compose.yml <<'EOF'
services:
  db:
    image: postgres:15-alpine
    container_name: oga_postgres
    restart: unless-stopped
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-oil_user}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-oil_pass}
      POSTGRES_DB: ${POSTGRES_DB:-oil_gas}
    ports:
      - "5432:5432"
    volumes:
      - ./data/db_data:/var/lib/postgresql/data
      - ./sql:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-oil_user} -d ${POSTGRES_DB:-oil_gas}"]
      interval: 5s
      timeout: 5s
      retries: 20

  etl_service:
    build:
      context: .
      dockerfile: src/Dockerfile
    container_name: oga_etl
    environment:
      POSTGRES_HOST: db
      POSTGRES_PORT: 5432
      POSTGRES_USER: ${POSTGRES_USER:-oil_user}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-oil_pass}
      POSTGRES_DB: ${POSTGRES_DB:-oil_gas}
    depends_on:
      db:
        condition: service_healthy
    volumes:
      - ./data:/app/data
      - ./config:/app/config
      - ./src:/app/src
    command: ["python", "-m", "src.main"]

  api_service:
    build:
      context: .
      dockerfile: api/Dockerfile
    container_name: oga_api
    environment:
      POSTGRES_HOST: db
      POSTGRES_PORT: 5432
      POSTGRES_USER: ${POSTGRES_USER:-oil_user}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-oil_pass}
      POSTGRES_DB: ${POSTGRES_DB:-oil_gas}
    depends_on:
      db:
        condition: service_healthy
    ports:
      - "8000:8000"
    volumes:
      - ./api:/app/api
      - ./config:/app/config
    command: ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
EOF

cat > src/Dockerfile <<'EOF'
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY src ./src
COPY config ./config
RUN mkdir -p data/raw data/processed data/external

CMD ["python", "-m", "src.main"]
EOF

cat > api/Dockerfile <<'EOF'
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY api ./api
COPY config ./config

EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
EOF

cat > sql/001_init.sql <<'EOF'
CREATE TABLE IF NOT EXISTS etl_runs (
  id BIGSERIAL PRIMARY KEY,
  records_loaded INTEGER NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
EOF

cat > src/extraction/extract.py <<'EOF'
import pandas as pd


def extract_sample_data() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"well_id": "W-001", "oil_bbl": 120.5, "gas_mcf": 345.0},
            {"well_id": "W-002", "oil_bbl": 98.1, "gas_mcf": 280.4},
            {"well_id": "W-003", "oil_bbl": 130.0, "gas_mcf": 360.8},
        ]
    )
EOF

cat > src/transformation/transform.py <<'EOF'
import pandas as pd


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["boe"] = out["oil_bbl"] + (out["gas_mcf"] / 6.0)
    return out
EOF

cat > src/database/connection.py <<'EOF'
import os
import time

import psycopg


def _dsn() -> str:
    host = os.getenv("POSTGRES_HOST", "db")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "oil_user")
    password = os.getenv("POSTGRES_PASSWORD", "oil_pass")
    database = os.getenv("POSTGRES_DB", "oil_gas")
    return f"host={host} port={port} user={user} password={password} dbname={database}"


def wait_for_db(max_attempts: int = 30, sleep_seconds: int = 2) -> None:
    for _ in range(max_attempts):
        try:
            with psycopg.connect(_dsn()):
                return
        except Exception:
            time.sleep(sleep_seconds)
    raise RuntimeError("No se pudo conectar a PostgreSQL dentro del tiempo esperado")


def get_connection() -> psycopg.Connection:
    return psycopg.connect(_dsn())
EOF

cat > src/database/repository.py <<'EOF'
from typing import Any

import pandas as pd
import psycopg


def init_tables(conn: psycopg.Connection[Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_runs (
                id BIGSERIAL PRIMARY KEY,
                records_loaded INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
    conn.commit()


def save_dataframe(conn: psycopg.Connection[Any], df: pd.DataFrame) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_runs (records_loaded, status)
            VALUES (%s, %s);
            """,
            (len(df), "SUCCESS"),
        )
    conn.commit()
EOF

cat > src/main.py <<'EOF'
from pathlib import Path

from src.database.connection import get_connection, wait_for_db
from src.database.repository import init_tables, save_dataframe
from src.extraction.extract import extract_sample_data
from src.transformation.transform import transform_data


def run() -> None:
    wait_for_db()
    data = extract_sample_data()
    transformed = transform_data(data)

    out = Path("data/processed/latest_run.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    transformed.to_csv(out, index=False)

    with get_connection() as conn:
        init_tables(conn)
        save_dataframe(conn, transformed)

    print(f"ETL OK. Filas: {len(transformed)} | Archivo: {out}")


if __name__ == "__main__":
    run()
EOF

cat > api/main.py <<'EOF'
import os

import psycopg
from fastapi import FastAPI, HTTPException

app = FastAPI(title="oil_gas_analytics API", version="0.1.0")


def _dsn() -> str:
    host = os.getenv("POSTGRES_HOST", "db")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "oil_user")
    password = os.getenv("POSTGRES_PASSWORD", "oil_pass")
    database = os.getenv("POSTGRES_DB", "oil_gas")
    return f"host={host} port={port} user={user} password={password} dbname={database}"


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/runs")
def list_runs(limit: int = 20) -> list[dict[str, object]]:
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
                "id": row[0],
                "records_loaded": row[1],
                "status": row[2],
                "created_at": row[3].isoformat() if row[3] else None,
            }
            for row in rows
        ]
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB no disponible: {exc}") from exc
EOF

cat > README.md <<'EOF'
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

## Endpoints API
- `GET http://localhost:8000/health`
- `GET http://localhost:8000/runs`
EOF

echo "Proyecto oil_gas_analytics inicializado correctamente."
echo "Siguiente paso: docker-compose up --build"
