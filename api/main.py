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
def health():
    return {"status": "ok"}


@app.get("/runs")
def list_runs(limit: int = 20):
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
        raise HTTPException(status_code=503, detail=f"DB no disponible: {exc}")