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


def wait_for_db(retries: int = 30, delay_seconds: int = 2) -> None:
    for _ in range(retries):
        try:
            with psycopg.connect(_dsn()):
                return
        except Exception:
            time.sleep(delay_seconds)
    raise RuntimeError("No se pudo conectar a Postgres a tiempo")


def get_connection():
    return psycopg.connect(_dsn())