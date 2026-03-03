import os

from src.database.connection import wait_for_db
from src.extraction.extract_data import run_extraction_and_load


def ensure_database_url() -> None:
    if os.getenv("DATABASE_URL"):
        return

    host = os.getenv("POSTGRES_HOST", "db")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    database = os.getenv("POSTGRES_DB")

    if user and password and database:
        os.environ["DATABASE_URL"] = (
            f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
        )


def run() -> None:
    wait_for_db()
    ensure_database_url()
    total_rows = run_extraction_and_load()
    print(f"ETL finalizado. Filas cargadas: {total_rows}")


if __name__ == "__main__":
    run()
