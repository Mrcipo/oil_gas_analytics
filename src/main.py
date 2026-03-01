from pathlib import Path

from src.extraction.extract import extract_sample_data
from src.transformation.transform import transform_data
from src.database.connection import get_connection, wait_for_db
from src.database.repository import init_tables, save_dataframe


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