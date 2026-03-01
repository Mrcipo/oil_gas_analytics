import pandas as pd


def init_tables(conn) -> None:
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


def save_dataframe(conn, df: pd.DataFrame) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO etl_runs (records_loaded, status) VALUES (%s, %s);",
            (len(df), "success"),
        )
    conn.commit()