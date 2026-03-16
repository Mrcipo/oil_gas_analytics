from __future__ import annotations

import pandas as pd


def validate_production_dataframe(df: pd.DataFrame) -> None:
    required_cols = {"id_pozo", "fecha", "prod_pet"}
    missing = required_cols.difference(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas requeridas para validacion: {sorted(missing)}")

    if (df["prod_pet"] < 0).any():
        raise ValueError("Se detectaron valores negativos en 'prod_pet'.")

    duplicated_mask = df.duplicated(subset=["id_pozo", "fecha"], keep=False)
    if duplicated_mask.any():
        raise ValueError("Se detectaron filas duplicadas para la clave (id_pozo, fecha).")
