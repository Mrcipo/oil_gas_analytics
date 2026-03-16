from __future__ import annotations

import pandas as pd
import pytest

from src.validation.data_quality import validate_production_dataframe


def test_production_dataframe_rejects_negative_prod_pet() -> None:
    df = pd.DataFrame(
        {
            "id_pozo": [1, 2],
            "fecha": pd.to_datetime(["2024-01-01", "2024-01-01"]),
            "prod_pet": [10.0, -1.0],
        }
    )

    with pytest.raises(ValueError, match="negativos en 'prod_pet'"):
        validate_production_dataframe(df)


def test_production_dataframe_rejects_duplicate_well_date_rows() -> None:
    df = pd.DataFrame(
        {
            "id_pozo": [1, 1],
            "fecha": pd.to_datetime(["2024-01-01", "2024-01-01"]),
            "prod_pet": [10.0, 12.0],
        }
    )

    with pytest.raises(ValueError, match="duplicadas para la clave"):
        validate_production_dataframe(df)
