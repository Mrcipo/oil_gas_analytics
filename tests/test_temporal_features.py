from __future__ import annotations

import pandas as pd

from src.features.temporal_features import build_feature_dataset, compute_zero_streak


def _base_feature_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id_pozo": [1, 1, 1, 1],
            "fecha": pd.to_datetime(
                ["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"]
            ),
            "target": [10.0, 20.0, 30.0, 40.0],
            "profundidad": [2500.0, 2500.0, 2500.0, 2500.0],
            "tipo_reservorio": ["CONVENCIONAL"] * 4,
        }
    )


def test_build_feature_dataset_creates_expected_lags() -> None:
    df = _base_feature_df()

    result = build_feature_dataset(df, value_col="target", downcast=False)
    april_row = result.loc[result["fecha"] == pd.Timestamp("2024-04-01")].iloc[0]

    assert april_row["target_lag_1"] == 30.0
    assert april_row["target_lag_3"] == 10.0


def test_build_feature_dataset_does_not_mix_wells() -> None:
    df = pd.DataFrame(
        {
            "id_pozo": [1, 1, 2, 2],
            "fecha": pd.to_datetime(
                ["2024-01-01", "2024-02-01", "2024-01-01", "2024-02-01"]
            ),
            "target": [10.0, 20.0, 100.0, 200.0],
            "profundidad": [2500.0, 2500.0, 3200.0, 3200.0],
            "tipo_reservorio": ["CONVENCIONAL", "CONVENCIONAL", "NO CONVENCIONAL", "NO CONVENCIONAL"],
        }
    )

    result = build_feature_dataset(df, value_col="target", downcast=False)
    pozo_b_row = result[
        (result["id_pozo"] == 2) & (result["fecha"] == pd.Timestamp("2024-02-01"))
    ].iloc[0]

    assert pozo_b_row["target_lag_1"] == 100.0
    assert pozo_b_row["target_lag_1"] != 20.0


def test_zero_streak_counts_consecutive_zeros_correctly() -> None:
    series = pd.Series([5.0, 0.0, 0.0, 3.0, 0.0, 0.0, 0.0])

    result = compute_zero_streak(series)

    assert result.tolist() == [0, 1, 2, 0, 1, 2, 3]
