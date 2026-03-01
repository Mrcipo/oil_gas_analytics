import pandas as pd


def extract_sample_data() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"well_id": "WELL-001", "production_bbl": 1200.5},
            {"well_id": "WELL-002", "production_bbl": 980.1},
        ]
    )