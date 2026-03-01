from datetime import datetime, timezone
import pandas as pd


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["processed_at"] = datetime.now(timezone.utc).isoformat()
    return out