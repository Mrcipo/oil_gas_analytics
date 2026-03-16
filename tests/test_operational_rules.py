from __future__ import annotations

import pandas as pd

from src.domain.operational_rules import (
    apply_operational_forecast_rules,
    derive_operational_state,
)


def test_well_is_marked_inactive_when_recent_production_is_zero() -> None:
    estado = derive_operational_state(
        prod_pet=0.0,
        target_lag_1=0.0,
        target_lag_3=0.0,
        streak_ceros=3,
    )

    assert estado == "Inactivo"


def test_inactive_well_hides_final_forecast() -> None:
    result = apply_operational_forecast_rules(
        pred_prod_pet_modelo=25.0,
        estado_operativo="Inactivo",
    )

    assert pd.isna(result["pred_prod_pet_final"])
    assert result["prediccion_confiable"] is False


def test_active_well_keeps_model_forecast() -> None:
    result = apply_operational_forecast_rules(
        pred_prod_pet_modelo=35.0,
        estado_operativo="Activo",
    )

    assert result["pred_prod_pet_final"] == 35.0
    assert result["prediccion_confiable"] is True
