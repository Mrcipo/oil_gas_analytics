from __future__ import annotations

from typing import Any

import numpy as np


def derive_operational_state(
    prod_pet: float | int | None,
    target_lag_1: float | int | None,
    target_lag_3: float | int | None,
    streak_ceros: int | None = None,
) -> str:
    prod_value = 0.0 if prod_pet is None else float(prod_pet)
    lag_1_value = 0.0 if target_lag_1 is None else float(target_lag_1)
    lag_3_value = 0.0 if target_lag_3 is None else float(target_lag_3)
    streak_value = 0 if streak_ceros is None or np.isnan(streak_ceros) else int(streak_ceros)

    # Estado operativo: separa pozos activos de pozos sin produccion reciente.
    is_inactive = (
        (prod_value == 0.0 and lag_1_value == 0.0 and lag_3_value == 0.0)
        or streak_value >= 3
    )
    return "Inactivo" if is_inactive else "Activo"


def apply_operational_forecast_rules(
    pred_prod_pet_modelo: float | int | None,
    estado_operativo: str,
) -> dict[str, Any]:
    pred_model = (
        np.nan
        if pred_prod_pet_modelo is None or np.isnan(pred_prod_pet_modelo)
        else float(pred_prod_pet_modelo)
    )
    is_active = estado_operativo == "Activo"
    pred_final = pred_model if is_active else np.nan
    prediccion_confiable = bool(is_active and not np.isnan(pred_model))

    if estado_operativo == "Inactivo":
        motivo = "Pozo sin produccion reciente. Forecast oculto por inactividad operativa."
    elif np.isnan(pred_model):
        motivo = "Sin prediccion disponible."
    else:
        motivo = "Forecast disponible para pozo activo."

    return {
        "pred_prod_pet_modelo": pred_model,
        "pred_prod_pet_final": pred_final,
        "prediccion_confiable": prediccion_confiable,
        "motivo_prediccion": motivo,
        "mostrar_prediccion_en_ui": prediccion_confiable,
    }
