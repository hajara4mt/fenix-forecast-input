# app/services/forecast_runner_service.py

from datetime import date
from typing import Dict, Any

# ⚠️ import depuis ton projet algo_prediction
from algo_prediction.algo_services.run_algo_services import run_building_and_persist


def run_forecast_and_return(
    building_id: str,
    start_date_ref: date,
    end_date_ref: date,
    start_date_pred: date,
    end_date_pred: date,
) -> Dict[str, Any]:
    """
    Service métier appelé par l'API.
    Lance l'algo, écrit dans ADLS, renvoie le JSON.
    """
    result = run_building_and_persist(
        building_id=building_id,
        start_ref=start_date_ref,
        end_ref=end_date_ref,
        start_pred=start_date_pred,
        end_pred=end_date_pred,
        month_str_max="2025-11",  # ta règle métier
    )

    return result
