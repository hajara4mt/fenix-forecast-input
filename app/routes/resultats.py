# app/routes/resultats.py

from __future__ import annotations

import math
import os
import traceback
from typing import Any, Dict, List

import pandas as pd
import requests
from fastapi import APIRouter, HTTPException

from app.models import (
    ForecastRequest,
    ForecastResponse,
    BuildingForecastBlock,
    ACoefficient,
    ModelCoefficients,
    MonthlyPredictiveConsumption,
    DeliverypointForecastBlock,
)

from app.routes.building import building_exists_in_silver, load_building_silver
from app.routes.deliverypoint import load_deliverypoint_silver

RUN_ALGO_URL = os.environ.get("RUN_ALGO_URL")

router = APIRouter(prefix="/forecast", tags=["forecast"])


def _float_or_zero(val) -> float:
    return float(val) if pd.notna(val) else 0.0


def _int_or_zero(val) -> int:
    return int(val) if pd.notna(val) else 0


def _safe_str(v) -> str:
    return "" if v is None else str(v)


def _sanitize_json(obj: Any) -> Any:
    """Remplace NaN/inf par None pour éviter json.dumps failure."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _sanitize_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_json(v) for v in obj]
    return obj


def _float_default(d: dict | None, key: str, default: float = 0.0) -> float:
    """Récupère une valeur flottante dans un dict, avec défaut raisonnable."""
    if not d:
        return default
    val = d.get(key, default)
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


@router.post("/resultat", response_model=ForecastResponse)
def forecast_resultat(payload: ForecastRequest):
    building_id = payload.id_building_primaire

    # 1) building existe ?
    if not building_exists_in_silver(building_id):
        raise HTTPException(
            status_code=404,
            detail=f"Building {building_id} introuvable en silver.",
        )

    # 1.b) infos building
    df_build = load_building_silver()
    row_b = df_build[df_build["id_building_primaire"] == building_id]
    if row_b.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Building {building_id} introuvable en silver (lecture).",
        )
    row_b = row_b.iloc[0]

    surface = _float_or_zero(row_b.get("surface"))
    occupant = _float_or_zero(row_b.get("occupant"))

    building_block = BuildingForecastBlock(
        weather_station=row_b.get("weather_station"),
        surface=surface,
        occupant=_int_or_zero(row_b.get("occupant")),
        total_energy_annual_consumption_reference=0.0,
        ratio_kwh_m2=0.0,
        ratio_kwh_occupant=0.0,
    )

    # 2) deliverypoints du building (pour construire le squelette)
    df_dp = load_deliverypoint_silver()
    if df_dp.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucun deliverypoint enregistré (silver vide).",
        )

    needed_dp = {"id_building_primaire", "deliverypoint_id_primaire"}
    if not needed_dp.issubset(df_dp.columns):
        raise HTTPException(
            status_code=500,
            detail="Colonnes manquantes dans silver/deliverypoint.",
        )

    df_dp_b = df_dp[df_dp["id_building_primaire"] == building_id].copy()
    if df_dp_b.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucun deliverypoint associé à ce building.",
        )

    dp_ids = df_dp_b["deliverypoint_id_primaire"].astype(str).unique().tolist()

    # 3) appeler l'algo via la Function App HTTP
    if not RUN_ALGO_URL:
        raise HTTPException(
            status_code=500,
            detail="RUN_ALGO_URL non configurée dans les variables d'environnement.",
        )

    try:
        func_payload = {
            "id_building_primaire": building_id,
            "start_date_ref": payload.start_date_ref.isoformat(),
            "end_date_ref": payload.end_date_ref.isoformat(),
            "start_date_pred": payload.start_date_pred.isoformat(),
            "end_date_pred": payload.end_date_pred.isoformat(),
        }

        resp = requests.post(RUN_ALGO_URL, json=func_payload, timeout=300)
        resp.raise_for_status()

        # La Function renvoie {"duration_ms": ..., "result": {...}}
        body = resp.json()
        out = body.get("result", body)

        print(">>> OUT KEYS:", out.keys())
        print(">>> len(out['outliers_details']) =", len(out.get("outliers_details") or []))
        print(">>> len(out['outliers_notes'])   =", len(out.get("outliers_notes") or []))

        if out.get("outliers_details"):
            print(">>> SAMPLE OUTLIER:", out["outliers_details"][0])

    except requests.exceptions.HTTPError as e:
        raise HTTPException(
            status_code=502,
            detail=f"Erreur HTTP depuis la Function run_algo: {e.response.text}",
        )
    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=502,
            detail=f"Impossible d'appeler la Function run_algo: {e}",
        )
    except ValueError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Réponse JSON invalide depuis run_algo: {e}",
        )
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Algo failed (via Function): {e}",
        )

    # 4) construire DataFrames depuis out
    df_pred = pd.DataFrame(out.get("results", []))   # predictions_monthly
    df_models = pd.DataFrame(out.get("models", []))  # models
    outliers_details = out.get("outliers_details") or []
    outliers_notes = out.get("outliers_notes") or []

    if not df_pred.empty:
        df_pred["deliverypoint_id_primaire"] = df_pred["deliverypoint_id_primaire"].astype(str)
        df_pred["month_str"] = df_pred["month_str"].astype(str)

    if not df_models.empty:
        df_models["deliverypoint_id_primaire"] = df_models["deliverypoint_id_primaire"].astype(str)

    # 5) months_expected (pour missing)
    pred_start = pd.to_datetime(payload.start_date_pred)
    pred_end = pd.to_datetime(payload.end_date_pred)
    months_expected: List[str] = (
        pd.period_range(pred_start.to_period("M"), pred_end.to_period("M"), freq="M")
        .astype(str)
        .tolist()
    )

    months_missing_by_dp: Dict[str, List[str]] = {}
    deliverypoints_blocks: List[DeliverypointForecastBlock] = []

    # 7) Construire un bloc par DP
    for dp_id in dp_ids:
        df_dp_pred = (
            df_pred[df_pred["deliverypoint_id_primaire"].astype(str) == dp_id].copy()
            if not df_pred.empty
            else pd.DataFrame()
        )

        # mois présents = mois où real_consumption existe
        df_dp_real = (
            df_dp_pred[pd.notna(df_dp_pred["real_consumption"])].copy()
            if not df_dp_pred.empty and "real_consumption" in df_dp_pred.columns
            else pd.DataFrame()
        )

        months_present = (
            df_dp_real["month_str"].astype(str).unique().tolist()
            if not df_dp_real.empty
            else []
        )

        months_missing = [m for m in months_expected if m not in months_present]
        if months_missing:
            months_missing_by_dp[dp_id] = months_missing

        # Liste des entrées mensuelles = uniquement mois avec réel
        monthly_entries: List[MonthlyPredictiveConsumption] = []
        if not df_dp_real.empty:
            df_dp_real = df_dp_real.sort_values("month_str", kind="stable")
            for _, row in df_dp_real.iterrows():
                rc = row["real_consumption"]
                pc = row.get("predictive_consumption")

                monthly_entries.append(
                    MonthlyPredictiveConsumption(
                        month=str(row["month_str"]),
                        real_consumption=float(rc),
                        predictive_consumption=float(pc) if pd.notna(pc) else None,
                        confidence_lower95=float(row["confidence_lower95"])
                        if pd.notna(row.get("confidence_lower95"))
                        else None,
                        confidence_upper95=float(row["confidence_upper95"])
                        if pd.notna(row.get("confidence_upper95"))
                        else None,
                    )
                )

        # Model coeffs : 1er model trouvé pour ce DP
        model_row = None
        if not df_models.empty:
            df_m = df_models[
                df_models["deliverypoint_id_primaire"].astype(str) == dp_id
            ]
            if not df_m.empty:
                model_row = df_m.iloc[0].to_dict()

        model_coefs = ModelCoefficients(
            a_coefficient=ACoefficient(
                hdd10=_float_default(model_row, "a_hdd", 0.0),
                cdd26=_float_default(model_row, "a_cdd", 0.0),
            ),
            b_coefficient=_float_default(model_row, "b_coefficient", 0.0),
            annual_consumption_reference=_float_default(
                model_row, "annual_consumption_reference", 0.0
            ),
            annual_ghg_emissions_reference=_float_default(
                model_row, "annual_ghg_emissions_reference", 0.0
            ),
            ME=_float_default(model_row, "ME", 0.0),
            RMSE=_float_default(model_row, "RMSE", 0.0),
            MAE=_float_default(model_row, "MAE", 0.0),
            MPE=_float_default(model_row, "MPE", 0.0),
            MAPE=_float_default(model_row, "MAPE", 0.0),
            R2=_float_default(model_row, "R2", 0.0),
        )

        deliverypoints_blocks.append(
            DeliverypointForecastBlock(
                deliverypoint_id_primaire=dp_id,
                model_coefficients=model_coefs,
                predictive_consumption=monthly_entries,
            )
        )

    # 8) metrics building
    total_ref = 0.0
    if not df_models.empty and "annual_consumption_reference" in df_models.columns:
        total_ref = float(
            pd.to_numeric(
                df_models["annual_consumption_reference"],
                errors="coerce",
            )
            .fillna(0)
            .sum()
        )

    ratio_m2 = (total_ref / surface) if surface not in (0.0, None) else 0.0
    ratio_occ = (total_ref / occupant) if occupant not in (0.0, None) else 0.0

    building_block.total_energy_annual_consumption_reference = total_ref
    building_block.ratio_kwh_m2 = ratio_m2
    building_block.ratio_kwh_occupant = ratio_occ

    # 9) réponse finale
    resp = ForecastResponse(
        id_building_primaire=building_id,
        building=building_block,
        deliverypoints=deliverypoints_blocks,
        months_missing_by_deliverypoint=months_missing_by_dp or None,
        outliers_details=outliers_details or None,
        outliers_notes=outliers_notes or None,
    )

    # Pydantic v2:
    return _sanitize_json(resp.model_dump())
