# app/routes/resultats.py

from fastapi import APIRouter, HTTPException
from typing import List, Dict, Optional
import pandas as pd

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
from app.routes.invoice import _load_invoice_silver_df

router = APIRouter(prefix="/forecast", tags=["forecast"])


@router.post("/resultat", response_model=ForecastResponse)
def forecast_resultat(payload: ForecastRequest):
    building_id = payload.id_building_primaire

    # 0) vérifier que la période de prédiction est dans la même année
    if payload.start_date_pred.year != payload.end_date_pred.year:
        raise HTTPException(
            status_code=400,
            detail=(
                "start_date_pred et end_date_pred doivent être dans la même année."
            ),
        )

    # 1) vérifier que le building existe en silver
    if not building_exists_in_silver(building_id):
        raise HTTPException(
            status_code=404,
            detail=f"Building {building_id} introuvable en silver.",
        )

    # 1.b) récupérer les infos du building (weather_station, surface, occupant)
    df_build = load_building_silver()
    row_b = df_build[df_build["id_building_primaire"] == building_id]
    if row_b.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Building {building_id} introuvable en silver (lecture).",
        )

    row_b = row_b.iloc[0]

    def _float_or_zero(val) -> float:
        return float(val) if pd.notna(val) else 0.0

    def _int_or_zero(val) -> int:
        return int(val) if pd.notna(val) else 0

    building_block = BuildingForecastBlock(
        weather_station=row_b.get("weather_station"),
        surface=_float_or_zero(row_b["surface"]) if "surface" in row_b else 0.0,
        occupant=_int_or_zero(row_b["occupant"]) if "occupant" in row_b else 0,
        total_energy_annual_consumption_reference=0.0,
        ratio_kwh_m2=0.0,
        ratio_kwh_occupant=0.0,
    )

    # 2) récupérer les deliverypoints de ce building
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

    # 3) période prédiction (mois attendus)
    pred_start = pd.to_datetime(payload.start_date_pred)
    pred_end = pd.to_datetime(payload.end_date_pred)

    months_expected: List[str] = (
        pd.period_range(pred_start.to_period("M"), pred_end.to_period("M"), freq="M")
        .astype(str)
        .tolist()
    )

    # 4) récupérer les invoices en silver
    df_inv = _load_invoice_silver_df()
    if df_inv.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune facture disponible en silver/invoice.",
        )

    needed_inv = {"deliverypoint_id_primaire", "start", "value"}
    if not needed_inv.issubset(df_inv.columns):
        raise HTTPException(
            status_code=500,
            detail="Colonnes manquantes dans silver/invoice.",
        )

    df_inv = df_inv.copy()
    df_inv["start"] = pd.to_datetime(df_inv["start"], errors="coerce")
    df_inv["value"] = pd.to_numeric(df_inv["value"], errors="coerce")

    # filtre sur période + deliverypoints du building
    df_inv = df_inv[
        df_inv["deliverypoint_id_primaire"].astype(str).isin(dp_ids)
    ].copy()
    df_inv = df_inv[
        (df_inv["start"] >= pred_start) & (df_inv["start"] <= pred_end)
    ].copy()

    if df_inv.empty:
        raise HTTPException(
            status_code=404,
            detail=(
                "Aucune facture trouvée pour les deliverypoints de ce building "
                "sur la période de prédiction demandée."
            ),
        )

    df_inv["month"] = df_inv["start"].dt.strftime("%Y-%m")

    # agrégation mensuelle par deliverypoint
    df_real = (
        df_inv.groupby(["deliverypoint_id_primaire", "month"], as_index=False)["value"]
        .sum()
        .rename(columns={"value": "real"})
    )

    # 5) construire les blocs deliverypoints + predictive_consumption
    deliverypoints_blocks: List[DeliverypointForecastBlock] = []

    # dictionnaire des mois manquants par deliverypoint
    months_missing_by_dp: Dict[str, List[str]] = {}

    for dp_id in dp_ids:
        df_dp_real = df_real[df_real["deliverypoint_id_primaire"].astype(str) == dp_id].copy()
        df_dp_real = df_dp_real.sort_values("month")

        # mois présents pour ce deliverypoint
        months_present = (
            df_dp_real["month"].astype(str).unique().tolist()
            if not df_dp_real.empty
            else []
        )
        months_missing = [m for m in months_expected if m not in months_present]
        if months_missing:
            months_missing_by_dp[dp_id] = months_missing

        # construire la liste des consommations mensuelles (uniquement mois présents)
        monthly_entries: List[MonthlyPredictiveConsumption] = []
        for _, row in df_dp_real.iterrows():
            monthly_entries.append(
                MonthlyPredictiveConsumption(
                    month=str(row["month"]),
                    real_consumption=float(row["real"]),    # depuis invoice
                    predictive_consumption=0.0,             # pour l'instant 0
                )
            )

        # coefficients tous à 0 pour l’instant
        model_coefs = ModelCoefficients(
            a_coefficient=ACoefficient(hdd10=0.0, cdd26=0.0),
            b_coefficient=0.0,
            annual_consumption_reference=0.0,
            annual_ghg_emissions_reference=0.0,
            ME=0.0,
            RMSE=0.0,
            MAE=0.0,
            MPE=0.0,
            MAPE=0.0,
            R2=0.0,
        )

        deliverypoints_blocks.append(
            DeliverypointForecastBlock(
                deliverypoint_id_primaire=dp_id,
                model_coefficients=model_coefs,
                predictive_consumption=monthly_entries,
            )
        )

    # 6) réponse finale
    return ForecastResponse(
        id_building_primaire=building_id,
        building=building_block,
        deliverypoints=deliverypoints_blocks,
        months_missing_by_deliverypoint=months_missing_by_dp or None,
    )
