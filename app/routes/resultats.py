from fastapi import APIRouter, HTTPException
import pandas as pd

from app.models import ForecastRequest, ForecastResponse, ForecastResultItem
from app.routes.building import building_exists_in_silver
from app.routes.deliverypoint import load_deliverypoint_silver
from app.routes.invoice import _load_invoice_silver_df

router = APIRouter(prefix="/forecast", tags=["forecast"])


@router.post("/resultat", response_model=ForecastResponse)
def forecast_resultat(payload: ForecastRequest):
    building_id = payload.id_building_primaire

    # 1) building existe ?
    if not building_exists_in_silver(building_id):
        raise HTTPException(status_code=404, detail=f"Building {building_id} introuvable en silver.")

    # 2) deliverypoints du building
    df_dp = load_deliverypoint_silver()
    if df_dp.empty:
        raise HTTPException(status_code=404, detail="Aucun deliverypoint enregistré (silver vide).")

    needed_dp = {"id_building_primaire", "deliverypoint_id_primaire"}
    if not needed_dp.issubset(df_dp.columns):
        raise HTTPException(status_code=500, detail="Colonnes manquantes dans silver/deliverypoint.")

    df_dp_b = df_dp[df_dp["id_building_primaire"] == building_id].copy()
    if df_dp_b.empty:
        raise HTTPException(status_code=404, detail="Aucun deliverypoint associé à ce building.")

    # 3) période préd (mois attendus)
    pred_start = pd.to_datetime(payload.start_date_pred)
    pred_end = pd.to_datetime(payload.end_date_pred)

    months_expected = (
        pd.period_range(pred_start.to_period("M"), pred_end.to_period("M"), freq="M")
        .astype(str)
        .tolist()
    )

    # 4) invoices
    df_inv = _load_invoice_silver_df()
    if df_inv.empty:
        raise HTTPException(status_code=404, detail="Aucune facture disponible en silver/invoice.")

    needed_inv = {"deliverypoint_id_primaire", "start", "value"}
    if not needed_inv.issubset(df_inv.columns):
        raise HTTPException(status_code=500, detail="Colonnes manquantes dans silver/invoice.")

    df_inv = df_inv.copy()
    df_inv["start"] = pd.to_datetime(df_inv["start"], errors="coerce")
    df_inv["value"] = pd.to_numeric(df_inv["value"], errors="coerce")

    # filtre sur période + deliverypoints du building
    dp_ids = df_dp_b["deliverypoint_id_primaire"].astype(str).unique().tolist()
    df_inv = df_inv[df_inv["deliverypoint_id_primaire"].astype(str).isin(dp_ids)].copy()
    df_inv = df_inv[(df_inv["start"] >= pred_start) & (df_inv["start"] <= pred_end)].copy()

    if df_inv.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune facture trouvée pour les deliverypoints de ce building sur la période demandée."
        )

    df_inv["month"] = df_inv["start"].dt.strftime("%Y-%m")

    # agrégation mensuelle par deliverypoint
    df_real = (
        df_inv.groupby(["deliverypoint_id_primaire", "month"], as_index=False)["value"]
        .sum()
        .rename(columns={"value": "real"})
    )

    # 5) base complète : tous dp x tous mois
    base = df_dp_b[["deliverypoint_id_primaire"]].copy()
    base["key"] = 1
    df_months = pd.DataFrame({"month": months_expected, "key": 1})

    df_out = base.merge(df_months, on="key", how="inner").drop(columns=["key"])

    # join real
    df_out = df_out.merge(df_real, on=["deliverypoint_id_primaire", "month"], how="left")
    df_out["has_invoice"] = df_out["real"].notna()
    df_out["real"] = pd.to_numeric(df_out["real"], errors="coerce").fillna(0.0)

    # predictive (pour l’instant)
    df_out["predictive"] = 0.0

    # 6) résultats UNIQUEMENT pour les mois présents
    df_present = df_out[df_out["has_invoice"]].copy()
    df_present = df_present.sort_values(["deliverypoint_id_primaire", "month"])

    results_present = [
        ForecastResultItem(
            deliverypoint_id_primaire=str(row["deliverypoint_id_primaire"]),
            month=str(row["month"]),
            real=float(row["real"]),
            predictive=float(row["predictive"]),
        )
        for _, row in df_present.iterrows()
    ]

    # 7) mois manquants par deliverypoint (optionnel)
    missing_by_dp = {}
    for dp_id, sub in df_out.groupby("deliverypoint_id_primaire"):
        months_present = sorted(sub.loc[sub["has_invoice"], "month"].astype(str).unique().tolist())
        months_missing = [m for m in months_expected if m not in months_present]
        if months_missing:
            missing_by_dp[str(dp_id)] = months_missing

    return ForecastResponse(
        id_building_primaire=building_id,
        results=results_present,
        months_missing_by_deliverypoint=(missing_by_dp if missing_by_dp else None),
    )
