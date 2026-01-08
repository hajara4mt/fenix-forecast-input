# app/routes/invoice.py

import io
import re
from uuid import uuid4
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import pandas as pd
from fastapi import APIRouter, HTTPException, status, BackgroundTasks

from app.models import InvoiceCreate, InvoiceRead, InvoiceBatchCreate
from app.azure_datalake import write_json_to_bronze, get_datalake_client, delete_file_from_bronze
from config import AZURE_STORAGE_FILESYSTEM
from app.jobs.invoice_silver import run_invoice_silver_job
from app.routes.deliverypoint import deliverypoint_exists_in_silver

SILVER_INVOICE_PATH = "silver/invoice/invoice.parquet"

router = APIRouter(prefix="/invoice", tags=["invoice"])


def _load_invoice_silver_df() -> pd.DataFrame:
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    try:
        file_client = fs.get_file_client(SILVER_INVOICE_PATH)
        raw_bytes = file_client.download_file().readall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Impossible de lire les factures en silver : {e}")

    if not raw_bytes:
        return pd.DataFrame()

    return pd.read_parquet(io.BytesIO(raw_bytes))


def parse_deliverypoint_id(dp_id: str) -> Tuple[str, str]:
    """
    deliverypoint_000_001 -> ("000", "001")
    """
    m = re.fullmatch(r"deliverypoint_(\d{3})_(\d{3})", str(dp_id))
    if not m:
        raise ValueError("deliverypoint_id_primaire invalide (attendu: deliverypoint_XXX_YYY)")
    return m.group(1), m.group(2)


def _get_next_index_by_dp_from_silver(dp_ids: List[str]) -> Dict[str, int]:
    """
    Retourne pour chaque deliverypoint_id_primaire le prochain index invoice (int).
    Basé sur la silver pour éviter les collisions même si on ne stocke plus 1 JSON par invoice.
    """
    df = _load_invoice_silver_df()
    next_by_dp: Dict[str, int] = {str(dp): 1 for dp in dp_ids}

    if df.empty or "deliverypoint_id_primaire" not in df.columns or "invoice_id_primaire" not in df.columns:
        return next_by_dp

    # filtre dp concernés
    df = df[df["deliverypoint_id_primaire"].astype(str).isin(set(map(str, dp_ids)))].copy()
    if df.empty:
        return next_by_dp

    # invoice_{buildingSuffix}_{dpSuffix}_{NN}
    pat = re.compile(r"^invoice_(\d{3})_(\d{3})_(\d{2})$")

    for dp in dp_ids:
        dp = str(dp)
        b_s, dp_s = parse_deliverypoint_id(dp)
        sub = df[df["deliverypoint_id_primaire"].astype(str) == dp]
        mx = 0
        for inv_id in sub["invoice_id_primaire"].astype(str).tolist():
            m = pat.match(inv_id)
            if m and m.group(1) == b_s and m.group(2) == dp_s:
                mx = max(mx, int(m.group(3)))
        next_by_dp[dp] = mx + 1

    return next_by_dp


@router.post("/batch_create", status_code=status.HTTP_201_CREATED)
def batch_create_invoices(payload: InvoiceBatchCreate, background_tasks: BackgroundTasks):
    invoices = payload.invoices
    if not invoices:
        raise HTTPException(status_code=400, detail="Liste invoices vide.")

    # 1) vérifier deliverypoints existent
    dp_ids = sorted({str(inv.deliverypoint_id_primaire) for inv in invoices})
    for dp_id in dp_ids:
        if not deliverypoint_exists_in_silver(dp_id):
            raise HTTPException(
                status_code=404,
                detail=f"deliverypoint_id_primaire introuvable en silver: {dp_id}",
            )

    # 2) next index par dp (depuis silver)
    next_idx_by_dp = _get_next_index_by_dp_from_silver(dp_ids)

    # 3) construire les items avec invoice_id_primaire + received_at
    received_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    items: List[dict] = []
    created_ids: List[str] = []

    for inv in invoices:
        dp_id = str(inv.deliverypoint_id_primaire)
        b_s, dp_s = parse_deliverypoint_id(dp_id)

        idx = next_idx_by_dp[dp_id]
        next_idx_by_dp[dp_id] += 1

        invoice_id = f"invoice_{b_s}_{dp_s}_{idx:02d}"
        created_ids.append(invoice_id)

        d = inv.model_dump(mode="json")
        d["invoice_id_primaire"] = invoice_id
        d["received_at"] = received_at
        items.append(d)

    # 4) écrire 1 SEUL fichier JSON batch dans bronze/invoice
    batch_id = f"invoice_batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
    batch_payload = {
        "batch_id": batch_id,
        "received_at": received_at,
        "items": items,
    }

    write_json_to_bronze(
        entity="invoice",
        file_name=f"{batch_id}.json",
        data=batch_payload,
    )

    # 5) lancer job silver une seule fois
    background_tasks.add_task(run_invoice_silver_job)

    return {
        "result": True,
        "batch_id": batch_id,
        "received_at": received_at,
        "invoices_created": len(items),
        "invoice_ids": created_ids,
    }
