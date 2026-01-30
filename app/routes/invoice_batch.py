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
from app.utils import random_token


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
    deliverypoint_01JH3QD_4C42 -> ("01JH3QD", "4C42")
    """
    m = re.fullmatch(r"deliverypoint_([A-Za-z0-9]+)_([A-Za-z0-9]+)", str(dp_id))
    if not m:
        raise ValueError(
            "deliverypoint_id_primaire invalide. "
            "Attendu : deliverypoint_<BID>_<DPID> (ex: deliverypoint_01JH3QD_4C42)."
        )
    return m.group(1), m.group(2)


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

    # 2) timestamp commun pour le batch
    received_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    items: List[dict] = []
    created_ids: List[str] = []
    used_ids: set[str] = set()  # sécurité anti-collision dans le même batch

    for inv in invoices:
        dp_id = str(inv.deliverypoint_id_primaire)

        # récupérer BID et DPID
        try:
            b_s, dp_s = parse_deliverypoint_id(dp_id)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        # générer un token pour l'invoice, ex: A9F3
        inv_token = random_token(4)

        invoice_id = f"invoice_{b_s}_{dp_s}_{inv_token}"

        # petite sécurité : si, par hasard extrême, on génère deux fois le même
        while invoice_id in used_ids:
            inv_token = random_token(4)
            invoice_id = f"invoice_{b_s}_{dp_s}_{inv_token}"

        used_ids.add(invoice_id)
        created_ids.append(invoice_id)

        d = inv.model_dump(mode="json")
        d["invoice_id_primaire"] = invoice_id
        d["received_at"] = received_at
        items.append(d)

    # 3) écrire 1 SEUL fichier JSON batch dans bronze/invoice
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

    # 4) lancer job silver une seule fois
    background_tasks.add_task(run_invoice_silver_job)

    return {
        "result": True,
        "batch_id": batch_id,
        "received_at": received_at,
        "invoices_created": len(items),
        "invoice_ids": created_ids,
    }
