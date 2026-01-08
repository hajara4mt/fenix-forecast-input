# app/jobs/invoice_silver.py

import json
from typing import List, Dict, Any

import pandas as pd

from app.azure_datalake import get_datalake_client
from config import AZURE_STORAGE_FILESYSTEM


def _extract_invoice_records(obj: Any, source_name: str) -> List[Dict]:
    """
    Accepte:
    - JSON unitaire: {invoice_id_primaire: ..., ...}
    - JSON batch: {batch_id: ..., received_at: ..., items: [ {...}, {...} ]}
      (ou clé 'invoices' au lieu de 'items')
    Retourne une liste de dicts "invoice".
    """
    if not isinstance(obj, dict):
        return []

    # cas batch
    if isinstance(obj.get("items"), list) or isinstance(obj.get("invoices"), list):
        items = obj.get("items") if isinstance(obj.get("items"), list) else obj.get("invoices")
        batch_received_at = obj.get("received_at")

        out: List[Dict] = []
        for it in items:
            if not isinstance(it, dict):
                continue

            # si l'item n'a pas received_at, on hérite du batch
            if "received_at" not in it and batch_received_at is not None:
                it = dict(it)  # copie
                it["received_at"] = batch_received_at

            # debug optionnel
            # it["_source_file"] = source_name

            out.append(it)
        return out

    # cas unitaire
    return [obj]


def load_invoice_bronze() -> pd.DataFrame:
    """
    Lit tous les JSON de bronze/invoice/ et les retourne dans un DataFrame.

    Supporte:
    - fichiers unitaires (1 invoice par fichier)
    - fichiers batch (liste d'invoices dans 'items' ou 'invoices')
    """
    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    try:
        paths = fs_client.get_paths("bronze/invoice")
    except Exception as e:
        print(f"⚠️ Impossible de lister bronze/invoice : {e}")
        return pd.DataFrame()

    records: List[Dict] = []

    for p in paths:
        if p.is_directory:
            continue

        file_client = fs_client.get_file_client(p.name)
        download = file_client.download_file()
        content = download.readall().decode("utf-8")

        try:
            obj = json.loads(content)
        except json.JSONDecodeError:
            print(f"⚠️ Fichier JSON invalide, ignoré : {p.name}")
            continue

        extracted = _extract_invoice_records(obj, source_name=p.name)
        if extracted:
            records.extend(extracted)

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records)


def transform_invoice(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoyage / typage / dédoublonnage du DataFrame invoice.
    """
    if df.empty:
        return df

    expected_cols = [
        "invoice_id_primaire",
        "deliverypoint_id_primaire",
        "invoice_code",
        "start",
        "end",
        "value",
        "received_at",
    ]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # Typage
    df["received_at"] = pd.to_datetime(df["received_at"], errors="coerce")
    df["start"] = pd.to_datetime(df["start"], errors="coerce").dt.date
    df["end"] = pd.to_datetime(df["end"], errors="coerce").dt.date
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # ⚠️ Très important: invoice_id_primaire doit exister pour dédoublonner
    # Si certains items batch n'ont pas invoice_id_primaire => ils seront "None" et ça casse le dedup.
    # On les drop pour éviter d'écraser tout.
    df = df[df["invoice_id_primaire"].notna()].copy()

    # Dédoublonnage : garder la dernière version par invoice_id_primaire
    df = df.sort_values(["invoice_id_primaire", "received_at"])
    df = df.drop_duplicates(subset=["invoice_id_primaire"], keep="last")

    return df[expected_cols]


def save_invoice_silver(df: pd.DataFrame) -> None:
    """
    Sauvegarde le DataFrame en Parquet dans silver/invoice/invoice.parquet
    """
    if df.empty:
        print("Aucune donnée invoice à écrire en silver.")
        return

    local_path = "invoice.parquet"
    df.to_parquet(local_path, index=False)

    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    remote_path = "silver/invoice/invoice.parquet"
    file_client = fs_client.get_file_client(remote_path)

    with open(local_path, "rb") as f:
        data = f.read()

    file_client.upload_data(data, overwrite=True)
    print(f"✅ Fichier Parquet écrit dans {remote_path}")


def run_invoice_silver_job():
    print("🔄 Lecture des données bronze/invoice ...")
    df_bronze = load_invoice_bronze()
    print(f"   {len(df_bronze)} lignes chargées depuis bronze (unitaire + batch).")

    print("🧹 Transformation / typage / dédoublonnage ...")
    df_silver = transform_invoice(df_bronze)
    print(f"   {len(df_silver)} lignes finales (invoices uniques).")

    print("💾 Écriture dans silver/invoice/invoice.parquet ...")
    save_invoice_silver(df_silver)

    print("✨ Job invoice silver terminé.")


if __name__ == "__main__":
    run_invoice_silver_job()
