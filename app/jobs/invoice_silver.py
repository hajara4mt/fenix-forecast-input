# app/jobs/invoice_silver.py

import json
from typing import List, Dict

import pandas as pd

from app.azure_datalake import get_datalake_client
from config import AZURE_STORAGE_FILESYSTEM   

def load_invoice_bronze() -> pd.DataFrame:
    """
    Lit tous les JSON de bronze/invoice/ et les retourne dans un DataFrame.
    Chaque fichier JSON = 1 ligne.
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
            data = json.loads(content)
            records.append(data)
        except json.JSONDecodeError:
            print(f"⚠️ Fichier JSON invalide, ignoré : {p.name}")

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records)


def transform_invoice(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoyage / typage / dédoublonnage du DataFrame invoice.
    """
    if df.empty:
        return df

    # colonnes qu’on veut garder en silver
    expected_cols = [
        "invoice_id_primaire",        # généré par l'API
        "deliverypoint_id_primaire",  # lien vers deliverypoint
        "invoice_code",
        "start",
        "end",
        "value",
        "received_at",
    ]

    # S'assurer que toutes les colonnes existent
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # Typage
    df["received_at"] = pd.to_datetime(df["received_at"], errors="coerce")
    df["start"] = pd.to_datetime(df["start"], errors="coerce").dt.date
    df["end"] = pd.to_datetime(df["end"], errors="coerce").dt.date

    # value en numérique (int ou float)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Dédoublonnage : garder la dernière version par invoice_id_primaire
    df = df.sort_values(["invoice_id_primaire", "received_at"])
    df = df.drop_duplicates(subset=["invoice_id_primaire"], keep="last")

    # Ré-ordonner les colonnes
    df = df[expected_cols]

    return df


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
    print(f"   {len(df_bronze)} fichiers chargés depuis bronze.")

    print("🧹 Transformation / typage / dédoublonnage ...")
    df_silver = transform_invoice(df_bronze)
    print(f"   {len(df_silver)} lignes finales (invoices uniques).")

    print("💾 Écriture dans silver/invoice/invoice.parquet ...")
    save_invoice_silver(df_silver)

    print("✨ Job invoice silver terminé.")


if __name__ == "__main__":
    run_invoice_silver_job()
