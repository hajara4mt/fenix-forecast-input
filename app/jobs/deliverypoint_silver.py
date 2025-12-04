# app/jobs/deliverypoint_silver.py

import json
from typing import List, Dict

import pandas as pd

from app.azure_datalake import get_datalake_client
from config import AZURE_STORAGE_FILESYSTEM


def load_deliverypoint_bronze() -> pd.DataFrame:
    """
    Lit tous les JSON de bronze/deliverypoint/ et les retourne dans un DataFrame.
    Chaque fichier JSON = 1 ligne.
    """
    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    paths = fs_client.get_paths("bronze/deliverypoint")

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


def transform_deliverypoint(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie / typage / dédoublonnage du DataFrame deliverypoint.
    """
    if df.empty:
        return df

    expected_cols = [
        "deliverypoint_id_primaire",  # généré par l'API
        "id_building_primaire",       # lien vers building
        "deliverypoint_code",
        "deliverypoint_number",
        "fluid",
        "fluid_unit",
        "received_at",
    ]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # Typage
    df["received_at"] = pd.to_datetime(df["received_at"], errors="coerce")

    # Dédoublonnage : garder la dernière version par deliverypoint_id_primaire
    df = df.sort_values(["deliverypoint_id_primaire", "received_at"])
    df = df.drop_duplicates(subset=["deliverypoint_id_primaire"], keep="last")

    df = df[expected_cols]
    return df


def save_deliverypoint_silver(df: pd.DataFrame) -> None:
    """
    Sauvegarde le DataFrame en Parquet dans silver/deliverypoint/deliverypoint.parquet
    """
    if df.empty:
        print("Aucune donnée deliverypoint à écrire en silver.")
        return

    local_path = "deliverypoint.parquet"
    df.to_parquet(local_path, index=False)

    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    remote_path = "silver/deliverypoint/deliverypoint.parquet"
    file_client = fs_client.get_file_client(remote_path)

    with open(local_path, "rb") as f:
        data = f.read()

    file_client.upload_data(data, overwrite=True)

    print(f"✅ Fichier Parquet écrit dans {remote_path}")


def run_deliverypoint_silver_job():
    print("🔄 Lecture des données bronze/deliverypoint ...")
    df_bronze = load_deliverypoint_bronze()
    print(f"   {len(df_bronze)} fichiers chargés depuis bronze.")

    print("🧹 Transformation / typage / dédoublonnage ...")
    df_silver = transform_deliverypoint(df_bronze)
    print(f"   {len(df_silver)} lignes finales (deliverypoints uniques).")

    print("💾 Écriture dans silver/deliverypoint/deliverypoint.parquet ...")
    save_deliverypoint_silver(df_silver)

    print("✨ Job deliverypoint silver terminé.")


if __name__ == "__main__":
    run_deliverypoint_silver_job()
