# app/jobs/usage_data_silver.py

import json
import os
from typing import List, Dict

import pandas as pd

from app.azure_datalake import get_datalake_client
from config import AZURE_STORAGE_FILESYSTEM


def load_usage_data_bronze() -> pd.DataFrame:
    """
    Lit tous les JSON de bronze/usage_data/ et les retourne dans un DataFrame.
    Chaque fichier JSON = 1 ligne.
    """

    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    # Liste tous les paths sous bronze/usage_data/
    paths = fs_client.get_paths("bronze/usage_data")

    records: List[Dict] = []

    for p in paths:
        # on ignore les dossiers, on ne prend que les fichiers
        if p.is_directory:
            continue

        file_client = fs_client.get_file_client(p.name)
        download = file_client.download_file()
        content = download.readall().decode("utf-8")

        try:
            data = json.loads(content)
            records.append(data)
        except json.JSONDecodeError:
            # si un fichier est corrompu, on peut soit le skipper, soit lever une erreur
            print(f"⚠️ Fichier JSON invalide, ignoré : {p.name}")

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    return df


def transform_usage_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie / typage / dédoublonnage du DataFrame usage_data.
    """

    if df.empty:
        return df

    expected_cols = [
        "usage_data_id_primaire",
        "id_building_primaire",
        "type",
        "date",
        "value",
        "received_at",
    ]

    # Ajouter les colonnes manquantes avec NaN si besoin
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # Typage
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Dates
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["received_at"] = pd.to_datetime(df["received_at"], errors="coerce")

    # Dédoublonnage : garder la dernière version par usage_data_id_primaire
    df = df.sort_values(["usage_data_id_primaire", "received_at"])
    df = df.drop_duplicates(subset=["usage_data_id_primaire"], keep="last")

    # On garde seulement les colonnes dans l'ordre propre
    df = df[expected_cols]

    return df


def save_usage_data_silver(df: pd.DataFrame) -> None:
    """
    Sauvegarde le DataFrame en Parquet dans silver/usage_data/usage_data.parquet
    """

    if df.empty:
        print("Aucune donnée usage_data à écrire en silver.")
        return

    # 1) Écrire en local
    local_path = "usage_data.parquet"
    df.to_parquet(local_path, index=False)

    # 2) Uploader vers ADLS dans silver/usage_data/usage_data.parquet
    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    remote_path = "silver/usage_data/usage_data.parquet"
    file_client = fs_client.get_file_client(remote_path)

    with open(local_path, "rb") as f:
        data = f.read()

    file_client.upload_data(data, overwrite=True)

    print(f"✅ Fichier Parquet écrit dans {remote_path}")


def run_usage_data_silver_job():
    """
    Job complet : bronze -> silver pour usage_data.
    À lancer manuellement ou via ADF plus tard.
    """
    print("🔄 Lecture des données bronze/usage_data ...")
    df_bronze = load_usage_data_bronze()

    print(f"   {len(df_bronze)} fichiers chargés depuis bronze.")

    print("🧹 Transformation / typage / dédoublonnage ...")
    df_silver = transform_usage_data(df_bronze)

    print(f"   {len(df_silver)} lignes finales (usage_data uniques).")

    print("💾 Écriture dans silver/usage_data/usage_data.parquet ...")
    save_usage_data_silver(df_silver)

    print("✨ Job usage_data silver terminé.")


if __name__ == "__main__":
    run_usage_data_silver_job()
