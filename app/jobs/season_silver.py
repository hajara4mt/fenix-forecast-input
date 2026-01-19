import json
from typing import List, Dict
import pandas as pd
import numpy as np

from app.azure_datalake import get_datalake_client
from config import AZURE_STORAGE_FILESYSTEM


def load_season_bronze() -> pd.DataFrame:
    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    paths = fs_client.get_paths("bronze/season")
    records: List[Dict] = []

    for p in paths:
        if p.is_directory:
            continue
        file_client = fs_client.get_file_client(p.name)
        content = file_client.download_file().readall().decode("utf-8")
        try:
            records.append(json.loads(content))
        except json.JSONDecodeError:
            print(f"⚠️ JSON invalide ignoré: {p.name}")

    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


def transform_season(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    expected_cols = [
        "season_id_primaire",
        "name",
        "edition_code",
        "plateforme",
        "start_date",
        "end_date",
        "received_at",
    ]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.date
    df["received_at"] = pd.to_datetime(df["received_at"], errors="coerce")

    # Nettoyage inf
    df = df.replace([np.inf, -np.inf], np.nan)

    # Dédoublonnage : dernière version par season_id_primaire
    df = df.sort_values(["season_id_primaire", "received_at"])
    df = df.drop_duplicates(subset=["season_id_primaire"], keep="last")

    return df[expected_cols]


def save_season_silver(df: pd.DataFrame) -> None:
    if df.empty:
        print("Aucune donnée season à écrire en silver.")
        return

    df = df.where(pd.notnull(df), None)

    local_path = "season.parquet"
    df.to_parquet(local_path, index=False)

    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    remote_path = "silver/season/season.parquet"
    file_client = fs_client.get_file_client(remote_path)

    with open(local_path, "rb") as f:
        data = f.read()

    file_client.upload_data(data, overwrite=True)
    print(f"✅ Parquet season écrit dans {remote_path}")


def run_season_silver_job():
    print("🔄 Lecture bronze/season ...")
    df_bronze = load_season_bronze()
    print(f"   {len(df_bronze)} fichiers chargés.")

    print("🧹 Transform ...")
    df_silver = transform_season(df_bronze)
    print(f"   {len(df_silver)} lignes finales.")

    print("💾 Écriture silver ...")
    save_season_silver(df_silver)
    print("✨ Job season terminé.")
