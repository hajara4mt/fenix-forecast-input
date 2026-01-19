# app/jobs/building_silver.py

import json
import os
from typing import List, Dict

import pandas as pd
import numpy as np  # 


from app.azure_datalake import get_datalake_client
from config import AZURE_STORAGE_FILESYSTEM


def load_building_bronze() -> pd.DataFrame:
    """
    Lit tous les JSON de bronze/building/ et les retourne dans un DataFrame.
    Chaque fichier JSON = 1 ligne.
    """

    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    # Liste tous les paths sous bronze/building/
    paths = fs_client.get_paths("bronze/building")

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



def transform_building(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    expected_cols = [
        "id_building_primaire",
        "platform_code",
        "building_code",
        "name",
        "latitude",
        "longitude",
        "organisation",
        "address",
        "city",
        "zipcode",
        "country",
        "typology",
        "geographical_area",
        "occupant",
        "surface",
        "reference_period_start",
        "reference_period_end",
        "weather_station",
        "received_at",
    ]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # ---- NUMÉRIQUES ----
    # On convertit proprement en float, sans forcer 0 là où ça n'a pas de sens
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["geographical_area"] = pd.to_numeric(df["geographical_area"], errors="coerce")
    df["occupant"] = pd.to_numeric(df["occupant"], errors="coerce")
    df["surface"] = pd.to_numeric(df["surface"], errors="coerce")

    # On corrige aussi les valeurs hors bornes
    df.loc[~df["latitude"].between(-90, 90) & df["latitude"].notna(), "latitude"] = np.nan
    df.loc[~df["longitude"].between(-180, 180) & df["longitude"].notna(), "longitude"] = np.nan

    # Si tu veux vraiment forcer occupant / surface à 0 quand manquants :
    df["occupant"] = df["occupant"].fillna(0)
    df["surface"] = df["surface"].fillna(0)

    # plus besoin de ce check, vu qu'on a fillna(0)
    # if df["occupant"].isna().any():
    #     raise ValueError(...)

    # ---- DATES ----
    df["reference_period_start"] = pd.to_datetime(
        df["reference_period_start"], errors="coerce"
    )
    df["reference_period_end"] = pd.to_datetime(
        df["reference_period_end"], errors="coerce"
    )
    df["received_at"] = pd.to_datetime(df["received_at"], errors="coerce")

    # ---- DÉDOUBLONNAGE ----
    df = df.sort_values(["id_building_primaire", "received_at"])
    df = df.drop_duplicates(subset=["id_building_primaire"], keep="last")

    df = df[expected_cols]
    return df



def save_building_silver(df: pd.DataFrame) -> None:
    if df.empty:
        print("Aucune donnée building à écrire en silver.")
        return

    # Copie pour ne pas muter l'original
    df = df.copy()

    # 1) Nettoyer les valeurs bizarres
    import numpy as np
    df = df.replace([np.inf, -np.inf], np.nan)

    # 2) Transformer tous les NaN en None (→ null en Arrow/Parquet)
    df = df.where(pd.notnull(df), None)

    # 3) Écrire en local
    local_path = "building.parquet"
    df.to_parquet(local_path, index=False)

    # 4) Uploader vers ADLS
    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    remote_path = "silver/building/building.parquet"
    file_client = fs_client.get_file_client(remote_path)

    with open(local_path, "rb") as f:
        data = f.read()

    file_client.upload_data(data, overwrite=True)

    print(f"✅ Fichier Parquet écrit dans {remote_path}")



def run_building_silver_job():
    """
    Job complet : bronze -> silver pour building.
    À lancer manuellement ou via ADF plus tard.
    """
    print("🔄 Lecture des données bronze/building ...")
    df_bronze = load_building_bronze()

    print(f"   {len(df_bronze)} fichiers chargés depuis bronze.")

    print("🧹 Transformation / typage / dédoublonnage ...")
    df_silver = transform_building(df_bronze)

    print(f"   {len(df_silver)} lignes finales (bâtiments uniques).")

    print("💾 Écriture dans silver/building/building.parquet ...")
    save_building_silver(df_silver)

    print("✨ Job building silver terminé.")


if __name__ == "__main__":
    run_building_silver_job()
