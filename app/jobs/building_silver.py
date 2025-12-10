# app/jobs/building_silver.py

import json
import os
from typing import List, Dict

import pandas as pd

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
    """
    Nettoie / typage / dédoublonnage du DataFrame building.
    """

    if df.empty:
        return df

    # On s'assure que ces colonnes existent (sinon pandas ne râle pas mais c'est silencieux)
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

    # Ajouter les colonnes manquantes avec NaN si besoin
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # Typage basique
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce").fillna(0)
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["geographical_area"] = pd.to_numeric(df["geographical_area"], errors="coerce")
    df["occupant"] = pd.to_numeric(df["occupant"], errors="coerce").fillna(0)
    df["surface"] = pd.to_numeric(df["surface"], errors="coerce").fillna(0)

    # vérifier qu'il n'y a aucun NaN
    if df["occupant"].isna().any():
      raise ValueError("Certaines lignes building ont un occupant invalide ou manquant.")

    # Dates
    df["reference_period_start"] = pd.to_datetime(
        df["reference_period_start"], errors="coerce"
    )
    df["reference_period_end"] = pd.to_datetime(
        df["reference_period_end"], errors="coerce"
    )
    df["received_at"] = pd.to_datetime(df["received_at"], errors="coerce")

    # Dédoublonnage : garder la dernière version par id_building_primaire
    df = df.sort_values(["id_building_primaire", "received_at"])
    df = df.drop_duplicates(subset=["id_building_primaire"], keep="last")

    # On garde seulement les colonnes dans l'ordre propre
    df = df[expected_cols]

    return df


def save_building_silver(df: pd.DataFrame) -> None:
    """
    Sauvegarde le DataFrame en Parquet dans silver/building/building.parquet
    """

    if df.empty:
        print("Aucune donnée building à écrire en silver.")
        return

    # 1) Écrire en local
    local_path = "building.parquet"
    df.to_parquet(local_path, index=False)

    # 2) Uploader vers ADLS dans silver/building/building.parquet
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
