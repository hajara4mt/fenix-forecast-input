from uuid import uuid4
from datetime import datetime, timezone

import re
from app.azure_datalake import get_datalake_client, AZURE_STORAGE_FILESYSTEM

def new_id() -> str:
    return str(uuid4())

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def next_deliverypoint_index_for_building(building_id: str) -> int:
    """
    building_id : 'building_000003'
    Retourne le prochain numéro de deliverypoint pour ce building :
    1,2,3,... en regardant les fichiers de bronze/deliverypoint.
    """

    # on extrait '000003'
    if not building_id.startswith("building_") or len(building_id) != len("building_000"):
        raise ValueError(f"id_building_primaire invalide : {building_id}")

    building_suffix = building_id.split("_", 1)[1]  # '000003'

    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    max_index = 0

    try:
        paths = fs.get_paths("bronze/deliverypoint")
    except Exception as e:
        print(f"⚠️ Impossible de lister bronze/deliverypoint : {e}")
        return 1

    # pattern du fichier : deliverypoint_000003_01.json
    pattern = re.compile(rf"deliverypoint_{building_suffix}_(\d{{3}})\.json")

    for p in paths:
        if p.is_directory:
            continue

        filename = p.name.split("/")[-1]
        m = pattern.match(filename)
        if m:
            try:
                num = int(m.group(1))
                if num > max_index:
                    max_index = num
            except ValueError:
                continue

    return max_index + 1  # prochain numéro


# app/utils.py
import io
import re
import pandas as pd
from typing import List

from fastapi import HTTPException
from app.azure_datalake import get_datalake_client, delete_file_from_bronze
from config import AZURE_STORAGE_FILESYSTEM


# =========================
# PATHS SILVER
# =========================
SILVER_BUILDING_PATH = "silver/building/building.parquet"
SILVER_DELIVERYPOINT_PATH = "silver/deliverypoint/deliverypoint.parquet"
SILVER_INVOICE_PATH = "silver/invoice/invoice.parquet"
SILVER_USAGE_DATA_PATH = "silver/usage_data/usage_data.parquet"


# =========================
# BUILDING (silver)
# =========================
def load_building_silver() -> pd.DataFrame:
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    file = fs.get_file_client(SILVER_BUILDING_PATH)

    try:
        data = file.download_file().readall()
    except Exception:
        return pd.DataFrame()

    return pd.read_parquet(io.BytesIO(data))


def save_building_silver(df: pd.DataFrame) -> None:
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    file = fs.get_file_client(SILVER_BUILDING_PATH)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    file.upload_data(buf.read(), overwrite=True)


def building_exists_in_silver(building_id: str) -> bool:
    df = load_building_silver()
    return (not df.empty) and ("id_building_primaire" in df.columns) and (building_id in df["id_building_primaire"].astype(str).values)


# =========================
# DELIVERYPOINT (silver)
# =========================
def load_deliverypoint_silver() -> pd.DataFrame:
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    file_client = fs.get_file_client(SILVER_DELIVERYPOINT_PATH)

    try:
        raw = file_client.download_file().readall()
    except Exception:
        return pd.DataFrame()

    return pd.read_parquet(io.BytesIO(raw))


def save_deliverypoint_silver(df: pd.DataFrame) -> None:
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    file_client = fs.get_file_client(SILVER_DELIVERYPOINT_PATH)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    file_client.upload_data(buf.read(), overwrite=True)


def deliverypoint_exists_in_silver(dp_id: str) -> bool:
    df = load_deliverypoint_silver()
    return (not df.empty) and ("deliverypoint_id_primaire" in df.columns) and (dp_id in df["deliverypoint_id_primaire"].astype(str).values)


def get_deliverypoints_for_building(building_id: str) -> pd.DataFrame:
    df = load_deliverypoint_silver()
    if df.empty:
        return pd.DataFrame()
    if "id_building_primaire" not in df.columns:
        return pd.DataFrame()
    return df[df["id_building_primaire"].astype(str) == str(building_id)].copy()


# =========================
# INVOICE (silver)
# =========================
def load_invoice_silver() -> pd.DataFrame:
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    try:
        file_client = fs.get_file_client(SILVER_INVOICE_PATH)
        raw = file_client.download_file().readall()
    except Exception:
        return pd.DataFrame()

    if not raw:
        return pd.DataFrame()

    return pd.read_parquet(io.BytesIO(raw))


def save_invoice_silver(df: pd.DataFrame) -> None:
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    file_client = fs.get_file_client(SILVER_INVOICE_PATH)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    file_client.upload_data(buf.read(), overwrite=True)


def delete_invoices_for_deliverypoint(dp_id: str) -> int:
    """
    Supprime toutes les invoices (silver + bronze) associées à un deliverypoint.
    Retourne le nombre d'invoices supprimées en silver.
    """
    df = load_invoice_silver()
    if df.empty or "deliverypoint_id_primaire" not in df.columns:
        return 0

    mask = df["deliverypoint_id_primaire"].astype(str) == str(dp_id)
    df_to_delete = df[mask].copy()

    if df_to_delete.empty:
        return 0

    # supprimer en silver
    df_keep = df[~mask].copy()
    save_invoice_silver(df_keep)

    # supprimer en bronze
    if "invoice_id_primaire" in df_to_delete.columns:
        for inv_id in df_to_delete["invoice_id_primaire"].astype(str).tolist():
            delete_file_from_bronze("invoice", f"{inv_id}.json")

    return len(df_to_delete)


# =========================
# USAGE DATA (silver)
# =========================
def load_usage_data_silver() -> pd.DataFrame:
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    file_client = fs.get_file_client(SILVER_USAGE_DATA_PATH)

    try:
        raw = file_client.download_file().readall()
    except Exception:
        return pd.DataFrame()

    return pd.read_parquet(io.BytesIO(raw))


def save_usage_data_silver(df: pd.DataFrame) -> None:
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    file_client = fs.get_file_client(SILVER_USAGE_DATA_PATH)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    file_client.upload_data(buf.read(), overwrite=True)


def delete_usage_data_for_building(building_id: str) -> int:
    """
    Supprime toutes les usage_data (silver + bronze) associées à un building.
    Retourne le nombre supprimé en silver.
    """
    df = load_usage_data_silver()
    if df.empty or "id_building_primaire" not in df.columns:
        return 0

    mask = df["id_building_primaire"].astype(str) == str(building_id)
    df_to_delete = df[mask].copy()

    if df_to_delete.empty:
        return 0

    # supprimer en silver
    df_keep = df[~mask].copy()
    save_usage_data_silver(df_keep)

    # supprimer en bronze (si tu stockes les JSON avec usage_data_id_primaire.json)
    if "usage_data_id_primaire" in df_to_delete.columns:
        for ud_id in df_to_delete["usage_data_id_primaire"].astype(str).tolist():
            delete_file_from_bronze("usage_data", f"{ud_id}.json")

    return len(df_to_delete)


import pandas as pd

def building_business_key_exists_in_silver(
    platform_code: str,
    building_code: str,
    load_building_silver_fn,
) -> bool:
    """
    True si (platform_code, building_code) existe déjà dans silver/building.
    load_building_silver_fn : ta fonction load_building_silver() pour éviter les imports circulaires.
    """
    try:
        df = load_building_silver_fn()
    except Exception:
        return False

    if df.empty:
        return False

    needed = {"platform_code", "building_code"}
    if not needed.issubset(df.columns):
        return False

    mask = (
        df["platform_code"].astype(str).str.strip() == str(platform_code).strip()
    ) & (
        df["building_code"].astype(str).str.strip() == str(building_code).strip()
    )

    return bool(mask.any())
