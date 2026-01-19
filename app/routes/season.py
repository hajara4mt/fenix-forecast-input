from fastapi import APIRouter, HTTPException, BackgroundTasks
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from azure.core.exceptions import ResourceNotFoundError

from app.azure_datalake import get_datalake_client, write_json_to_bronze
from config import AZURE_STORAGE_FILESYSTEM
from app.models import SeasonCreate, SeasonRead , SeasonRead1
from app.jobs.season_silver import run_season_silver_job

router = APIRouter(
    prefix="/season",
    tags=["season"],
)


def _df_json_safe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.where(pd.notnull(df), None)
    return df


def load_season_silver() -> pd.DataFrame:
    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    remote_path = "silver/season/season.parquet"
    file_client = fs_client.get_file_client(remote_path)

    try:
        download = file_client.download_file()
        data = download.readall()
    except Exception as e:
        print(f"Erreur chargement parquet silver season : {e}")
        return pd.DataFrame()

    local_path = "season_tmp.parquet"
    with open(local_path, "wb") as f:
        f.write(data)

    return pd.read_parquet(local_path)


def save_season_silver(df: pd.DataFrame) -> None:
    local_path = "season_tmp.parquet"
    df.to_parquet(local_path, index=False)

    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    remote_path = "silver/season/season.parquet"
    file_client = fs_client.get_file_client(remote_path)

    with open(local_path, "rb") as f:
        data = f.read()

    file_client.upload_data(data, overwrite=True)


# compteur global saison: season_001, season_002, ...
season_index: int = 0


@router.put("/create", status_code=201)
def create_season(payload: SeasonCreate, background_tasks: BackgroundTasks):
    global season_index

    season_index += 1
    season_id = f"season_{season_index:03d}"

    received_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    raw = payload.model_dump(mode="json")
    raw["season_id_primaire"] = season_id
    raw["received_at"] = received_at

    write_json_to_bronze(
        entity="season",
        file_name=f"{season_id}.json",
        data=raw,
    )

    background_tasks.add_task(run_season_silver_job)

    return {"result": True, "season_id_primaire": season_id, "received_at": received_at}


@router.get("/all", response_model=list[SeasonRead])
def get_season_all():
    df = load_season_silver()
    if df.empty:
        return []
    df = _df_json_safe(df)
    return df.to_dict(orient="records")


@router.get("/{season_id_primaire}", response_model=SeasonRead1)
def get_season_single(season_id_primaire: str):
    df = load_season_silver()
    row = df[df["season_id_primaire"] == season_id_primaire]
    if row.empty:
        raise HTTPException(status_code=404, detail="Saison non trouvée")

    row = _df_json_safe(row)
    return row.iloc[0].to_dict()


@router.delete("/{season_id_primaire}", status_code=200)
def delete_season(season_id_primaire: str):
    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    df = load_season_silver()
    if df.empty or "season_id_primaire" not in df.columns:
        raise HTTPException(status_code=404, detail="Aucune donnée season en silver")

    mask = df["season_id_primaire"] == season_id_primaire
    if not mask.any():
        raise HTTPException(status_code=404, detail="Saison non trouvée")

    df_filtered = df[~mask].reset_index(drop=True)
    save_season_silver(df_filtered)

    # delete bronze json
    remote_path_bronze = f"bronze/season/{season_id_primaire}.json"
    file_client_bronze = fs_client.get_file_client(remote_path_bronze)
    try:
        file_client_bronze.delete_file()
    except ResourceNotFoundError:
        print(f"⚠️ bronze absent pour {season_id_primaire}")

    return {"result": True, "message": f"Saison {season_id_primaire} supprimée."}


@router.patch("/update/{season_id_primaire}", status_code=200)
def update_season(season_id_primaire: str, payload: SeasonCreate):
    df = load_season_silver()
    if df.empty or "season_id_primaire" not in df.columns:
        raise HTTPException(status_code=404, detail="Aucune donnée season en silver")

    mask = df["season_id_primaire"] == season_id_primaire
    if not mask.any():
        raise HTTPException(status_code=404, detail="Saison non trouvée")

    idx = df.index[mask][0]

    new_data = payload.model_dump(mode="json")
    for col, value in new_data.items():
        if col in df.columns:
            df.at[idx, col] = value

    df.at[idx, "season_id_primaire"] = season_id_primaire
    df.at[idx, "received_at"] = datetime.now(timezone.utc)

    save_season_silver(df)

    return {"result": True, "message": "Saison mise à jour.", "season_id_primaire": season_id_primaire}
