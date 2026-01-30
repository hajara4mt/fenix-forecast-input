from fastapi import APIRouter, HTTPException, BackgroundTasks
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from azure.core.exceptions import ResourceNotFoundError
from app.utils import random_token
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


@router.put("/create", status_code=201)
def create_season(payload: SeasonCreate, background_tasks: BackgroundTasks):
     # 0) cohérence simple : start <= end
    if payload.start_date > payload.end_date:
        raise HTTPException(
            status_code=400,
            detail="La date de début de saison doit être <= à la date de fin.",
        )

    # 1) générer un token aléatoire pour la saison
    season_token = random_token(5)  # même longueur que building
    season_id = f"season_{season_token}"

    # 2) horodatage
    received_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 3) payload → dict JSON-friendly
    raw = payload.model_dump(mode="json")
    raw["season_id_primaire"] = season_id
    raw["received_at"] = received_at

    # 4) écrire en bronze : bronze/season/season_01JH3QD.json
    write_json_to_bronze(
        entity="season",
        file_name=f"{season_id}.json",
        data=raw,
    )

    # 5) lancer le job bronze -> silver
    background_tasks.add_task(run_season_silver_job)

    return {
        "result": True,
        "season_id_primaire": season_id,
        "received_at": received_at,
    }



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
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée season en silver"
        )

    mask = df["season_id_primaire"] == season_id_primaire
    if not mask.any():
        raise HTTPException(
            status_code=404,
            detail="Saison non trouvée"
        )

    idx = df.index[mask][0]

    # 🔎 (optionnel mais recommandé) cohérence des dates
    # adapte les noms si ton SeasonCreate utilise start_date / end_date
    if hasattr(payload, "start_date") and hasattr(payload, "end_date"):
        if payload.start_date > payload.end_date:
            raise HTTPException(
                status_code=400,
                detail="start_date doit être <= end_date pour une saison.",
            )

    # ⚠️ IMPORTANT : mode="python" pour garder les types (date, etc.),
    # pas mode="json" qui met tout en str
    new_data = payload.model_dump(mode="python")

    for col, value in new_data.items():
        if col in df.columns:
            df.at[idx, col] = value

    # on garde le même id primaire
    df.at[idx, "season_id_primaire"] = season_id_primaire

    # horodatage de mise à jour
    now_dt = datetime.now(timezone.utc)
    df.at[idx, "received_at"] = now_dt

    # (optionnel mais très safe si tu veux forcer les types)
    # si tes colonnes s'appellent start_date / end_date en silver :
    # df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
    # df["end_date"] = pd.to_datetime(df["end_date"]).dt.date

    save_season_silver(df)

    return {
        "result": True,
        "message": "Saison mise à jour.",
        "season_id_primaire": season_id_primaire,
        "received_at": now_dt.isoformat(),
    }