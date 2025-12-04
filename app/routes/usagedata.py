# app/routers/usage_data.py

from fastapi import APIRouter, HTTPException, BackgroundTasks
import pandas as pd 
from azure.core.exceptions import ResourceNotFoundError

from datetime import datetime, timezone
from app.azure_datalake import write_json_to_bronze
from app.azure_datalake import get_datalake_client
from config import AZURE_STORAGE_FILESYSTEM
from app.models import UsageDataCreate , UsageDataRead
from app.jobs.usage_data_silver import run_usage_data_silver_job
from app.routes.building import building_exists_in_silver , load_building_silver


import re  # pour sécuriser le format de l'id building

router = APIRouter(
    prefix="/usage-data",
    tags=["usage-data"],
)


def load_usage_data_silver() -> pd.DataFrame:
    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    remote_path = "silver/usage_data/usage_data.parquet"
    file_client = fs_client.get_file_client(remote_path)

    try:
        download = file_client.download_file()
        data = download.readall()
    except Exception as e:
        print(f"Erreur chargement parquet silver usage_data : {e}")
        return pd.DataFrame()

    # sauvegarde temporaire locale
    local_path = "usage_data_tmp.parquet"
    with open(local_path, "wb") as f:
        f.write(data)

    df = pd.read_parquet(local_path)
    return df


def save_usage_data_silver(df: pd.DataFrame) -> None:

    # 1) Sauvegarde locale temporaire
    local_path = "usage_data_tmp.parquet"
    df.to_parquet(local_path, index=False)

    # 2) Upload vers la Silver ADLS
    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    remote_path = "silver/usage_data/usage_data.parquet"
    file_client = fs_client.get_file_client(remote_path)

    with open(local_path, "rb") as f:
        data = f.read()

    file_client.upload_data(data, overwrite=True)

##3 - fontion de test: 



# compteur par building : { "003": 1, "004": 5, ... }
usage_index_by_building: dict[str, int] = {}


@router.put("/create", status_code=201)
def create_usage_data(payload: UsageDataCreate , background_tasks: BackgroundTasks):
    """On crée un nouveau id de usage data de la forme  : usage_data_{buildingIndex}_{usageIndexPourCeBuilding}"""
    global usage_index_by_building

    # 1) vérifier / extraire l'index du building
    building_id = payload.id_building_primaire  # ex: "building_003"
    match = re.fullmatch(r"building_(\d+)", building_id)
    if not match:
        raise HTTPException(
            status_code=400,
            detail=f"id_building_primaire invalide (attendu 'building_XXX') : {building_id}",
        )

    building_index = match.group(1)  # "003"

    # 2) déterminer l'index d'usage pour CE building
    current_usage_index = usage_index_by_building.get(building_index, 0) + 1
    usage_index_by_building[building_index] = current_usage_index

    # 3) construire l'id usage : usage_data_003_001
    usage_id = f"usage_data_{building_index}_{current_usage_index:03d}"

    # 4) timestamp
    received_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 5) payload → dict JSON-friendly
    raw_dict = payload.model_dump(mode="json")
    raw_dict["usage_data_id_primaire"] = usage_id
    raw_dict["received_at"] = received_at

    # 6) écrire dans bronze/usage_data/usage_data_003_001.json
    write_json_to_bronze(
        entity="usage_data",
        file_name=f"{usage_id}.json",
        data=raw_dict,
    )

    # 7) lancer le job bronze -> silver en tâche de fond
    background_tasks.add_task(run_usage_data_silver_job)

    # 7) réponse API
    return {
        "result": True,
        "usage_data_id_primaire": usage_id,
        "received_at": received_at,
    }


##-----------------------------------------
##-------------Get Collection--------------
#---------------------------------------
@router.get("/all", response_model=list[UsageDataRead])
def get_usage_data_collection():
    df = load_usage_data_silver()

    # Optionnel : tri
    # df = df.sort_values("received_at")

    return df.to_dict(orient="records")




##-----------------------------------------
##----------Get Single Usage Data--------------
#---------------------------------------
@router.get("/{usage_data_id_primaire}", response_model=UsageDataRead)
def get_usage_data_single(usage_data_id_primaire: str):
    df = load_usage_data_silver()

    row = df[df["usage_data_id_primaire"] == usage_data_id_primaire]

    if row.empty:
        raise HTTPException(status_code=404, detail="Usage data non trouvée")

    return row.iloc[0].to_dict()


##-----------------------------------------
##----------supprimer Single Usage Data--------------
#---------------------------------------
@router.delete("/{usage_data_id_primaire}", status_code=200)
def delete_usage_data(usage_data_id_primaire: str):
    
    # 1) Clients ADLS
    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    # 2) Charger la silver et vérifier que l'id existe
    df = load_usage_data_silver()

    if df.empty or "usage_data_id_primaire" not in df.columns:
        raise HTTPException(status_code=404, detail="Aucune donnée usage_data en silver")

    mask = df["usage_data_id_primaire"] == usage_data_id_primaire

    if not mask.any():
        raise HTTPException(
            status_code=404,
            detail=f"Usage data non trouvée pour id {usage_data_id_primaire}",
        )

    # 3) Filtrer la ligne à supprimer
    df_filtered = df[~mask].reset_index(drop=True)

    # 4) Réécrire le parquet silver (même chemin)
    local_path = "usage_data_tmp.parquet"
    df_filtered.to_parquet(local_path, index=False)

    remote_path_silver = "silver/usage_data/usage_data.parquet"
    file_client_silver = fs_client.get_file_client(remote_path_silver)

    with open(local_path, "rb") as f:
        data = f.read()

    file_client_silver.upload_data(data, overwrite=True)

    # 5) Supprimer le JSON en bronze (si présent)
    remote_path_bronze = f"bronze/usage_data/{usage_data_id_primaire}.json"
    file_client_bronze = fs_client.get_file_client(remote_path_bronze)

    try:
        file_client_bronze.delete_file()
    except ResourceNotFoundError:
        # On log juste, mais on ne considère pas ça comme une erreur bloquante
        print(f"⚠️ Fichier bronze absent pour {usage_data_id_primaire}, déjà supprimé ?")

    return {
        "result": True,
        "message": f"Usage data {usage_data_id_primaire} supprimée avec succés",
    }


##----------------------------------------------------------
##-----------------MAJ---------------------------
#------------------------------------------------------

@router.patch("/update/{usage_data_id_primaire}", status_code=200)
def update_usage_data(usage_data_id_primaire: str, payload: UsageDataCreate):

    # 🔎 0) Vérifier la forme de id_building_primaire SI on le met à jour
    if payload.id_building_primaire is not None:
        if not re.fullmatch(r"building_\d{3}", payload.id_building_primaire):
            raise HTTPException(
                status_code=400,
                detail=(
                    "Format invalide pour id_building_primaire. "
                    "La forme attendue est : building_XXX (3 chiffres)."
                ),
            )
        
     # 0.b) existence dans la silver building
        if not building_exists_in_silver(payload.id_building_primaire):
            raise HTTPException(
                status_code=400,
                detail=(
                    "Le id_building_primaire fourni n'existe pas en silver. "
                    "Merci de créer d'abord le building correspondant."
                ),
            )
    
    df = load_usage_data_silver()

    if df.empty or "usage_data_id_primaire" not in df.columns:
        raise HTTPException(status_code=404, detail="Aucune donnée usage_data en silver")

    # trouver la ligne à mettre à jour
    mask = df["usage_data_id_primaire"] == usage_data_id_primaire
    if not mask.any():
        raise HTTPException(status_code=404, detail="Usage data non trouvée")

    idx = df.index[mask][0]

    # payload -> dict JSON-friendly (id_building_primaire, type, date, value)
    new_data = payload.model_dump(mode="json")

    # on met à jour uniquement les colonnes existantes dans le DF
    for col, value in new_data.items():
        if col in df.columns:
            df.at[idx, col] = value

    # on force l'id primaire à rester le même
    df.at[idx, "usage_data_id_primaire"] = usage_data_id_primaire

    # on met à jour le received_at
    received_at = datetime.now(timezone.utc)
    df.at[idx, "received_at"] = received_at

    # sauvegarde parquet silver
    save_usage_data_silver(df)

    return {
        "result": True,
        "message": "Mise à jour de l'usage data réussie.",
        "usage_data_id_primaire": usage_data_id_primaire,
        "received_at": received_at.isoformat(),
    }
