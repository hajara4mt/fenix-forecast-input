# app/routers/building.py
from fastapi import APIRouter
from uuid import uuid4
from datetime import datetime, timezone
from fastapi import HTTPException , status
from app.models import BuildingCreate, BuildingCreatedResponse, BuildingRead
from app.azure_datalake import write_json_to_bronze , delete_file_from_bronze
import pandas as pd
from app.azure_datalake import get_datalake_client
from config import AZURE_STORAGE_FILESYSTEM
import re




SILVER_BUILDING_PATH = "silver/building/building.parquet"

####Regarde dans bronze/building/ tous les fichiers du type building_XXXXXX.json,et retourne le plus grand numéro trouvé. Si aucun fichier, retourne 0 

def initialize_building_index_from_bronze() -> int:
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    max_index = 0

    try:
        paths = fs.get_paths("bronze/building")
    except Exception as e:
        print(f"⚠️ Impossible de lister bronze/building : {e}")
        return 0

    pattern = re.compile(r"building_(\d{6})\.json")

    for p in paths:
        if p.is_directory:
            continue

        filename = p.name.split("/")[-1]
        match = pattern.match(filename)
        if match:
            num_str = match.group(1)
            try:
                num = int(num_str)
                if num > max_index:
                    max_index = num
            except ValueError:
                continue

    return max_index

##lire les jsons du Datalak 

def load_building_silver() -> pd.DataFrame:
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    file = fs.get_file_client(SILVER_BUILDING_PATH)

    data = file.download_file().readall()
    with open("tmp_building_silver.parquet", "wb") as f:
        f.write(data)

    return pd.read_parquet("tmp_building_silver.parquet")



def save_building_silver(df):
    """
    Réécrit tout le parquet silver/building/building.parquet
    à partir du DataFrame fourni.
    """
    # écrire en local
    tmp_path = "tmp_building_silver.parquet"
    df.to_parquet(tmp_path, index=False)

    # uploader vers ADLS
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    file = fs.get_file_client(SILVER_BUILDING_PATH)

    with open(tmp_path, "rb") as f:
        data = f.read()

    file.upload_data(data, overwrite=True)





router = APIRouter(
    prefix="/building",
    tags=["building"],
)

current_building_index = 0  

## Création de batiment et mise en place d'un ID_primaire 
@router.put("/create", status_code=201)
def create_building(payload: BuildingCreate):
    global current_building_index

    # 1) Générer un id primaire
    current_building_index = initialize_building_index_from_bronze() + 1
    building_id = f"building_{current_building_index:03d}"

    # 2) Préparer les données brutes à stocker (dict)
    received_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    raw_dict = payload.model_dump(mode="json")
    raw_dict["id_building_primaire"] = building_id
    raw_dict["received_at"] = received_at

    # 3) Écrire en zone bronze dans le dossier "building"
    #    => bronze/building/building_000001.json
    write_json_to_bronze(
        entity="building",
        file_name=f"{building_id}.json",
        data=raw_dict,
    )

    # 4) Réponse API
    return {
        "result": True,
        "id_building_primaire": building_id,
        "received_at": received_at,
        "schema_version": 1,
    }


## Get collection Building 

@router.get("/all", response_model=list[BuildingRead])
def get_building_collection():
    df = load_building_silver()
    return df.to_dict(orient="records")


## Get single Building 

@router.get("/{id_building_primaire}", response_model=BuildingRead)
def get_building_single(building_id: str):
    df = load_building_silver()
    row = df[df["id_building_primaire"] == building_id]

    if row.empty:
        raise HTTPException(status_code=404, detail="Building non trouvé")

    return row.iloc[0].to_dict()

###Route de suppression de batiment 

@router.delete( "/{id_building_primaire}", status_code=200, summary="Suppression de bâtiment")
def delete_building(building_id: str):
    """
    Supprime un building de la zone silver (Parquet) à partir de son id_building_primaire.
    """
    df = load_building_silver()

    # On filtre les lignes qui NE sont PAS ce building_id
    mask = df["id_building_primaire"] != building_id
    df_filtered = df[mask]

    # Si rien n'a été supprimé, c'est que l'id n'existait pas
    if len(df_filtered) == len(df):
        raise HTTPException(status_code=404, detail="Id_Building_Primaire non trouvé, suppression impossible")

    # On réécrit le parquet avec le DF filtré
    save_building_silver(df_filtered)

    # suppression du fichier JSON en bronze
    delete_file_from_bronze("building", f"{building_id}.json")

    return {
    "result": True,
    "message": "Le bâtiment a été supprimé avec succès."}

## Mise à jour des inputs d'un Building et du cup la mise à Jour dans le fichier parquet de la Silver 

@router.patch("/update/{id_building_primaire}", status_code=200)
def update_building(id_building_primaire: str, payload: BuildingCreate):
    df = load_building_silver()

    mask = df["id_building_primaire"] == id_building_primaire
    if not mask.any():
        raise HTTPException(status_code=404, detail="Building non trouvé")

    idx = df.index[mask][0]

    new_data = payload.model_dump(mode="json")
    for col, value in new_data.items():
        if col in df.columns:
            df.at[idx, col] = value

    df.at[idx, "id_building_primaire"] = id_building_primaire
    received_at = datetime.now(timezone.utc)
    df.at[idx, "received_at"] = received_at

    save_building_silver(df)

    return {
        "result": True,
        "message": "Mise à jour du bâtiment réussie.",
        "id_building_primaire": id_building_primaire,
        "received_at": received_at.isoformat(),
    }