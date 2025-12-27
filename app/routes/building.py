# app/routers/building.py
from fastapi import APIRouter ,  BackgroundTasks , HTTPException , status
import numpy as np  # <-- ajoute ça en haut du fichier si pas déjà fait
from app.utils import building_business_key_exists_in_silver
from uuid import uuid4
from datetime import datetime, timezone
from app.models import BuildingCreate, BuildingCreatedResponse, BuildingRead
from app.azure_datalake import write_json_to_bronze , delete_file_from_bronze
import pandas as pd
from app.azure_datalake import get_datalake_client
from config import AZURE_STORAGE_FILESYSTEM
import re
from app.jobs.building_silver import run_building_silver_job
from app.jobs.degreedays_silver import ensure_degreedays_for_station
 # adapte si ton fichier s'appelle usagedata.py
from app.azure_datalake import delete_file_from_bronze
from app.utils import (
    load_building_silver, save_building_silver, building_exists_in_silver,
    get_deliverypoints_for_building,
    delete_invoices_for_deliverypoint,
    delete_usage_data_for_building,
    load_deliverypoint_silver, save_deliverypoint_silver,
)






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

    pattern = re.compile(r"building_(\d{3})\.json")

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

# load_building_silver() -> pd.DataFrame:
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    file = fs.get_file_client(SILVER_BUILDING_PATH)

    data = file.download_file().readall()
    with open("tmp_building_silver.parquet", "wb") as f:
        f.write(data)

    return pd.read_parquet("tmp_building_silver.parquet")



#def save_building_silver(df):
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


## chemin de test de presence d'un id building dans la silver 
#def building_exists_in_silver(building_id: str) -> bool:
    """
    Retourne True si id_building_primaire existe dans la silver building.
    """
    try:
        df_building = load_building_silver()
    except Exception:
        return False

    if df_building.empty or "id_building_primaire" not in df_building.columns:
        return False

    return building_id in df_building["id_building_primaire"].values



router = APIRouter(
    prefix="/building",
    tags=["building"],
)

#current_building_index = 0  

#def delete_usage_data_for_building(building_id: str) -> int:
#    df = load_usage_data_silver()
 #   if df.empty or "id_building_primaire" not in df.columns:
  #      return 0

   # mask = df["id_building_primaire"].astype(str) == str(building_id)
   # df_to_delete = df.loc[mask].copy()
    #if df_to_delete.empty:
     #   return 0

    # silver
    #df_filtered = df.loc[~mask].copy()
    #save_usage_data_silver(df_filtered)

    # bronze
  #  if "usage_data_id_primaire" in df_to_delete.columns:
   ##     for ud_id in df_to_delete["usage_data_id_primaire"].astype(str).tolist():
     #       delete_file_from_bronze("usage_data", f"{ud_id}.json")

    #return len(df_to_delete)


#def delete_deliverypoints_for_building(building_id: str) -> list[str]:
  #  """
  #  Supprime tous les deliverypoints liés au building :
   # - silver/deliverypoint
    #- bronze/deliverypoint
    #Retourne la liste des dp_ids supprimés (utile pour delete invoices).
  #  """
   # df_dp = load_deliverypoint_silver()
    #if df_dp.empty or "id_building_primaire" not in df_dp.columns:
     #   return []

    #mask = df_dp["id_building_primaire"].astype(str) == str(building_id)
    #df_to_delete = df_dp.loc[mask].copy()
    #if df_to_delete.empty:
     #   return []

    #dp_ids = df_to_delete["deliverypoint_id_primaire"].astype(str).tolist()

    # silver
    #df_filtered = df_dp.loc[~mask].copy()
   # save_deliverypoint_silver(df_filtered)

    # bronze
    #for dp_id in dp_ids:
    #    delete_file_from_bronze("deliverypoint", f"{dp_id}.json")

    #return dp_ids


#def delete_invoices_for_deliverypoints(dp_ids: list[str]) -> int:
  ##  """
  #  Supprime toutes les invoices liées à une liste de deliverypoints
#     """
# #    if not dp_ids:
#         return 0

#     df_inv = _load_invoice_silver_df()
#     if df_inv.empty or "deliverypoint_id_primaire" not in df_inv.columns:
#         return 0

#     mask = df_inv["deliverypoint_id_primaire"].astype(str).isin(set(map(str, dp_ids)))
#     df_to_delete = df_inv.loc[mask].copy()

#     if df_to_delete.empty:
#         return 0

#     # silver
#     df_filtered = df_inv.loc[~mask].copy()
#     _save_invoice_silver_df(df_filtered)

#     # bronze
#     if "invoice_id_primaire" in df_to_delete.columns:
#         for inv_id in df_to_delete["invoice_id_primaire"].astype(str).tolist():
#             delete_file_from_bronze("invoice", f"{inv_id}.json")

#     return len(df_to_delete)
# ##


## Création de batiment et mise en place d'un ID_primaire 
@router.put("/create", status_code=201)
def create_building(payload: BuildingCreate, background_tasks: BackgroundTasks):
    global current_building_index


    # ✅ PRE-CHECK silver : (platform_code, building_code)
    if building_business_key_exists_in_silver(
        platform_code=payload.platform_code,
        building_code=payload.building_code,
        load_building_silver_fn=load_building_silver,
    ):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Building existe déjà (platform_code, building_code) en silver.",
        )

    # 1) Générer un id primaire
    current_building_index = initialize_building_index_from_bronze() + 1
    building_id = f"building_{current_building_index:03d}"

    

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
    #4) Lancer le job bronze -> silver en tâche de fond
    background_tasks.add_task(run_building_silver_job)

    if payload.weather_station and payload.reference_period_start:
        background_tasks.add_task(
            ensure_degreedays_for_station,
            station_id=payload.weather_station,
            ref_start=payload.reference_period_start,
        )

    # 4) Réponse API
    return {
        "result": True,
        "id_building_primaire": building_id,
        "received_at": received_at,
        "schema_version": 1,
    }


## Get collection Building 

#@router.get("/all", response_model=list[BuildingRead])
#def get_building_collection():
#    df = load_building_silver()
#    return df.to_dict(orient="records")

@router.get("/all", response_model=list[BuildingRead])
def get_building_collection():
    df = load_building_silver()

    if df.empty:
        return []

    # ❌ On enlève toutes les lignes où occupant est NaN
    if "occupant" in df.columns:
        df = df[df["occupant"].notna()]
        # et on force le type int pour que Pydantic soit content
        df["occupant"] = df["occupant"].astype(int)

    

    # 3) Conversion finale en liste de dicts
    return df.to_dict(orient="records")

    


## Get single Building 

@router.get("/{id_building_primaire}", response_model=BuildingRead)
def get_building_single(id_building_primaire: str):
    df = load_building_silver()
    row = df[df["id_building_primaire"] == id_building_primaire]

    if row.empty:
        raise HTTPException(status_code=404, detail="Building non trouvé")

    return row.iloc[0].to_dict()

###Route de suppression de batiment 
@router.delete("/{id_building_primaire}", status_code=200, summary="Suppression de bâtiment (cascade)")
def delete_building(id_building_primaire: str):

    # 1) vérifier building existe
    df_b = load_building_silver()
    if df_b.empty or "id_building_primaire" not in df_b.columns:
        raise HTTPException(status_code=404, detail="Aucun building en silver.")

    if str(id_building_primaire) not in df_b["id_building_primaire"].astype(str).values:
        raise HTTPException(status_code=404, detail="Id_Building_Primaire non trouvé, suppression impossible")

    # 2) récupérer deliverypoints du building
    df_dp_b = get_deliverypoints_for_building(id_building_primaire)
    if df_dp_b.empty:
        dp_ids = []
    else:
        dp_ids = df_dp_b["deliverypoint_id_primaire"].astype(str).tolist()

        # supprimer deliverypoints en silver
        df_dp_all = load_deliverypoint_silver()
        df_dp_all = df_dp_all[df_dp_all["id_building_primaire"].astype(str) != str(id_building_primaire)].copy()
        save_deliverypoint_silver(df_dp_all)

        # supprimer deliverypoints en bronze
        for dp_id in dp_ids:
            delete_file_from_bronze("deliverypoint", f"{dp_id}.json")

    # 3) supprimer invoices pour chaque deliverypoint
    nb_invoices_deleted = 0
    for dp_id in dp_ids:
        nb_invoices_deleted += delete_invoices_for_deliverypoint(dp_id)

    # 4) supprimer usage_data du building
    nb_usage_deleted = delete_usage_data_for_building(id_building_primaire)

    # 5) supprimer building en silver
    df_b_filtered = df_b[df_b["id_building_primaire"].astype(str) != str(id_building_primaire)].copy()
    save_building_silver(df_b_filtered)

    # 6) supprimer building en bronze
    delete_file_from_bronze("building", f"{id_building_primaire}.json")

    return {
        "result": True,
        "message": "Le bâtiment a été supprimé avec succès (cascade).",
        "deliverypoints_deleted": len(dp_ids),
        "invoices_deleted": nb_invoices_deleted,
        "usage_data_deleted": nb_usage_deleted,
    }


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