##routes/buildingpoints
from fastapi import APIRouter, HTTPException , status , BackgroundTasks
from datetime import datetime, timezone
import io , re
import pandas as pd
from app.azure_datalake import write_json_to_bronze , delete_file_from_bronze
from app.utils import next_deliverypoint_index_for_building
from app.azure_datalake import get_datalake_client, AZURE_STORAGE_FILESYSTEM
from app.models import DeliveryPointCreate, DeliveryPointRead
from app.jobs.deliverypoint_silver import run_deliverypoint_silver_job
from app.utils import (
    building_exists_in_silver,
    load_deliverypoint_silver, save_deliverypoint_silver,
    delete_invoices_for_deliverypoint,
)





SILVER_DELIVERYPOINT_PATH = "silver/deliverypoint/deliverypoint.parquet"


def load_deliverypoint_silver() -> pd.DataFrame:
    """
    Lit le parquet silver des deliverypoints et renvoie un DataFrame.
    Si le fichier n'existe pas encore, renvoie un DF vide avec le bon schéma.
    """
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    file_client = fs.get_file_client(SILVER_DELIVERYPOINT_PATH)

    try:
        download = file_client.download_file()
    except Exception:
        # pas encore de fichier silver -> DF vide
        cols = [
            "deliverypoint_id_primaire",
            "id_building_primaire",
            "deliverypoint_code",
            "deliverypoint_number",
            "fluid",
            "fluid_unit",
            "received_at",
        ]
        return pd.DataFrame(columns=cols)

    data = download.readall()
    with io.BytesIO(data) as buffer:
        df = pd.read_parquet(buffer)

    return df


def save_deliverypoint_silver(df: pd.DataFrame) -> None:
    """
    Ecrit le DataFrame en parquet dans silver/deliverypoint.
    """
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    file_client = fs.get_file_client(SILVER_DELIVERYPOINT_PATH)

    with io.BytesIO() as buffer:
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        file_client.upload_data(buffer.read(), overwrite=True)


def deliverypoint_exists_in_silver(dp_id: str) -> bool:
    
    try:
        df_dp = load_deliverypoint_silver()
    except Exception:
        return False

    if df_dp.empty or "deliverypoint_id_primaire" not in df_dp.columns:
        return False

    return dp_id in df_dp["deliverypoint_id_primaire"].values


router = APIRouter(
    prefix="/deliverypoint",
    tags=["deliverypoint"],
)


@router.put("/create", status_code=201)
def create_deliverypoint(payload: DeliveryPointCreate , background_tasks: BackgroundTasks):

    # 1) on récupère l'id du building
    building_id = payload.id_building_primaire

    try:
        # 2) on calcule le prochain index pour ce building
        next_idx = next_deliverypoint_index_for_building(building_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # building_id = 'building_000003' -> '000003'
    building_suffix = building_id.split("_", 1)[1]

    # 3) on construit l'id primaire du deliverypoint
    #    ex: deliverypoint_000003_001
    deliverypoint_id = f"deliverypoint_{building_suffix}_{next_idx:03d}"

    # 4) timestamp
    received_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 5) préparer le JSON à stocker en bronze (dict "JSON-friendly")
    raw_dict = payload.model_dump(mode="json")
    raw_dict["deliverypoint_id_primaire"] = deliverypoint_id
    raw_dict["received_at"] = received_at

    # 6) écrire dans bronze/deliverypoint/deliverypoint_000003_01.json
    write_json_to_bronze(
        entity="deliverypoint",
        file_name=f"{deliverypoint_id}.json",
        data=raw_dict,
    )

    # 🔁 7) lancer le job bronze -> silver en tâche de fond
    background_tasks.add_task(run_deliverypoint_silver_job)

    # 7) réponse API
    return {
        "result": True,
        "deliverypoint_id_primaire": deliverypoint_id,
        "received_at": received_at,
        "MAE": "à calculer",
        "MAPE": "à calculer",
        "ME": "à calculer",
        "MPE": "à calculer",
        "R2": "à calculer",
        "RMSE": "à calculer",
        "consumption_reference": "à calculer",
        "a_coefficient_CDD": "à calculer",
        "a_coefficient_HDD": "à calculer",
        "b_coefficient": "à calculer"
    }


# ------------------------
#  GET COLLECTION
# ------------------------

@router.get("/all", response_model=list[DeliveryPointRead])
def get_all_deliverypoints():
    """
    Retourne la liste de tous les deliverypoints (silver).
    """
    df = load_deliverypoint_silver()

    if df.empty:
        return []

    records = df.to_dict(orient="records")
    return records


# ------------------------
#  GET SINGLE
# ------------------------

@router.get("/{deliverypoint_id_primaire}", response_model=DeliveryPointRead)
def get_deliverypoint(deliverypoint_id_primaire: str):
    """
    Retourne un deliverypoint par son id primaire (silver).
    """
    df = load_deliverypoint_silver()

    mask = df["deliverypoint_id_primaire"] == deliverypoint_id_primaire
    if not mask.any():
        raise HTTPException(status_code=404, detail="DeliveryPoint non trouvé")

    row = df.loc[mask].iloc[0].to_dict()
    return row

# ------------------------
# Supprimer DeliveryPoint
# ------------------------

@router.delete("/{deliverypoint_id_primaire}", status_code=200)
def delete_deliverypoint(deliverypoint_id_primaire: str):

    # ✅ cascade invoices
    nb_invoices_deleted = delete_invoices_for_deliverypoint(deliverypoint_id_primaire)

    # supprimer le deliverypoint en silver
    df = load_deliverypoint_silver()
    if df.empty or "deliverypoint_id_primaire" not in df.columns:
        raise HTTPException(status_code=404, detail="Aucun deliverypoint en silver.")

    mask_keep = df["deliverypoint_id_primaire"].astype(str) != str(deliverypoint_id_primaire)
    df_filtered = df.loc[mask_keep].copy()

    if len(df_filtered) == len(df):
        raise HTTPException(status_code=404, detail="DeliveryPoint non trouvé")

    save_deliverypoint_silver(df_filtered)

    # supprimer bronze JSON du deliverypoint
    delete_file_from_bronze("deliverypoint", f"{deliverypoint_id_primaire}.json")

    return {
        "result": True,
        "message": "DeliveryPoint supprimé (cascade invoices appliquée).",
        "invoices_deleted": nb_invoices_deleted,
    }

##------------------------------------------#
##Mise à jour de deliverypoint 
##------------------------------------------#

# ...

@router.patch(
    "/update/{deliverypoint_id_primaire}",
    status_code=status.HTTP_200_OK,
)
def update_deliverypoint(
    deliverypoint_id_primaire: str,
    payload: DeliveryPointCreate,
):
    """
    Mise à jour des données d’un deliverypoint 
    """

      # 🔎 0) Vérifier la forme de id_building_primaire SI on le met à jour
    # (DeliveryPointCreate a forcément ce champ, donc on le valide)
    if payload.id_building_primaire is not None:
        if not re.fullmatch(r"building_\d{3}", payload.id_building_primaire):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
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

    # 1) Charger la silver
    df = load_deliverypoint_silver()

    if df.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Aucun deliverypoint enregistré."
        )

    # 2) Filtrer sur l'id primaire
    mask = df["deliverypoint_id_primaire"] == deliverypoint_id_primaire
    if not mask.any():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="DeliveryPoint non trouvé."
        )

    # 3) Nouvelle horodatation
    received_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 4) Préparer le dict avec les nouvelles données
    new_data = payload.model_dump(mode="python")
    new_data["deliverypoint_id_primaire"] = deliverypoint_id_primaire
    new_data["received_at"] = received_at

    # 5) Mettre à jour colonne par colonne
    for col, value in new_data.items():
        if col in df.columns:
            df.loc[mask, col] = value

    # 6) Sauvegarder la silver mise à jour
    save_deliverypoint_silver(df)

    # 7) Mettre à jour le JSON de bronze
    write_json_to_bronze(
        entity="deliverypoint",
        file_name=f"{deliverypoint_id_primaire}.json",
        data=new_data,
    )

    # 8) Réponse "mise à jour réussie"
    return {
        "result": True,
        "message": "DeliveryPoint mis à jour avec succès.",
        "deliverypoint_id_primaire": deliverypoint_id_primaire,
        "received_at": received_at,
    }