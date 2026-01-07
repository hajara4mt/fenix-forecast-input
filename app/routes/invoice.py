###routes/invoices.py
import re , io , pandas as pd
from fastapi import APIRouter, HTTPException, status , BackgroundTasks, Query
from datetime import datetime, timezone
from typing import List , Optional
from pydantic import BaseModel
from app.models import InvoiceCreate , InvoiceRead
from app.azure_datalake import write_json_to_bronze, get_datalake_client , delete_file_from_bronze
from config import AZURE_STORAGE_FILESYSTEM
from app.jobs.invoice_silver import run_invoice_silver_job
from app.utils import deliverypoint_exists_in_silver


SILVER_INVOICE_PATH = "silver/invoice/invoice.parquet"

def _load_invoice_silver_df() -> pd.DataFrame:
    """
    Lit silver/invoice/invoice.parquet dans un DataFrame.
    Soulève une HTTPException 500 si problème d'accès.
    """
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    try:
        file_client = fs.get_file_client(SILVER_INVOICE_PATH)
        download = file_client.download_file()
        raw_bytes = download.readall()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Impossible de lire les factures en silver : {e}",
        )

    if not raw_bytes:
        return pd.DataFrame()

    return pd.read_parquet(io.BytesIO(raw_bytes))




class InvoiceCollectionResponse(BaseModel):
    items: List[InvoiceRead]
    message: Optional[str] = None


router = APIRouter(
    prefix="/invoice",
    tags=["invoice"],
)


def parse_deliverypoint_id(dp_id: str):
    parts = dp_id.split("_")
    if len(parts) != 3:
        raise ValueError("deliverypoint_id_primaire invalide")
    return parts[1], parts[2]  # buildingSuffix, deliverypointSuffix


def get_next_invoice_index(building_suffix: str, dp_suffix: str) -> int:

    expected_prefix = f"invoice_{building_suffix}_{dp_suffix}_"

    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    try:
        paths = fs.get_paths("bronze/invoice")
    except Exception:
        return 1  # aucun fichier → première facture

    pattern = re.compile(
        rf"invoice_{building_suffix}_{dp_suffix}_(\d+)\.json"
    )

    max_idx = 0

    for p in paths:
        if p.is_directory:
            continue

        filename = p.name.split("/")[-1]
        match = pattern.match(filename)

        if match:
            idx = int(match.group(1))
            max_idx = max(max_idx, idx)

    return max_idx + 1

def _save_invoice_silver_df(df: pd.DataFrame) -> None:
    """
    Sauvegarde le DataFrame dans silver/invoice/invoice.parquet.
    """
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    file_client = fs.get_file_client(SILVER_INVOICE_PATH)

    # on écrit en mémoire plutôt qu'en fichier local
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    file_client.upload_data(buffer.read(), overwrite=True)


def delete_invoices_for_deliverypoint(dp_id: str) -> int:
    """
    Supprime toutes les invoices liées à un deliverypoint :
    - silver/invoice (parquet)
    - bronze/invoice (json)
    Retourne le nombre d'invoices supprimées.
    """
    df = _load_invoice_silver_df()

    if df.empty or "deliverypoint_id_primaire" not in df.columns:
        return 0

    mask = df["deliverypoint_id_primaire"].astype(str) == str(dp_id)
    df_to_delete = df.loc[mask].copy()

    if df_to_delete.empty:
        return 0

    # 1) supprimer dans silver
    df_filtered = df.loc[~mask].copy()
    _save_invoice_silver_df(df_filtered)

    # 2) supprimer les JSON bronze
    if "invoice_id_primaire" in df_to_delete.columns:
        for inv_id in df_to_delete["invoice_id_primaire"].astype(str).tolist():
            delete_file_from_bronze("invoice", f"{inv_id}.json")

    return len(df_to_delete)


@router.put("/create", status_code=status.HTTP_201_CREATED)
def create_invoice(payload: InvoiceCreate , background_tasks: BackgroundTasks):

    try:
        building_suffix, dp_suffix = parse_deliverypoint_id(
            payload.deliverypoint_id_primaire
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # compute next invoice index
    next_invoice_idx = get_next_invoice_index(building_suffix, dp_suffix)

    # build invoice id
    invoice_id = (
        f"invoice_{building_suffix}_{dp_suffix}_{next_invoice_idx:02d}"
    )

    # timestamp
    received_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # prepare JSON
    raw_dict = payload.model_dump(mode="json")
    raw_dict["invoice_id_primaire"] = invoice_id
    raw_dict["received_at"] = received_at

    # write bronze
    write_json_to_bronze(
        entity="invoice",
        file_name=f"{invoice_id}.json",
        data=raw_dict,
    )  

    # 🔁 lancer le job bronze -> silver en tâche de fond
    background_tasks.add_task(run_invoice_silver_job)

    return {
        "result": True,
        "invoice_id_primaire": invoice_id,
        "received_at": received_at,
    }

######################################### ########################################
####Tous les invoices selon le deliverypoint ########################################
####################################################################################
#@router.get("/all", response_model=List[InvoiceRead])
#def get_all_invoices():
   # """   Retourne toutes les factures présentes dans silver/invoice/invoice.parquet."""
    #df = _load_invoice_silver_df()

    #if df.empty:
     #   return []

    # Tri optionnel pour plus de lisibilité
#    if "invoice_id_primaire" in df.columns:
#      df = df.sort_values("invoice_id_primaire")
  #  records = df.to_dict(orient="records")
 #   return [InvoiceRead(**r) for r in records]


@router.get(
    "/all",
    response_model=InvoiceCollectionResponse,
    summary="Lister les invoices d’un deliverypoint"
)
def get_invoices_by_deliverypoint(
    deliverypoint_id_primaire: str = Query(..., description="ID primaire du deliverypoint")
):
    # 1) Vérifier deliverypoint
    if not deliverypoint_exists_in_silver(deliverypoint_id_primaire):
        raise HTTPException(
            status_code=404,
            detail=f"DeliveryPoint {deliverypoint_id_primaire} introuvable en silver."
        )

    # 2) Charger silver invoice
    df = _load_invoice_silver_df()

    if df.empty:
        return {
            "items": [],
            "message": f"Le deliverypoint {deliverypoint_id_primaire} contient 0 invoice."
        }

    if "deliverypoint_id_primaire" not in df.columns:
        raise HTTPException(
            status_code=500,
            detail="Colonne deliverypoint_id_primaire absente du parquet silver/invoice."
        )

    # 3) Filtrer
    df_dp = df[df["deliverypoint_id_primaire"].astype(str) == str(deliverypoint_id_primaire)].copy()
    count = len(df_dp)

    if df_dp.empty:
        return {
            "items": [],
            "message": f"Le deliverypoint {deliverypoint_id_primaire} contient 0 invoice."
        }

    # (optionnel) tri lisible
    if "invoice_id_primaire" in df_dp.columns:
        df_dp = df_dp.sort_values("invoice_id_primaire")

    return {
        "items": df_dp.to_dict(orient="records"),
        "message": f"Le deliverypoint {deliverypoint_id_primaire} contient {count} invoice(s)."
    }




@router.get("/{invoice_id_primaire}", response_model=InvoiceRead)
def get_invoice(invoice_id_primaire: str):
    """
    Retourne une facture unique à partir de son id primaire.
    """
    df = _load_invoice_silver_df()

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune facture disponible dans la zone silver.",
        )

    if "invoice_id_primaire" not in df.columns:
        raise HTTPException(
            status_code=500,
            detail="Colonne invoice_id_primaire absente du parquet silver.",
        )

    mask = df["invoice_id_primaire"] == invoice_id_primaire

    if not mask.any():
        raise HTTPException(
            status_code=404,
            detail=f"Invoice {invoice_id_primaire} introuvable.",
        )

    row = df.loc[mask].iloc[0]
    return InvoiceRead(**row.to_dict())


@router.delete("/{invoice_id_primaire}", status_code=status.HTTP_200_OK)
def delete_invoice(invoice_id_primaire: str):
  
    # 1) Charger la silver
    try:
        df = _load_invoice_silver_df()
    except HTTPException as e:
        # si problème de lecture, on propage
        raise e

    if df.empty or "invoice_id_primaire" not in df.columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Aucune invoice disponible en silver.",
        )

    # 2) Filtrer pour enlever cette invoice
    mask_keep = df["invoice_id_primaire"] != invoice_id_primaire
    df_filtered = df[mask_keep]

    # Si aucune ligne n'a été supprimée → l'id n'existe pas
    if len(df_filtered) == len(df):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Invoice {invoice_id_primaire} introuvable.",
        )

    # 3) Sauvegarder la silver sans cette invoice
    _save_invoice_silver_df(df_filtered)

    # 4) Supprimer aussi le JSON en bronze
    delete_file_from_bronze(
        entity="invoice",
        file_name=f"{invoice_id_primaire}.json",
    )

    # 5) Réponse
    return {
        "result": True,
        "message": "L'invoice a été supprimé avec succès.",
    }

@router.patch("/update/{invoice_id_primaire}", status_code=status.HTTP_200_OK)
def update_invoice(invoice_id_primaire: str, payload: InvoiceCreate):


    # 0) Vérifier la forme de deliverypoint_id_primaire SI on le met à jour
    if payload.deliverypoint_id_primaire is not None:
        if not re.fullmatch(r"deliverypoint_\d{3}_\d{3}", payload.deliverypoint_id_primaire):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "Format invalide pour deliverypoint_id_primaire. "
                    "La forme attendue est : deliverypoint_XXX_YYY "
                    "(3 chiffres pour le building, 3 pour l'index)."
                ),
            )
    
    if not deliverypoint_exists_in_silver(payload.deliverypoint_id_primaire):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "Le deliverypoint_id_primaire fourni n'existe pas en silver. "
                    "Merci de créer d'abord le deliverypoint correspondant."
                ),
            )

    # 1) Charger la silver
    df = _load_invoice_silver_df()

    if df.empty or "invoice_id_primaire" not in df.columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Aucune invoice disponible en silver."
        )

    # 2) Vérifier que l'ID existe
    mask = df["invoice_id_primaire"] == invoice_id_primaire
    if not mask.any():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Invoice {invoice_id_primaire} introuvable."
        )

    # 3) Nouveau timestamp
    now_dt = datetime.now(timezone.utc)
    received_at_str = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # 4) Préparer les données "python" pour la silver
    new_data_python = payload.model_dump(mode="python")
    new_data_python["invoice_id_primaire"] = invoice_id_primaire
    new_data_python["received_at"] = now_dt

    # Mise à jour colonne par colonne dans le DataFrame
    for col, value in new_data_python.items():
        if col in df.columns:
            df.loc[mask, col] = value

    # 5) Sauvegarder la silver mise à jour
    _save_invoice_silver_df(df)

    # 6) Préparer les données "json-friendly" pour la bronze
    new_data_json = payload.model_dump(mode="json")
    new_data_json["invoice_id_primaire"] = invoice_id_primaire
    new_data_json["received_at"] = received_at_str

    # Écraser le fichier JSON de bronze correspondant
    write_json_to_bronze(
        entity="invoice",
        file_name=f"{invoice_id_primaire}.json",
        data=new_data_json,
    )

    # 7) Réponse
    return {
        "result": True,
        "message": "L'invoice a été mise à jour avec succès.",
        "invoice_id_primaire": invoice_id_primaire,
        "received_at": received_at_str,
    }