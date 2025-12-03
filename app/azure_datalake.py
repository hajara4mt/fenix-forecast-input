from typing import Dict
import json

from azure.storage.filedatalake import DataLakeServiceClient

from config import (
    AZURE_STORAGE_ACCOUNT_NAME,
    AZURE_STORAGE_ACCOUNT_KEY,
    AZURE_STORAGE_FILESYSTEM,
)

def get_datalake_client() -> DataLakeServiceClient:
    """
    Crée un client DataLake connecté à ton compte Azure Storage.
    """
    return DataLakeServiceClient(
        account_url=f"https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
        credential=AZURE_STORAGE_ACCOUNT_KEY,
    )

def write_json_to_bronze(entity: str, file_name: str, data: Dict):
    """
    Écrit un JSON brut dans la zone bronze, dans le dossier de l'entité.
    - entity : "building", "deliverypoint", "invoice", "usage_data"
    - file_name : ex. "building_000001.json"
    - data : dict Python (sera converti en JSON)
    """
    service_client = get_datalake_client()
    filesystem_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    # Exemple de chemin : "bronze/building"
    directory_path = f"bronze/{entity}"
    directory_client = filesystem_client.get_directory_client(directory_path)

    # Crée le dossier s'il n'existe pas (sinon, ne plante pas)
    directory_client.create_directory()

    # Chemin complet du fichier à l'intérieur du dossier
    file_client = directory_client.get_file_client(file_name)

    # Transforme le dict en JSON (bytes)
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

    # Upload (overwrite=True si on réécrit un fichier du même nom)
    file_client.upload_data(payload, overwrite=True)

## suprrimer le fichier de json 

def delete_file_from_bronze(entity: str, file_name: str):
    
    service = get_datalake_client()
    fs = service.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    path = f"bronze/{entity}/{file_name}"
    file_client = fs.get_file_client(path)

    try:
        file_client.delete_file()
        return True
    except Exception as e:
        print(f"⚠️ Impossible de supprimer {path} : {e}")
        return False
