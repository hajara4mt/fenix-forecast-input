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
    pattern = re.compile(rf"deliverypoint_{building_suffix}_(\d{{2}})\.json")

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
