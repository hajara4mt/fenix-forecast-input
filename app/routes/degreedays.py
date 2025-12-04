# app/routers/degreedays.py

from fastapi import APIRouter, Query, HTTPException , BackgroundTasks
from datetime import date, datetime, timezone
from typing import List, Dict
from collections import defaultdict
from app.jobs.degreedays_silver import run_degreedays_silver_job
from app.degreedays_client import get_monthly_hdd_cdd
from app.azure_datalake import write_json_to_bronze
from app.jobs.degreedays_silver import ensure_degreedays_for_station
from app.jobs.degreedays_silver import load_degreedays_silver


router = APIRouter(
    prefix="/degreedays",
    tags=["degreedays"],
)


@router.get("/all")
def get_all_degreedays():
    """
    Retourne toutes les lignes de la silver degreedays :
    - station_id
    - year, month, period_month
    - indicator (hdd/cdd)
    - basis
    - value
    - received_at
    """
    df = load_degreedays_silver()

    if df.empty:
        return []

    # On renvoie chaque ligne comme un dict JSON
    return df.to_dict(orient="records")


@router.get("/monthly")
def get_monthly(
    station_id: str = Query(..., description="Code station météo, ex: LFML"),
    start: date = Query(..., description="Date de début au format YYYY-MM-DD"),
    end: date = Query(..., description="Date de fin au format YYYY-MM-DD"),
    background_tasks: BackgroundTasks = None,

):
   
 ###   Appelle l'API DegreeDays pour une station et une période,
#    renvoie les données ET les stocke en zone bronze de cette façon :

 #     bronze/degreedays/<year>/<month>/dd_<station>_<year>_<month>.json
 #   """

    try:
        # 1) Récupération des données auprès de DegreeDays
        data: List[Dict] = get_monthly_hdd_cdd(
            station_id=station_id,
            start=start,
            end=end,
        )
    except Exception as e:
        # Si l'appel DegreeDays plante, on renvoie une erreur claire
        raise HTTPException(status_code=500, detail=f"Erreur DegreeDays: {e}")

    # Si aucune donnée, on renvoie juste une réponse vide
    if not data:
        return []

    # 2) On groupe par mois "YYYY-MM" (clé 'month' renvoyée par degreedays_client)
    by_month: Dict[str, List[Dict]] = defaultdict(list)
    for row in data:
        month_key = row.get("month")
        if not month_key:
            # si jamais 'month' n'est pas là, on ignore la ligne
            continue
        by_month[month_key].append(row)

    received_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 3) Pour chaque mois, on écrit un fichier JSON dans ADLS
    for month_key, rows in by_month.items():
        # month_key du type "2024-01"
        try:
            year_str, month_str = month_key.split("-")
        except ValueError:
            # si jamais le format n'est pas "YYYY-MM"
            continue

        # dossier : bronze/degreedays/<year>/<month>/
        # ex: bronze/degreedays/2024/01/
        entity_path = f"degreedays/{year_str}/{month_str}"

        # fichier : dd_<station>_<year>_<month>.json
        # ex: dd_LFML_2024_01.json
        file_name = f"dd_{station_id}_{year_str}_{month_str}.json"

        payload = {
            "station_id": station_id,
            "year": int(year_str),
            "month": int(month_str),
            "data": rows,          # toutes les lignes HDD/CDD pour ce mois
            "received_at": received_at,
        }

        # écriture dans ADLS (zone bronze)
        write_json_to_bronze(
            entity=entity_path,
            file_name=file_name,
            data=payload,
        )

        # 4) Lancer le job bronze -> silver en tâche de fond
    if background_tasks is not None:
        background_tasks.add_task(run_degreedays_silver_job)
    
   

    # 5) On renvoie les données comme avant
    return data
