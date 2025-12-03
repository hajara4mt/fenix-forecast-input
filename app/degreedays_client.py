# app/degreedays_client.py

import os
from datetime import date
from typing import List, Dict, Any

from degreedays.api import DegreeDaysApi, AccountKey, SecurityKey
from degreedays.api.data import (
    Location,
    DataSpecs,
    DataSpec,
    Calculation,
    Temperature,
    DatedBreakdown,
    Period,
    LocationDataRequest,
)
from degreedays.time import DayRange


# ----- Configuration des clés -----
ACCOUNT_KEY = os.getenv("DEGREEDAYS_ACCOUNT_KEY")
SECURITY_KEY = os.getenv("DEGREEDAYS_SECURITY_KEY")

if not ACCOUNT_KEY or not SECURITY_KEY:
    raise RuntimeError(
        "DEGREEDAYS_ACCOUNT_KEY ou DEGREEDAYS_SECURITY_KEY non définies."
    )

# ----- Client API -----
api = DegreeDaysApi.fromKeys(
    AccountKey(ACCOUNT_KEY),
    SecurityKey(SECURITY_KEY),
)

def get_monthly_hdd_cdd(
    station_id: str,
    start: date,
    end: date,
    hdd_bases: List[int] = [10, 15, 18],
    cdd_bases: List[int] = [21, 24, 26],
) -> List[Dict[str, Any]]:


    # ----- On définit la plage de dates demandée -----
    period = Period.dayRange(DayRange(start, end))

    # ----- On construit les DataSpec HDD/CDD -----
    def hdd_spec(base_c: int) -> DataSpec:
        return DataSpec.dated(
            Calculation.heatingDegreeDays(Temperature.celsius(base_c)),
            DatedBreakdown.monthly(period),
        )

    def cdd_spec(base_c: int) -> DataSpec:
        return DataSpec.dated(
            Calculation.coolingDegreeDays(Temperature.celsius(base_c)),
            DatedBreakdown.monthly(period),
        )

    hdd_specs = [hdd_spec(b) for b in hdd_bases]
    cdd_specs = [cdd_spec(b) for b in cdd_bases]

    # Combine toutes les specs
    all_specs = DataSpecs(*(hdd_specs + cdd_specs))

    # ----- On prépare la requête API -----
    location = Location.stationId(station_id)
    request = LocationDataRequest(location, all_specs)

    # ----- Appel API -----
    response = api.dataApi.getLocationData(request)

    # ----- Transformation en liste de dicts -----
    results: List[Dict[str, Any]] = []

    for spec in hdd_specs + cdd_specs:
        dataset = response.dataSets[spec]
        calc = spec.calculation

        indicator_name = "hdd" if "heating" in type(calc).__name__.lower() else "cdd"
        basis = calc.baseTemperature.value

        for v in dataset.values:
            results.append(
                {
                    "station_id": station_id,
        
                    "indicator.name": indicator_name,
                    "indicator.basis": basis,
                    "month": v.firstDay.isoformat()[:7],  # "YYYY-MM"
                    "value": v.value,
                }
            )

    return results
