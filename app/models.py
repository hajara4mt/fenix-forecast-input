from typing import Optional
from datetime import date , datetime
from typing import Annotated
from enum import Enum



from pydantic import BaseModel, Field, field_validator, ValidationInfo , model_validator

class BaseCreatedResponse(BaseModel):
    result: bool = True
    received_at: str
    schema_version: int = 1

class BuildingCreatedResponse(BaseCreatedResponse):
    id_building_primaire: str

class CompteurCreatedResponse(BaseCreatedResponse):
    compteur_id_primaire: str

class ConsoCreatedResponse(BaseCreatedResponse):
    conso_id_primaire: str

class UsageDataCreatedResponse(BaseCreatedResponse):
    usage_data_id_primaire: str

    # ---- Building (création logique côté Fenix) ----

class FluidType(str, Enum):
    elec = "elec"
    gas = "gaz"
    fod = "fod"
    heat = "heat"
    cold = "cold"
    wood = "wood"


class BuildingCreate(BaseModel):
    
    model_config = {"extra": "forbid"}

    platform_code: Annotated[str, Field(min_length=1)]
    building_code: Annotated[str, Field(min_length=1)]
    name: Annotated[str, Field(min_length=1)]

    latitude: Optional[Annotated[float, Field(ge=-90, le=90)]] = None
    longitude: Optional[Annotated[float, Field(ge=-180, le=180)]] = None

    organisation: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    zipcode: Optional[str] = None
    country: Optional[str] = None
    typology: Optional[str] = None

    geographical_area: Optional[int] = None
    occupant: Optional[Annotated[int, Field(ge=0)]] = None
    surface: Optional[Annotated[int, Field(ge=0)]] = None

    reference_period_start: Optional[date] = None
    reference_period_end: Optional[date] = None
    weather_station: Optional[str] = None

    @field_validator("reference_period_end")
    def validate_dates(cls, v, info: ValidationInfo):
        # info.data contient les autres champs déjà validés
        start = info.data.get("reference_period_start")
        if start and v and start > v:
            raise ValueError("La date de début de la période de référence doit être avant ou égale à la date de fin de la période de référence")
        return v


class DeliveryPointCreate(BaseModel):
    model_config = {"extra": "forbid"}
    id_building_primaire: Annotated[str, Field(min_length=1)]

    deliverypoint_code: Annotated[str, Field(min_length=1)]

    deliverypoint_number: Annotated[str, Field(min_length=1)]

##limiter les valeurs d'entrées de fluid aux valeurs definiées dans FluidType
    fluid: FluidType  

    fluid_unit: Annotated[str, Field(min_length=1)]


class InvoiceCreate(BaseModel):
    model_config = {"extra": "forbid"}
    # on utilise l'id primaire du deliverypoint généré par ta route précédente
    deliverypoint_id_primaire: Annotated[str, Field(min_length=1)]

    # ex: cuv2p1_building_922_deliverypoint_14_invoice_12
    invoice_code: Annotated[str, Field(min_length=1)]

    # période de la facture
    start: date   # format "YYYY-MM-DD"
    end: date     # format "YYYY-MM-DD"

    # montant / consommation (int, >= 0)
    value: Annotated[int, Field(ge=0)]

    @model_validator(mode="after")
    def check_dates(self) -> "InvoiceCreate":
        # ici self contient déjà start et end validés
        if self.end < self.start:
            raise ValueError("La date de fin (end) doit être postérieure ou égale à la date de début (start).")
        return self
    
class UsageDataCreate(BaseModel):
    model_config = {"extra": "forbid"}
    id_building_primaire: Annotated[str, Field(min_length=1)]

    # ex: "surface", "occupancy", ...
    type: Annotated[str, Field(min_length=1)]

    # ex: "2025-10-01"
    date: date  # format YYYY-MM-DD

    # ex: 1200 (>= 0)
    value: Annotated[int, Field(ge=0)]

class UsageDataRead(BaseModel):
    usage_data_id_primaire: str
    id_building_primaire: str
    type: str
    date: date
    value: int
    received_at: datetime


class BuildingRead(BaseModel):
    id_building_primaire: str
    platform_code: Optional[str] = None
    building_code: Optional[str] = None
    name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    organisation: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    zipcode: Optional[str] = None
    country: Optional[str] = None
    typology: Optional[str] = None
    geographical_area: Optional[int] = None
    occupant: Optional[int] = None
    surface: Optional[float] = None
    reference_period_start: Optional[date] = None
    reference_period_end: Optional[date] = None
    weather_station: Optional[str] = None
    received_at: Optional[datetime] = None


class DeliveryPointRead(BaseModel):
    model_config = {"extra": "forbid"}
    # id primaire interne
    deliverypoint_id_primaire: str

    # lien vers le building
    id_building_primaire: str

    # champs de création
    deliverypoint_code: str
    deliverypoint_number: str
    fluid: FluidType
    fluid_unit: str

    # metadata
    received_at: datetime

class InvoiceRead(BaseModel):
    invoice_id_primaire: str
    deliverypoint_id_primaire: str
    invoice_code: str

    # dates de facture
    start: date
    end: date

    value: Annotated[float, Field(ge=0)]
    received_at: datetime
