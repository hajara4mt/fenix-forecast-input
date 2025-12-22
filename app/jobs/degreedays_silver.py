# app/jobs/degreedays_silver.py

import json
import calendar
from typing import List, Dict
from datetime import date, datetime, timezone
from collections import defaultdict

import pandas as pd

from app.azure_datalake import get_datalake_client, write_json_to_bronze
from config import AZURE_STORAGE_FILESYSTEM
from app.degreedays_client import get_monthly_hdd_cdd  # appelle l'API DegreeDays

SILVER_DEGREEDAYS_PATH = "silver/degreedays/degreedays_monthly.parquet"


# ---------------------------
# Bronze -> Silver helpers
# ---------------------------

def load_degreedays_bronze() -> pd.DataFrame:
    """
    Lit tous les JSON sous bronze/degreedays/ (toutes années / mois)
    et retourne un DataFrame flattené niveau "data".
    """
    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    paths = fs_client.get_paths("bronze/degreedays")

    records: List[Dict] = []

    for p in paths:
        if p.is_directory:
            continue

        file_client = fs_client.get_file_client(p.name)
        download = file_client.download_file()
        content = download.readall().decode("utf-8")

        try:
            root = json.loads(content)
        except json.JSONDecodeError:
            print(f"⚠️ Fichier JSON invalide, ignoré : {p.name}")
            continue

        station_id = root.get("station_id")
        year = root.get("year")
        month = root.get("month")
        received_at = root.get("received_at")
        data_list = root.get("data", [])

        for item in data_list:
            rec = {
                "station_id": station_id,
                "year": year,
                "month": month,
                "period_month": item.get("month"),             # "YYYY-MM"
                "indicator": item.get("indicator.name"),       # hdd / cdd
                "basis": item.get("indicator.basis"),          # 10,15,18,21...
                "value": item.get("value"),
                "received_at": received_at,
            }
            records.append(rec)

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records)


def transform_degreedays(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    expected_cols = [
        "station_id",
        "year",
        "month",
        "period_month",
        "indicator",
        "basis",
        "value",
        "received_at",
    ]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["basis"] = pd.to_numeric(df["basis"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["received_at"] = pd.to_datetime(df["received_at"], errors="coerce")

    df = df.sort_values(["station_id", "period_month", "indicator", "basis", "received_at"])
    df = df.drop_duplicates(
        subset=["station_id", "period_month", "indicator", "basis"],
        keep="last",
    )

    return df[expected_cols]


def save_degreedays_silver(df: pd.DataFrame) -> None:
    if df.empty:
        print("Aucune donnée degreedays à écrire en silver.")
        return

    local_path = "degreedays_monthly.parquet"
    df.to_parquet(local_path, index=False)

    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)

    remote_path = "silver/degreedays/degreedays_monthly.parquet"
    file_client = fs_client.get_file_client(remote_path)

    with open(local_path, "rb") as f:
        data = f.read()

    file_client.upload_data(data, overwrite=True)
    print(f"✅ Fichier Parquet écrit dans {remote_path}")


def run_degreedays_silver_job():
    print("🔄 Lecture des données bronze/degreedays ...")
    df_bronze = load_degreedays_bronze()
    print(f"   {len(df_bronze)} lignes flattenées depuis bronze.")

    print("🧹 Transformation / typage / dédoublonnage ...")
    df_silver = transform_degreedays(df_bronze)
    print(f"   {len(df_silver)} lignes finales (station/mois/indicator/basis).")

    print("💾 Écriture dans silver/degreedays/degreedays_monthly.parquet ...")
    save_degreedays_silver(df_silver)

    print("✨ Job degreedays silver terminé.")


# ---------------------------
# Silver loader
# ---------------------------

def load_degreedays_silver() -> pd.DataFrame:
    """
    Charge le parquet silver des DegreeDays (si existe),
    sinon retourne un DataFrame vide.
    """
    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
    file_client = fs_client.get_file_client(SILVER_DEGREEDAYS_PATH)

    try:
        data = file_client.download_file().readall()
    except Exception:
        return pd.DataFrame()

    local_path = "degreedays_monthly_tmp.parquet"
    with open(local_path, "wb") as f:
        f.write(data)

    return pd.read_parquet(local_path)


# ---------------------------
# Utilities
# ---------------------------

def list_months_between(start: date, end: date) -> list[str]:
    """
    Retourne la liste des mois 'YYYY-MM' entre start et end (inclus).
    """
    months: list[str] = []
    current = date(start.year, start.month, 1)
    end_month = date(end.year, end.month, 1)

    while current <= end_month:
        months.append(current.strftime("%Y-%m"))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)

    return months


# ---------------------------
# ✅ SAFE ensure function (avec try/except global)
# ---------------------------

def ensure_degreedays_for_station(
    station_id: str,
    ref_start: date,
    ref_end: date | None = None,
) -> None:
    """
    Assure que pour une station donnée, tous les mois entre ref_start et AUJOURD'HUI
    sont présents dans la silver degreedays.

    IMPORTANT:
    - Cette fonction est appelée en BackgroundTask -> elle ne doit JAMAIS lever d'exception.
    """
    try:
        if ref_end is None:
            ref_end = date.today()

        wanted_months = set(list_months_between(ref_start, ref_end))

        df_silver = load_degreedays_silver()

        if df_silver.empty or "station_id" not in df_silver.columns:
            have_months = set()
        else:
            subset = df_silver[df_silver["station_id"] == station_id]
            if subset.empty:
                have_months = set()
            else:
                have_months = set(subset["period_month"].astype(str).tolist())

        missing_months = sorted(wanted_months - have_months)

        if not missing_months:
            # tu peux laisser ce log ou le supprimer
            # print(f"✅ Tous les mois sont déjà présents pour la station {station_id}.")
            return

        # ✅ c'est ce log que tu veux voir
        print(f"⚠️ Mois manquants pour {station_id} : {missing_months}")

        # Déterminer une période minimale à demander à DegreeDays
        first_missing = missing_months[0]
        last_missing = missing_months[-1]

        start_year, start_month = map(int, first_missing.split("-"))
        end_year, end_month = map(int, last_missing.split("-"))

        start_dt = date(start_year, start_month, 1)
        last_day = calendar.monthrange(end_year, end_month)[1]
        end_dt = date(end_year, end_month, last_day)

        # ⚠️ zone à risque: DegreeDays peut lever SourceDataCoverage
        data = get_monthly_hdd_cdd(
            station_id=station_id,
            start=start_dt,
            end=end_dt,
        )

        if not data:
            # pas une "erreur", juste aucune donnée
            return

        # Grouper par mois et écrire seulement les mois vraiment manquants
        by_month: Dict[str, List[Dict]] = defaultdict(list)
        for row in data:
            month_key = row.get("month")
            if not month_key:
                continue
            if month_key not in missing_months:
                continue
            by_month[month_key].append(row)

        received_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        for month_key, rows in by_month.items():
            try:
                year_str, month_str = month_key.split("-")
            except ValueError:
                continue

            entity_path = f"degreedays/{year_str}/{month_str}"
            file_name = f"dd_{station_id}_{year_str}_{month_str}.json"

            payload = {
                "station_id": station_id,
                "year": int(year_str),
                "month": int(month_str),
                "data": rows,
                "received_at": received_at,
            }

            write_json_to_bronze(
                entity=entity_path,
                file_name=file_name,
                data=payload,
            )

        # Rebuild silver
        run_degreedays_silver_job()

    except Exception as e:
        # ✅ GLOBAL CATCH obligatoire: on ne laisse JAMAIS une exception remonter
        # Ici, on fait exprès de ne pas afficher de stacktrace
        msg = str(e)

        # Cas DegreeDays coverage / manque données -> on ne spam pas
        # (ton message "⚠️ Mois manquants ..." a déjà été print plus haut)
        if "SourceDataCoverage" in msg or "not have enough recorded temperature readings" in msg:
            return

        # Autre erreur inattendue -> message court
        print(f"⚠️ ensure_degreedays_for_station a échoué pour {station_id} : {msg}")
        return


if __name__ == "__main__":
    run_degreedays_silver_job()
