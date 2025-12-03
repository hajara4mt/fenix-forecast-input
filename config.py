import os
from dotenv import load_dotenv

load_dotenv()

AZURE_STORAGE_ACCOUNT_NAME = "stfenixforecast"
AZURE_STORAGE_FILESYSTEM = "fenixlake"
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

if not AZURE_STORAGE_ACCOUNT_KEY:
    raise RuntimeError("AZURE_STORAGE_ACCOUNT_KEY n'est pas définie dans les variables d'environnement.")
