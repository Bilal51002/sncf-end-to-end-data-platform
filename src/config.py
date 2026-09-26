"""
Configuration du pipeline ETL : lecture des variables d'environnement (.env)
et construction des URLs de connexion SQLAlchemy pour les deux bases.
"""

import os

from dotenv import load_dotenv

# Cherche le .env a la racine du projet (deux niveaux au-dessus de ce fichier :
# src/config.py -> racine du repo)
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(_ROOT, ".env"))

SOURCE_DB_HOST = os.getenv("SOURCE_DB_HOST", "localhost")
SOURCE_DB_PORT = os.getenv("SOURCE_DB_PORT", "5433")
SOURCE_DB_NAME = os.getenv("SOURCE_DB_NAME", "sncf_oltp")
SOURCE_DB_USER = os.getenv("SOURCE_DB_USER", "northwind_user")
SOURCE_DB_PASSWORD = os.getenv("SOURCE_DB_PASSWORD", "changeme")

DW_DB_HOST = os.getenv("DW_DB_HOST", "localhost")
DW_DB_PORT = os.getenv("DW_DB_PORT", "5434")
DW_DB_NAME = os.getenv("DW_DB_NAME", "sncf_dw")
DW_DB_USER = os.getenv("DW_DB_USER", "dw_user")
DW_DB_PASSWORD = os.getenv("DW_DB_PASSWORD", "changeme")

SOURCE_DB_URL = (
    f"postgresql+psycopg2://{SOURCE_DB_USER}:{SOURCE_DB_PASSWORD}"
    f"@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"
)
DW_DB_URL = (
    f"postgresql+psycopg2://{DW_DB_USER}:{DW_DB_PASSWORD}@{DW_DB_HOST}:{DW_DB_PORT}/{DW_DB_NAME}"
)

# Taille des lots pour le traitement des grandes tables (trajet, reservation)
CHUNK_SIZE = int(os.getenv("ETL_CHUNK_SIZE", "500000"))
