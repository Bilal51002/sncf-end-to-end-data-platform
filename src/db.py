"""
Fabrique des connexions aux deux bases :
- SQLAlchemy engine pour pandas (read_sql / lectures)
- Connexion psycopg2 brute pour les chargements rapides via COPY
"""
import psycopg2
from sqlalchemy import create_engine

from . import config


def get_source_engine():
    """Moteur SQLAlchemy vers la base source (raw)."""
    return create_engine(config.SOURCE_DB_URL)


def get_dw_engine():
    """Moteur SQLAlchemy vers le data warehouse (dw)."""
    return create_engine(config.DW_DB_URL)


def get_dw_connection():
    """Connexion psycopg2 brute vers le DW, utilisee pour COPY (chargement rapide)."""
    return psycopg2.connect(
        host=config.DW_DB_HOST,
        port=config.DW_DB_PORT,
        dbname=config.DW_DB_NAME,
        user=config.DW_DB_USER,
        password=config.DW_DB_PASSWORD,
    )
