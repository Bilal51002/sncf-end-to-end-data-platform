"""
Fixtures partagees pour les tests.

Les tests d'integration (tests/integration/) necessitent que les containers
Postgres soient demarres et que le pipeline ait deja ete execute au moins
une fois (dw peuple). S'ils ne sont pas joignables, ces tests sont
automatiquement "skip" plutot que d'echouer bruyamment.
"""

import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

# Permet d'importer le package src/ depuis la racine du repo
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import config  # noqa: E402


@pytest.fixture(scope="session")
def dw_engine():
    engine = create_engine(config.DW_DB_URL)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        pytest.skip("Base dw injoignable : demarre les containers Docker pour lancer ces tests.")
    return engine


@pytest.fixture(scope="session")
def source_engine():
    engine = create_engine(config.SOURCE_DB_URL)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        pytest.skip(
            "Base source injoignable : demarre les containers Docker pour lancer ces tests."
        )
    return engine
