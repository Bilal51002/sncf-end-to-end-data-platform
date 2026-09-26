"""
Tests unitaires sur src/db.py.
create_engine et psycopg2.connect sont mockes : on verifie que les bons
parametres de connexion sont transmis, sans jamais ouvrir de connexion.
"""

import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src import config, db  # noqa: E402


class TestGetSourceEngine:
    @patch("src.db.create_engine")
    def test_utilise_source_db_url(self, mock_create_engine):
        db.get_source_engine()
        mock_create_engine.assert_called_once_with(config.SOURCE_DB_URL)


class TestGetDwEngine:
    @patch("src.db.create_engine")
    def test_utilise_dw_db_url(self, mock_create_engine):
        db.get_dw_engine()
        mock_create_engine.assert_called_once_with(config.DW_DB_URL)


class TestGetDwConnection:
    @patch("src.db.psycopg2.connect")
    def test_utilise_les_parametres_dw(self, mock_connect):
        db.get_dw_connection()
        mock_connect.assert_called_once_with(
            host=config.DW_DB_HOST,
            port=config.DW_DB_PORT,
            dbname=config.DW_DB_NAME,
            user=config.DW_DB_USER,
            password=config.DW_DB_PASSWORD,
            options="-c statement_timeout=900000 -c idle_in_transaction_session_timeout=300000",
        )
