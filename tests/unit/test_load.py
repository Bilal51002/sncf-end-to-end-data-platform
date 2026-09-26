"""
Tests unitaires sur src/load/load.py.
La connexion psycopg2 et le moteur SQLAlchemy sont mockes : on verifie
les appels (TRUNCATE, COPY, requete de mapping) sans toucher a Postgres.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.load.load import fetch_key_map, load_dataframe, truncate_dw  # noqa: E402


class TestTruncateDw:
    def test_execute_le_truncate_dans_le_bon_ordre_et_commit(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value

        truncate_dw(conn)

        sql_execute = cursor.execute.call_args[0][0]
        # faits avant dimensions, pour respecter les contraintes de FK
        assert "fact_reservation" in sql_execute
        assert "fact_trajet" in sql_execute
        assert sql_execute.index("fact_reservation") < sql_execute.index("dim_client")
        assert "RESTART IDENTITY CASCADE" in sql_execute
        conn.commit.assert_called_once()


class TestLoadDataframe:
    def test_dataframe_vide_ne_fait_aucun_copy(self):
        conn = MagicMock()
        df = pd.DataFrame(columns=["id_train"])

        n = load_dataframe(conn, df, "dim_train", ["id_train"])

        assert n == 0
        conn.cursor.assert_not_called()

    def test_appelle_copy_expert_avec_les_bonnes_colonnes(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        df = pd.DataFrame({"id_train": [1, 2], "code_train": ["A1", "A2"]})

        n = load_dataframe(conn, df, "dim_train", ["id_train", "code_train"])

        assert n == 2
        cursor.copy_expert.assert_called_once()
        sql_arg = cursor.copy_expert.call_args[0][0]
        assert "COPY dw.dim_train (id_train, code_train)" in sql_arg
        conn.commit.assert_called_once()

    def test_ne_selectionne_que_les_colonnes_demandees(self):
        """Si le DataFrame contient des colonnes en plus (ex: cle de jointure
        temporaire), seules les colonnes passees en parametre doivent partir
        dans le CSV envoye a COPY."""
        conn = MagicMock()
        df = pd.DataFrame(
            {
                "id_train": [1],
                "code_train": ["A1"],
                "colonne_technique_inutile": ["x"],
            }
        )

        load_dataframe(conn, df, "dim_train", ["id_train", "code_train"])

        cursor = conn.cursor.return_value.__enter__.return_value
        buffer_envoye = cursor.copy_expert.call_args[0][1]
        contenu = buffer_envoye.getvalue()
        assert "x" not in contenu


class TestFetchKeyMap:
    @patch("src.load.load.pd.read_sql")
    def test_construit_la_requete_de_mapping(self, mock_read_sql):
        mock_read_sql.return_value = pd.DataFrame({"id_train": [1, 2], "train_key": [10, 20]})
        engine = MagicMock()

        result = fetch_key_map(engine, "dim_train", "id_train", "train_key")

        mock_read_sql.assert_called_once_with(
            "SELECT id_train, train_key FROM dw.dim_train", engine
        )
        assert list(result["train_key"]) == [10, 20]
