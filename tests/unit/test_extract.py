"""
Tests unitaires sur src/extract/extract.py.
Le moteur SQLAlchemy est mocke : on verifie que la bonne requete est
construite et transmise a pandas, sans ouvrir de connexion reelle.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.extract.extract import extract_chunks, extract_full  # noqa: E402


class TestExtractFull:
    @patch("src.extract.extract.pd.read_sql")
    def test_construit_la_requete_avec_le_bon_schema_et_table(self, mock_read_sql):
        mock_read_sql.return_value = pd.DataFrame({"id_train": [1, 2]})
        engine = MagicMock()

        result = extract_full(engine, "train")

        mock_read_sql.assert_called_once_with("select * from raw.train", engine)
        assert len(result) == 2

    @patch("src.extract.extract.pd.read_sql")
    def test_schema_personnalise(self, mock_read_sql):
        mock_read_sql.return_value = pd.DataFrame()
        engine = MagicMock()

        extract_full(engine, "client", schema="raw_remote")

        mock_read_sql.assert_called_once_with("select * from raw_remote.client", engine)


class TestExtractChunks:
    @patch("src.extract.extract.pd.read_sql")
    def test_passe_chunksize_a_pandas(self, mock_read_sql):
        mock_read_sql.return_value = iter([pd.DataFrame({"id_trajet": [1]})])
        engine = MagicMock()

        extract_chunks(engine, "trajet", 500_000)

        mock_read_sql.assert_called_once_with("select * from raw.trajet", engine, chunksize=500_000)

    @patch("src.extract.extract.pd.read_sql")
    def test_retourne_un_iterable_de_chunks(self, mock_read_sql):
        chunk1 = pd.DataFrame({"id_trajet": [1, 2]})
        chunk2 = pd.DataFrame({"id_trajet": [3]})
        mock_read_sql.return_value = iter([chunk1, chunk2])
        engine = MagicMock()

        chunks = list(extract_chunks(engine, "trajet", 2))

        assert len(chunks) == 2
        assert len(chunks[0]) == 2
        assert len(chunks[1]) == 1
