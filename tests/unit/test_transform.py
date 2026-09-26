"""
Tests unitaires sur src/transform/transform.py.
Ne necessitent aucune connexion a une base : DataFrames construits a la main.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.transform.transform import (  # noqa: E402
    _normalize_sexe,
    _normalize_ville,
    _to_date_key,
    transform_dim_gare,
    transform_fact_reservation,
    transform_fact_trajet,
)


class TestNormalizeSexe:
    def test_formes_longues_et_abregees(self):
        s = pd.Series(["Homme", "Femme", "H", "F", "homme", "FEMME"])
        result = _normalize_sexe(s)
        assert list(result) == ["H", "F", "H", "F", "H", "F"]

    def test_valeur_inconnue_repli_premiere_lettre(self):
        s = pd.Series(["Xyz"])
        result = _normalize_sexe(s)
        assert list(result) == ["X"]


class TestNormalizeVille:
    def test_casse_variee_uniformisee(self):
        s = pd.Series(["STRASBOURG", "nantes", "  Lyon  ", "paris"])
        result = _normalize_ville(s)
        assert list(result) == ["Strasbourg", "Nantes", "Lyon", "Paris"]


class TestToDateKey:
    def test_format_yyyymmdd(self):
        s = pd.Series(pd.to_datetime(["2021-03-15", "2024-12-01"]))
        result = _to_date_key(s)
        assert list(result) == [20210315, 20241201]


class TestTransformDimGare:
    def test_electrification_mappee_en_booleen(self):
        df = pd.DataFrame(
            {
                "id_gare": [1, 2],
                "nom_gare": ["Gare A", "Gare B"],
                "ville": ["lyon", "PARIS"],
                "region": ["Rhone", "IDF"],
                "pays": ["France", "France"],
                "nb_quais": [4, 10],
                "type_gare": ["Regionale", "Nationale"],
                "taille_gare": ["Moyenne", "Grande"],
                "electrification": ["Electrifiée", "Non électrifiée"],
                "annee_mise_en_service": [1990, None],
                "annee_mise_service": [None, 1985],
                "latitude": [45.75, 48.85],
                "longitude": [4.85, 2.35],
                "categorie_strategique": ["B", "A"],
            }
        )
        result = transform_dim_gare(df)
        assert list(result["electrification"]) == [True, False]

    def test_coalesce_annee_mise_en_service(self):
        df = pd.DataFrame(
            {
                "id_gare": [1, 2],
                "nom_gare": ["Gare A", "Gare B"],
                "ville": ["Lyon", "Paris"],
                "region": ["Rhone", "IDF"],
                "pays": ["France", "France"],
                "nb_quais": [4, 10],
                "type_gare": ["Regionale", "Nationale"],
                "taille_gare": ["Moyenne", "Grande"],
                "electrification": ["Electrifiée", "Electrifiée"],
                "annee_mise_en_service": [1990, None],
                "annee_mise_service": [None, 1985],
                "latitude": [45.75, 48.85],
                "longitude": [4.85, 2.35],
                "categorie_strategique": ["B", "A"],
            }
        )
        result = transform_dim_gare(df)
        # la ligne 2 n'a pas annee_mise_en_service -> doit reprendre annee_mise_service
        assert list(result["annee_mise_en_service"]) == [1990, 1985]

    def test_ville_normalisee(self):
        df = pd.DataFrame(
            {
                "id_gare": [1],
                "nom_gare": ["Gare A"],
                "ville": ["STRASBOURG"],
                "region": ["Alsace"],
                "pays": ["France"],
                "nb_quais": [4],
                "type_gare": ["Regionale"],
                "taille_gare": ["Moyenne"],
                "electrification": ["Electrifiée"],
                "annee_mise_en_service": [1990],
                "annee_mise_service": [1990],
                "latitude": [48.58],
                "longitude": [7.75],
                "categorie_strategique": ["B"],
            }
        )
        result = transform_dim_gare(df)
        assert result["ville"].iloc[0] == "Strasbourg"


class TestTransformFactTrajet:
    def test_resolution_des_cles_de_substitution(self):
        df_trajet = pd.DataFrame(
            {
                "id_trajet": [1],
                "id_train": [100],
                "id_gare_depart": [10],
                "id_gare_arrivee": [20],
                "date_depart": pd.to_datetime(["2023-06-01"]),
                "heure_depart": ["08:00:00"],
                "heure_arrivee_prevue": ["10:00:00"],
                "distance_km": [400],
                "statut_circulation": ["A l'heure"],
            }
        )
        dim_train_map = pd.DataFrame({"id_train": [100], "train_key": [1]})
        dim_gare_map = pd.DataFrame({"id_gare": [10, 20], "gare_key": [1, 2]})

        result = transform_fact_trajet(df_trajet, dim_train_map, dim_gare_map)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["train_key"] == 1
        assert row["gare_depart_key"] == 1
        assert row["gare_arrivee_key"] == 2
        assert row["date_key"] == 20230601

    def test_trajet_sans_dimension_correspondante_est_exclu(self):
        """Un id_train absent de la dimension ne doit pas apparaitre dans le resultat
        (jointure inner) plutot que de planter ou d'inserer une cle nulle."""
        df_trajet = pd.DataFrame(
            {
                "id_trajet": [1],
                "id_train": [999],  # n'existe pas dans dim_train_map
                "id_gare_depart": [10],
                "id_gare_arrivee": [20],
                "date_depart": pd.to_datetime(["2023-06-01"]),
                "heure_depart": ["08:00:00"],
                "heure_arrivee_prevue": ["10:00:00"],
                "distance_km": [400],
                "statut_circulation": ["A l'heure"],
            }
        )
        dim_train_map = pd.DataFrame({"id_train": [100], "train_key": [1]})
        dim_gare_map = pd.DataFrame({"id_gare": [10, 20], "gare_key": [1, 2]})

        result = transform_fact_trajet(df_trajet, dim_train_map, dim_gare_map)
        assert len(result) == 0


class TestTransformFactReservation:
    def test_resolution_des_cles_de_substitution(self):
        df_reservation = pd.DataFrame(
            {
                "id_reservation": [1],
                "id_client": [50],
                "id_trajet": [1],
                "date_reservation": pd.to_datetime(["2023-05-20"]),
                "tarif_type": ["Normal"],
                "classe_reservee": ["2"],
                "prix_unitaire": [45.50],
                "nb_passagers": [2],
                "montant_total": [91.00],
                "canal_vente": ["Web"],
                "mode_paiement": ["CB"],
                "statut_reservation": ["Confirmee"],
            }
        )
        dim_client_map = pd.DataFrame({"id_client": [50], "client_key": [5]})
        fact_trajet_map = pd.DataFrame({"id_trajet": [1], "trajet_key": [7]})

        result = transform_fact_reservation(df_reservation, dim_client_map, fact_trajet_map)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["client_key"] == 5
        assert row["trajet_key"] == 7
        assert row["date_key"] == 20230520
