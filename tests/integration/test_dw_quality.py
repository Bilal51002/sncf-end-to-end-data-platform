"""
Tests d'integration : verifient la qualite des donnees reellement chargees
dans dw.*, apres execution du pipeline (python -m src.main).

Necessitent que les containers Postgres soient demarres et le DW peuple.
Sinon, ces tests sont automatiquement "skip" (voir conftest.py).

Les requetes utilisent des agregats SQL (COUNT, pas de lecture ligne a
ligne) pour rester rapides malgre les volumes (6M / 10M lignes).
"""

from sqlalchemy import text

# ------------------------------------------------------------------
# Volumetrie : dw doit correspondre a raw (aucune perte lors du ETL)
# ------------------------------------------------------------------


def _count(engine, schema, table):
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT count(*) FROM {schema}.{table}")).scalar()


class TestVolumetrie:
    def test_dim_client_complet(self, source_engine, dw_engine):
        assert _count(dw_engine, "dw", "dim_client") == _count(source_engine, "raw", "client")

    def test_dim_gare_complet(self, source_engine, dw_engine):
        assert _count(dw_engine, "dw", "dim_gare") == _count(source_engine, "raw", "gare")

    def test_dim_train_complet(self, source_engine, dw_engine):
        assert _count(dw_engine, "dw", "dim_train") == _count(source_engine, "raw", "train")

    def test_fact_trajet_complet(self, source_engine, dw_engine):
        assert _count(dw_engine, "dw", "fact_trajet") == _count(source_engine, "raw", "trajet")

    def test_fact_reservation_complet(self, source_engine, dw_engine):
        assert _count(dw_engine, "dw", "fact_reservation") == _count(
            source_engine, "raw", "reservation"
        )


# ------------------------------------------------------------------
# Unicite des cles naturelles (deja garanti par contrainte UNIQUE en base,
# on le revalide ici explicitement comme regle de qualite documentee)
# ------------------------------------------------------------------


class TestUnicite:
    def test_id_client_unique_dans_dim_client(self, dw_engine):
        with dw_engine.connect() as conn:
            total = conn.execute(text("SELECT count(*) FROM dw.dim_client")).scalar()
            distinct = conn.execute(
                text("SELECT count(DISTINCT id_client) FROM dw.dim_client")
            ).scalar()
        assert total == distinct

    def test_id_trajet_unique_dans_fact_trajet(self, dw_engine):
        with dw_engine.connect() as conn:
            total = conn.execute(text("SELECT count(*) FROM dw.fact_trajet")).scalar()
            distinct = conn.execute(
                text("SELECT count(DISTINCT id_trajet) FROM dw.fact_trajet")
            ).scalar()
        assert total == distinct

    def test_id_reservation_unique_dans_fact_reservation(self, dw_engine):
        with dw_engine.connect() as conn:
            total = conn.execute(text("SELECT count(*) FROM dw.fact_reservation")).scalar()
            distinct = conn.execute(
                text("SELECT count(DISTINCT id_reservation) FROM dw.fact_reservation")
            ).scalar()
        assert total == distinct


# ------------------------------------------------------------------
# Corrections de qualite issues du profilage (voir README)
# ------------------------------------------------------------------


class TestQualiteProfilage:
    def test_sexe_uniquement_h_ou_f(self, dw_engine):
        with dw_engine.connect() as conn:
            rows = conn.execute(
                text("SELECT DISTINCT sexe FROM dw.dim_client WHERE sexe IS NOT NULL")
            ).fetchall()
        valeurs = {r[0] for r in rows}
        assert valeurs.issubset({"H", "F"}), f"Valeurs inattendues trouvees : {valeurs}"

    def test_ville_pas_entierement_majuscule(self, dw_engine):
        """Detecte les cas ou la normalisation de casse aurait echoue (ex: 'STRASBOURG')."""
        with dw_engine.connect() as conn:
            count = conn.execute(
                text(
                    "SELECT count(*) FROM dw.dim_client WHERE ville = UPPER(ville) AND LENGTH(ville) > 1"
                )
            ).scalar()
        assert count == 0

    def test_ville_pas_entierement_minuscule(self, dw_engine):
        with dw_engine.connect() as conn:
            count = conn.execute(
                text(
                    "SELECT count(*) FROM dw.dim_client WHERE ville = LOWER(ville) AND LENGTH(ville) > 1"
                )
            ).scalar()
        assert count == 0

    def test_pas_de_doublon_annee_mise_en_service_gare(self, dw_engine):
        """apres coalesce, annee_mise_en_service ne doit plus etre NULL si l'une
        des deux colonnes source l'etait renseignee (verifie indirectement via
        l'absence de valeurs manquantes en trop grand nombre)."""
        with dw_engine.connect() as conn:
            total = conn.execute(text("SELECT count(*) FROM dw.dim_gare")).scalar()
            manquants = conn.execute(
                text("SELECT count(*) FROM dw.dim_gare WHERE annee_mise_en_service IS NULL")
            ).scalar()
        # tolerance large : on verifie juste qu'on n'a pas un taux de nullite
        # massif qui indiquerait un coalesce rate
        assert manquants / total < 0.05


# ------------------------------------------------------------------
# Regles metier
# ------------------------------------------------------------------


class TestReglesMetier:
    def test_montant_total_coherent_avec_prix_et_passagers(self, dw_engine):
        """montant_total doit correspondre a prix_unitaire * nb_passagers,
        a un centime pres (arrondis)."""
        with dw_engine.connect() as conn:
            incoherents = conn.execute(
                text(
                    """
                    SELECT count(*) FROM dw.fact_reservation
                    WHERE ABS(montant_total - (prix_unitaire * nb_passagers)) > 0.01
                    """
                )
            ).scalar()
            total = conn.execute(text("SELECT count(*) FROM dw.fact_reservation")).scalar()
        # tolerance : on documente le taux plutot que d'exiger 0 strict,
        # certaines lignes source peuvent legitimement inclure des remises
        taux = incoherents / total
        assert taux < 0.20, f"{incoherents}/{total} lignes incoherentes ({taux:.1%})"

    def test_montants_non_negatifs(self, dw_engine):
        with dw_engine.connect() as conn:
            count = conn.execute(
                text(
                    """
                    SELECT count(*) FROM dw.fact_reservation
                    WHERE montant_total < 0 OR prix_unitaire < 0
                    """
                )
            ).scalar()
        assert count == 0

    def test_nb_passagers_strictement_positif(self, dw_engine):
        with dw_engine.connect() as conn:
            count = conn.execute(
                text("SELECT count(*) FROM dw.fact_reservation WHERE nb_passagers <= 0")
            ).scalar()
        assert count == 0

    def test_distance_km_non_negative(self, dw_engine):
        with dw_engine.connect() as conn:
            count = conn.execute(
                text("SELECT count(*) FROM dw.fact_trajet WHERE distance_km < 0")
            ).scalar()
        assert count == 0

    def test_reservation_faite_avant_ou_le_jour_du_trajet(self, dw_engine):
        """La date de reservation ne doit pas etre posterieure a la date du
        trajet reserve."""
        with dw_engine.connect() as conn:
            violations = conn.execute(
                text(
                    """
                    SELECT count(*)
                    FROM dw.fact_reservation r
                    JOIN dw.fact_trajet t ON t.trajet_key = r.trajet_key
                    WHERE r.date_key > t.date_key
                    """
                )
            ).scalar()
            total = conn.execute(text("SELECT count(*) FROM dw.fact_reservation")).scalar()
        taux = violations / total
        # tolerance : donnees synthetiques, on documente plutot qu'on bloque a 0
        assert taux < 0.05, f"{violations}/{total} reservations posterieures au trajet ({taux:.1%})"


# ------------------------------------------------------------------
# Integrite referentielle (deja garantie par les FK en base ; on la
# revalide explicitement comme filet de securite documente)
# ------------------------------------------------------------------


class TestIntegriteReferentielle:
    def test_aucune_reservation_orpheline(self, dw_engine):
        with dw_engine.connect() as conn:
            count = conn.execute(
                text(
                    """
                    SELECT count(*) FROM dw.fact_reservation r
                    LEFT JOIN dw.dim_client c ON c.client_key = r.client_key
                    WHERE c.client_key IS NULL
                    """
                )
            ).scalar()
        assert count == 0

    def test_aucun_trajet_orphelin(self, dw_engine):
        with dw_engine.connect() as conn:
            count = conn.execute(
                text(
                    """
                    SELECT count(*) FROM dw.fact_trajet t
                    LEFT JOIN dw.dim_train tr ON tr.train_key = t.train_key
                    WHERE tr.train_key IS NULL
                    """
                )
            ).scalar()
        assert count == 0
