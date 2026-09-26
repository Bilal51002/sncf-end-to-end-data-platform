"""
Pipeline ETL SNCF : raw -> dw, orchestre par Airflow.

Chaque etape de src/main.py devient une tache Airflow distincte, dans le
meme ordre (dimensions d'abord, puis les faits qui en dependent).

NOTE IMPORTANTE : on utilise le logger Python standard (logging), jamais
print(). Sur les taches longues (des dizaines de minutes), le tube stdout
utilise par Airflow 3 pour capturer print() peut se fermer prematurement
("BrokenPipeError: [Errno 32] Broken pipe"), faisant echouer la tache
alors que le travail reel (chargement des donnees) s'est bien termine.
Le logger passe par un circuit plus robuste (celui des lignes JSON
structlog que l'on voit dans les logs du scheduler) et n'a pas ce probleme.
"""

import logging
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from src import config
from src.db import get_dw_connection, get_dw_engine, get_source_engine
from src.extract.extract import extract_chunks, extract_full
from src.load.load import fetch_key_map, load_dataframe, truncate_dw
from src.transform.transform import (
    transform_dim_client,
    transform_dim_gare,
    transform_dim_train,
    transform_fact_reservation,
    transform_fact_trajet,
)

log = logging.getLogger("airflow.task")

DIM_CLIENT_COLS = [
    "id_client",
    "nom",
    "prenom",
    "date_naissance",
    "sexe",
    "type_client",
    "ville",
    "code_postal",
    "pays",
    "email",
    "telephone",
    "date_creation_compte",
    "statut_compte",
]
DIM_GARE_COLS = [
    "id_gare",
    "nom_gare",
    "ville",
    "region",
    "pays",
    "nb_quais",
    "type_gare",
    "taille_gare",
    "electrification",
    "annee_mise_en_service",
    "latitude",
    "longitude",
    "categorie_strategique",
]
DIM_TRAIN_COLS = [
    "id_train",
    "code_train",
    "type_train",
    "capacite_totale",
    "capacite_classe1",
    "capacite_classe2",
    "ville_depart_base",
    "ville_arrivee_base",
    "annee_mise_en_service",
    "statut_train",
    "duree_estimee_minutes",
    "energie",
]
FACT_TRAJET_COLS = [
    "id_trajet",
    "date_key",
    "train_key",
    "gare_depart_key",
    "gare_arrivee_key",
    "heure_depart",
    "heure_arrivee_prevue",
    "distance_km",
    "statut_circulation",
]
FACT_RESERVATION_COLS = [
    "id_reservation",
    "date_key",
    "client_key",
    "trajet_key",
    "tarif_type",
    "classe_reservee",
    "prix_unitaire",
    "nb_passagers",
    "montant_total",
    "canal_vente",
    "mode_paiement",
    "statut_reservation",
]


def _purge_dw():
    conn = get_dw_connection()
    try:
        truncate_dw(conn)
        log.info("dw purge : OK")
    finally:
        conn.close()


def _load_dim_client():
    source_engine = get_source_engine()
    dw_conn = get_dw_connection()
    try:
        df = extract_full(source_engine, "client")
        df = transform_dim_client(df)
        n = load_dataframe(dw_conn, df, "dim_client", DIM_CLIENT_COLS)
        log.info("dim_client : %s lignes chargees", n)
    finally:
        dw_conn.close()


def _load_dim_gare():
    source_engine = get_source_engine()
    dw_conn = get_dw_connection()
    try:
        df = extract_full(source_engine, "gare")
        df = transform_dim_gare(df)
        n = load_dataframe(dw_conn, df, "dim_gare", DIM_GARE_COLS)
        log.info("dim_gare : %s lignes chargees", n)
    finally:
        dw_conn.close()


def _load_dim_train():
    source_engine = get_source_engine()
    dw_conn = get_dw_connection()
    try:
        df = extract_full(source_engine, "train")
        df = transform_dim_train(df)
        n = load_dataframe(dw_conn, df, "dim_train", DIM_TRAIN_COLS)
        log.info("dim_train : %s lignes chargees", n)
    finally:
        dw_conn.close()


def _load_fact_trajet():
    source_engine = get_source_engine()
    dw_engine = get_dw_engine()
    dw_conn = get_dw_connection()
    try:
        dim_train_map = fetch_key_map(dw_engine, "dim_train", "id_train", "train_key")
        dim_gare_map = fetch_key_map(dw_engine, "dim_gare", "id_gare", "gare_key")

        total = 0
        for i, chunk in enumerate(
            extract_chunks(source_engine, "trajet", config.CHUNK_SIZE), start=1
        ):
            transformed = transform_fact_trajet(chunk, dim_train_map, dim_gare_map)
            n = load_dataframe(dw_conn, transformed, "fact_trajet", FACT_TRAJET_COLS)
            total += n
            log.info("fact_trajet : lot %s -> %s lignes (total %s)", i, n, total)
        log.info("fact_trajet : %s lignes chargees au total", total)
    finally:
        dw_conn.close()


def _load_fact_reservation():
    source_engine = get_source_engine()
    dw_engine = get_dw_engine()
    dw_conn = get_dw_connection()
    try:
        dim_client_map = fetch_key_map(dw_engine, "dim_client", "id_client", "client_key")
        fact_trajet_map = fetch_key_map(dw_engine, "fact_trajet", "id_trajet", "trajet_key")

        total = 0
        for i, chunk in enumerate(
            extract_chunks(source_engine, "reservation", config.CHUNK_SIZE), start=1
        ):
            transformed = transform_fact_reservation(chunk, dim_client_map, fact_trajet_map)
            n = load_dataframe(dw_conn, transformed, "fact_reservation", FACT_RESERVATION_COLS)
            total += n
            log.info("fact_reservation : lot %s -> %s lignes (total %s)", i, n, total)
        log.info("fact_reservation : %s lignes chargees au total", total)
    finally:
        dw_conn.close()


def _quality_check():
    """Controles rapides post-chargement (les tests complets vivent dans tests/)."""
    dw_engine = get_dw_engine()
    with dw_engine.connect() as conn:
        from sqlalchemy import text

        negatifs = conn.execute(
            text(
                "SELECT count(*) FROM dw.fact_reservation "
                "WHERE montant_total < 0 OR prix_unitaire < 0"
            )
        ).scalar()
        orphelins = conn.execute(
            text(
                "SELECT count(*) FROM dw.fact_reservation r "
                "LEFT JOIN dw.dim_client c ON c.client_key = r.client_key "
                "WHERE c.client_key IS NULL"
            )
        ).scalar()
        nb_trajets = conn.execute(text("SELECT count(*) FROM dw.fact_trajet")).scalar()
        nb_reservations = conn.execute(text("SELECT count(*) FROM dw.fact_reservation")).scalar()

    if negatifs > 0:
        raise ValueError(f"{negatifs} montant(s) negatif(s) trouve(s) dans fact_reservation")
    if orphelins > 0:
        raise ValueError(f"{orphelins} reservation(s) orpheline(s) (client introuvable)")
    # Garde-fou : un chargement "reussi" mais vide n'est pas un vrai succes.
    if nb_trajets == 0 or nb_reservations == 0:
        raise ValueError(
            f"Tables vides apres chargement (fact_trajet={nb_trajets}, "
            f"fact_reservation={nb_reservations}) : la source raw etait-elle vide ?"
        )
    log.info(
        "Controles qualite OK : fact_trajet=%s, fact_reservation=%s",
        nb_trajets,
        nb_reservations,
    )


with DAG(
    dag_id="sncf_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["sncf", "etl", "data-engineering"],
) as dag:
    purge_dw = PythonOperator(task_id="purge_dw", python_callable=_purge_dw)

    load_dim_client = PythonOperator(task_id="load_dim_client", python_callable=_load_dim_client)
    load_dim_gare = PythonOperator(task_id="load_dim_gare", python_callable=_load_dim_gare)
    load_dim_train = PythonOperator(task_id="load_dim_train", python_callable=_load_dim_train)

    load_fact_trajet = PythonOperator(task_id="load_fact_trajet", python_callable=_load_fact_trajet)
    load_fact_reservation = PythonOperator(
        task_id="load_fact_reservation", python_callable=_load_fact_reservation
    )

    quality_check = PythonOperator(task_id="quality_check", python_callable=_quality_check)

    purge_dw >> [load_dim_client, load_dim_gare, load_dim_train]
    [load_dim_train, load_dim_gare] >> load_fact_trajet
    [load_dim_client, load_fact_trajet] >> load_fact_reservation
    load_fact_reservation >> quality_check
