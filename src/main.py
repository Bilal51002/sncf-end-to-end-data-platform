"""
Pipeline ETL complet raw -> dw, en Python (pandas + SQLAlchemy + psycopg2).

Usage (depuis la racine du repo, avec les deux containers Postgres demarres
et leurs ports exposes sur l'hote, cf .env) :

    python -m src.main

Etapes :
1. Purge des tables dw.* (rechargement complet, idempotent)
2. Dimensions : client, gare, train (chargement complet, petites tables)
3. fact_trajet : par lots (chunks), avec resolution des cles de substitution
4. fact_reservation : par lots, avec resolution des cles (dont trajet_key,
   qui necessite que fact_trajet soit deja charge)
"""
import time

from . import config
from .db import get_source_engine, get_dw_engine, get_dw_connection
from .extract.extract import extract_full, extract_chunks
from .transform.transform import (
    transform_dim_client,
    transform_dim_gare,
    transform_dim_train,
    transform_fact_trajet,
    transform_fact_reservation,
)
from .load.load import truncate_dw, load_dataframe, fetch_key_map

DIM_CLIENT_COLS = [
    "id_client", "nom", "prenom", "date_naissance", "sexe", "type_client",
    "ville", "code_postal", "pays", "email", "telephone",
    "date_creation_compte", "statut_compte",
]
DIM_GARE_COLS = [
    "id_gare", "nom_gare", "ville", "region", "pays", "nb_quais",
    "type_gare", "taille_gare", "electrification",
    "annee_mise_en_service", "latitude", "longitude", "categorie_strategique",
]
DIM_TRAIN_COLS = [
    "id_train", "code_train", "type_train", "capacite_totale",
    "capacite_classe1", "capacite_classe2",
    "ville_depart_base", "ville_arrivee_base",
    "annee_mise_en_service", "statut_train", "duree_estimee_minutes", "energie",
]
FACT_TRAJET_COLS = [
    "id_trajet", "date_key", "train_key", "gare_depart_key", "gare_arrivee_key",
    "heure_depart", "heure_arrivee_prevue", "distance_km", "statut_circulation",
]
FACT_RESERVATION_COLS = [
    "id_reservation", "date_key", "client_key", "trajet_key",
    "tarif_type", "classe_reservee", "prix_unitaire", "nb_passagers",
    "montant_total", "canal_vente", "mode_paiement", "statut_reservation",
]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run() -> None:
    source_engine = get_source_engine()
    dw_engine = get_dw_engine()
    dw_conn = get_dw_connection()

    try:
        log("Purge des tables dw.* avant rechargement complet...")
        truncate_dw(dw_conn)

        # ---------------- Dimensions ----------------
        log("Extraction + transformation + chargement : dim_client")
        df_client = extract_full(source_engine, "client")
        df_client = transform_dim_client(df_client)
        n = load_dataframe(dw_conn, df_client, "dim_client", DIM_CLIENT_COLS)
        log(f"  -> {n} lignes chargees dans dim_client")

        log("Extraction + transformation + chargement : dim_gare")
        df_gare = extract_full(source_engine, "gare")
        df_gare = transform_dim_gare(df_gare)
        n = load_dataframe(dw_conn, df_gare, "dim_gare", DIM_GARE_COLS)
        log(f"  -> {n} lignes chargees dans dim_gare")

        log("Extraction + transformation + chargement : dim_train")
        df_train = extract_full(source_engine, "train")
        df_train = transform_dim_train(df_train)
        n = load_dataframe(dw_conn, df_train, "dim_train", DIM_TRAIN_COLS)
        log(f"  -> {n} lignes chargees dans dim_train")

        # Mappings id -> surrogate key, necessaires pour les faits
        dim_train_map = fetch_key_map(dw_engine, "dim_train", "id_train", "train_key")
        dim_gare_map = fetch_key_map(dw_engine, "dim_gare", "id_gare", "gare_key")
        dim_client_map = fetch_key_map(dw_engine, "dim_client", "id_client", "client_key")

        # ---------------- fact_trajet (par lots) ----------------
        log(f"Chargement de fact_trajet par lots de {config.CHUNK_SIZE}...")
        total = 0
        for i, chunk in enumerate(
            extract_chunks(source_engine, "trajet", config.CHUNK_SIZE), start=1
        ):
            transformed = transform_fact_trajet(chunk, dim_train_map, dim_gare_map)
            n = load_dataframe(dw_conn, transformed, "fact_trajet", FACT_TRAJET_COLS)
            total += n
            log(f"  lot {i} : {n} lignes chargees (cumul : {total})")
        log(f"fact_trajet termine : {total} lignes au total")

        # Mapping id_trajet -> trajet_key, necessaire pour fact_reservation
        fact_trajet_map = fetch_key_map(
            dw_engine, "fact_trajet", "id_trajet", "trajet_key"
        )

        # ---------------- fact_reservation (par lots) ----------------
        log(f"Chargement de fact_reservation par lots de {config.CHUNK_SIZE}...")
        total = 0
        for i, chunk in enumerate(
            extract_chunks(source_engine, "reservation", config.CHUNK_SIZE), start=1
        ):
            transformed = transform_fact_reservation(
                chunk, dim_client_map, fact_trajet_map
            )
            n = load_dataframe(
                dw_conn, transformed, "fact_reservation", FACT_RESERVATION_COLS
            )
            total += n
            log(f"  lot {i} : {n} lignes chargees (cumul : {total})")
        log(f"fact_reservation termine : {total} lignes au total")

        log("Pipeline ETL termine avec succes.")

    finally:
        dw_conn.close()


if __name__ == "__main__":
    run()
