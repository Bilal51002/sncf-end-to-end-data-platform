"""
Chargement dans le DW. Utilise COPY (via psycopg2) plutot que des INSERT
un par un : indispensable pour charger 6 a 10 millions de lignes en un temps
raisonnable.
"""

import csv
import io
import tempfile

import pandas as pd


def truncate_dw(conn) -> None:
    """
    Vide les tables du DW avant rechargement complet, dans l'ordre qui
    respecte les contraintes de cles etrangeres (faits avant dimensions).
    RESTART IDENTITY remet a zero les compteurs des cles de substitution.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            TRUNCATE TABLE
                dw.fact_reservation,
                dw.fact_trajet,
                dw.dim_client,
                dw.dim_gare,
                dw.dim_train
            RESTART IDENTITY CASCADE;
            """
        )
    conn.commit()


def load_dataframe(conn, df: pd.DataFrame, table: str, columns: list[str]) -> int:
    """
    Charge un DataFrame dans dw.<table> via COPY FROM STDIN (rapide).
    Retourne le nombre de lignes chargees.

    Desactive les verifications de cles etrangeres pendant le chargement
    pour eviter les timeouts sur les grosses tables de faits.

    Utilise un fichier temporaire au lieu de StringIO pour eviter
    de garder tout le CSV en memoire (prevention OOM sur gros chunks).
    """
    if df.empty:
        return 0

    # Ecrire le CSV dans un fichier temporaire au lieu de la memoire
    with tempfile.NamedTemporaryFile(mode="w+", delete=True, suffix=".csv") as tmp:
        df[columns].to_csv(
            tmp,
            index=False,
            header=False,
            sep="\t",
            na_rep="\\N",
            quoting=csv.QUOTE_MINIMAL,
        )
        tmp.flush()
        tmp.seek(0)

        with conn.cursor() as cur:
            # Desactiver les FK checks pour ce COPY (session_replication_role = replica)
            # Cela evite les timeouts sur les FK (ex: dim_date) pendant le bulk load
            cur.execute("SET session_replication_role = 'replica';")
            try:
                cur.copy_expert(
                    f"COPY dw.{table} ({', '.join(columns)}) "
                    f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')",
                    tmp,
                )
            finally:
                # Reactiver les FK checks
                cur.execute("SET session_replication_role = 'origin';")
    conn.commit()
    return len(df)


def fetch_key_map(engine, table: str, natural_key: str, surrogate_key: str) -> pd.DataFrame:
    """Recupere le mapping cle naturelle -> cle de substitution depuis une dim deja chargee."""
    query = f"SELECT {natural_key}, {surrogate_key} FROM dw.{table}"
    return pd.read_sql(query, engine)


def kill_other_connections(conn, dbname: str, table: str) -> None:
    """
    Termine les autres connexions liees a cette table precise (pas toute
    la base) avant de commencer un chargement. Ne filtre PAS sur
    state='active' : Postgres conserve le texte de la derniere requete
    executee meme quand une connexion est 'idle' (entre deux chunks COPY,
    pendant que Python prepare le prochain lot cote client) - c'est
    justement dans cette fenetre qu'une tache tuee par Airflow (zombie,
    heartbeat) laisse une connexion fantome invisible si on filtre sur
    'active' seulement.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = %s
              AND pid <> pg_backend_pid()
              AND query ILIKE %s
            """,
            (dbname, f"%COPY {table}%"),
        )
    conn.commit()