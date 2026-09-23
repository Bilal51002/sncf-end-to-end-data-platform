"""
Chargement dans le DW. Utilise COPY (via psycopg2) plutot que des INSERT
un par un : indispensable pour charger 6 a 10 millions de lignes en un temps
raisonnable.
"""
import csv
import io

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
    """
    if df.empty:
        return 0

    buffer = io.StringIO()
    df[columns].to_csv(
        buffer, index=False, header=False, sep="\t",
        na_rep="\\N", quoting=csv.QUOTE_MINIMAL,
    )
    buffer.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY dw.{table} ({', '.join(columns)}) "
            f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')",
            buffer,
        )
    conn.commit()
    return len(df)


def fetch_key_map(engine, table: str, natural_key: str, surrogate_key: str) -> pd.DataFrame:
    """Recupere le mapping cle naturelle -> cle de substitution depuis une dim deja chargee."""
    query = f"SELECT {natural_key}, {surrogate_key} FROM dw.{table}"
    return pd.read_sql(query, engine)
