import pandas as pd

# Colonne cle naturelle a utiliser pour un tri stable, par table.
# Sans ORDER BY explicite, PostgreSQL ne garantit aucun ordre de retour
# stable entre deux fetch d'un meme curseur, surtout avec des workers
# paralleles actifs (max_parallel_workers_per_gather > 0) : deux fetch
# successifs peuvent alors renvoyer une ligne en double ou en sauter une,
# ce qui provoquait des UniqueViolation aleatoires lors du chargement
# par chunks de fact_trajet et fact_reservation.
_ORDER_BY_COLUMN = {
    "trajet": "id_trajet",
    "reservation": "id_reservation",
    "client": "id_client",
    "gare": "id_gare",
    "train": "id_train",
}


def extract_full(engine, table_name: str, schema: str = "raw") -> pd.DataFrame:
    order_col = _ORDER_BY_COLUMN.get(table_name)
    order_clause = f" ORDER BY {order_col}" if order_col else ""
    query = f"select * from {schema}.{table_name}{order_clause}"
    return pd.read_sql(query, engine)


def extract_chunks(engine, table_name: str, chunksize: int, schema: str = "raw"):
    order_col = _ORDER_BY_COLUMN.get(table_name)
    if not order_col:
        raise ValueError(
            f"Pas de colonne de tri definie pour '{table_name}' : le chunking "
            "sans ORDER BY n'est pas fiable (risque de lignes dupliquees ou "
            "manquantes entre deux chunks). Ajoutez cette table a "
            "_ORDER_BY_COLUMN."
        )
    query = f"select * from {schema}.{table_name} ORDER BY {order_col}"
    return pd.read_sql(query, engine, chunksize=chunksize)
