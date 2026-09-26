import pandas as pd


def extract_full(engine, table_name: str, schema: str = "raw") -> pd.DataFrame:
    query = f"select * from {schema}.{table_name}"
    return pd.read_sql(query, engine)


def extract_chunks(engine, table_name: str, chunksize: int, schema: str = "raw"):
    query = f"select * from {schema}.{table_name}"
    return pd.read_sql(query, engine, chunksize=chunksize)
