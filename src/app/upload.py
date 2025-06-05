import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

def upload_gold_to_postgres(
    gold_filename="perdcomp_gold",
    table_name="gold_jvs",
    mode="append"  # "replace" para full load, "append" para append
):
    """
    Reads the gold layer data (Delta/Parquet) and inserts it into the Postgres table.
    Connection variables are read from .env:
      - PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS, SCHEMA

    mode: "replace" (full load) ou "append" (apenas adiciona)
    """
    # Load variables from .env
    load_dotenv()

    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASS")
    schema = os.getenv("SCHEMA", "public")

    print(f"Starting upload to Postgres at {host}:{port}, database '{db}'")
    print(f"Target schema: {schema}")
    print(f"Target table: {table_name}")
    print(f"Upload mode: {mode}")
    print(f"Reading gold file from: {os.path.join('storage', 'gold', gold_filename)}")

    # Gold file path
    gold_path = os.path.join("storage", "gold", gold_filename)

    # Reads the Delta/Parquet as a pandas DataFrame
    try:
        df = pd.read_parquet(gold_path)
        print(f"File read successfully. Rows: {len(df)}, Columns: {len(df.columns)}")
    except Exception as e:
        print(f"Error reading the gold layer: {e}")
        return

    # Creates the connection string
    conn_str = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    )

    try:
        engine = create_engine(conn_str)
        print("Database connection created. Starting data upload...")
        df.to_sql(table_name, engine, if_exists=mode, index=False, schema=schema)
        print(f"Data inserted into table {schema}.{table_name} successfully!")
    except Exception as e:
        print(f"Error inserting into Postgres: {e}")

# Example usage:
# Full load (replace)
upload_gold_to_postgres(mode="replace", table_name="bahia_perdcomp")
# Append
# upload_gold_to_postgres(mode="append")