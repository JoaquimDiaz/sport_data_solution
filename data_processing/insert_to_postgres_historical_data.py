import logging
import polars as pl
import psycopg

import config

logger = logging.getLogger(__name__)

def main():

    df = pl.read_csv(config.HISTORIC_DATA_FILE)

    df.glimpse()

    uri = build_uri(
        user = config.POSTGRES_USER, 
        password = config.POSTGRES_PASSWORD, 
        db = config.POSTGRES_DB,
        port = config.POSTGRES_PORT
        )

    with psycopg.connect(uri) as pg_conn:
        create_table_if_not_exists(pg_conn)

    df.write_database(
        table_name = 'sport_table',
        connection = uri,
        if_table_exists="append"
    )

def build_uri(user, password, db, server = '127.0.0.1', port = 5433):
    return f"postgresql://{user}:{password}@{server}:{port}/{db}"

def create_table_if_not_exists(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {config.SPORT_TABLE} (
                id UUID PRIMARY KEY,
                id_salarie TEXT NOT NULL,
                date_debut TIMESTAMP NOT NULL,
                date_fin TIMESTAMP NOT NULL,
                sport_type TEXT NOT NULL,
                distance_metre INT,
                comment TEXT
            );
        """)
    pg_conn.commit()

if __name__ == "__main__":
    main()