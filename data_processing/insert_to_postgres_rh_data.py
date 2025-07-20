import logging
import polars as pl
import json
from sqlalchemy import create_engine

import config

logger = logging.getLogger(__name__)

def main():

    df = pl.read_csv(config.DATA_FILE)

    with open(config.DISTANCE_FILE, "r", encoding="utf-8") as file:
        distance_dict = json.load(file)

    df_distance = pl.DataFrame({
        "adresse_du_domicile": list(distance_dict.keys()),
        "work_distance": list(distance_dict.values())
    })

    df = df.join(df_distance, on="adresse_du_domicile", how="left")

    nb_missing =  df.get_column('work_distance').null_count()

    if nb_missing > 0:
        logger.warning("Missing '%i' values in dataframe for 'work distance'.", nb_missing)

    else:
        logger.info("No missing values for 'work_distance' column.")

    df.glimpse()

    uri = build_uri(
        user = config.POSTGRES_USER, 
        password = config.POSTGRES_PASSWORD, 
        db = config.POSTGRES_DB,
        port = config.POSTGRES_PORT
        )

    df.write_database(
        table_name = 'rh_table',
        connection = uri,
        if_table_exists="append"
    )

def build_uri(user, password, db, server = '127.0.0.1', port = 5433):
    return f"postgresql://{user}:{password}@{server}:{port}/{db}"

if __name__ == "__main__":
    main()