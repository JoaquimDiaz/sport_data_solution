# transform_for_pbi_prefect.py
from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import psycopg
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

import config 

load_dotenv()

# ─────────────────────── Functions ──────────────────────────
QUERY_SPORT = f"""
    SELECT id_salarie,
           date_debut,
           date_fin,
           sport_type,
           distance_metre
    FROM {config.SPORT_TABLE};
"""

QUERY_RH = f"""
    SELECT id_salarie,
           nom,
           prenom,
           salaire_brut,
           moyen_de_deplacement,
           pratique_d_un_sport,
           work_distance
    FROM {config.RH_TABLE};
"""

# ─────────────────────── tasks ────────────────────────────
@task(retries=2, retry_delay_seconds=30, log_prints=True)
def load_data() -> tuple[pl.DataFrame, pl.DataFrame]:
    logger = get_run_logger()
    with psycopg.connect(config.PG_URI) as conn:
        df_sport = pl.read_database(QUERY_SPORT, conn)
        df_rh    = pl.read_database(QUERY_RH, conn)
    logger.info(f"Loaded sport={df_sport.shape[0]} rows | rh={df_rh.shape[0]} rows")
    return df_sport, df_rh


@task(log_prints=True)
def transform(df_sport: pl.DataFrame, df_rh: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    logger = get_run_logger()

    # 1. Prime transformation
    df_rh = (
        df_rh.with_columns(
            pl.col("moyen_de_deplacement")
            .is_in(["Marche/running", "Vélo/Trottinette/Autres"])
            .alias("prime_sportive")
        )
        .with_columns(
            pl.when(pl.col("prime_sportive"))
            .then(pl.col("salaire_brut") * config.TAUX_PRIME)
            .otherwise(0)
            .alias("montant_prime")
        )
        .drop("salaire_brut")
    )

    # 2. Sport activity count 2025
    df_2025 = df_sport.filter(pl.col("date_debut").dt.year() == 2025)
    df_count = (
        df_2025
        .group_by("id_salarie")
        .agg(pl.len().alias("nb_activites_2025"))
        .with_columns(pl.col("id_salarie").cast(pl.Utf8))
    )

    df_rh = (
        df_rh.with_columns(pl.col("id_salarie").cast(pl.Utf8))
        .join(df_count, on="id_salarie", how="left")
        .with_columns(
            pl.when(pl.col("nb_activites_2025") > config.ACTIVITY_THRESHOLD).then(True).otherwise(False).alias("prime_cp")
        )
        .with_columns(
            pl.when(pl.col("prime_cp")).then(config.NB_CP_PRIME).otherwise(0).alias("nb_prime_cp")
        )
    )

    logger.info("Transformation finished")
    return df_sport, df_rh


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def write_to_postgres(df_sport: pl.DataFrame, df_rh: pl.DataFrame):
    logger = get_run_logger()

    df_sport.write_database(config.PBI_SPORT_TABLE, config.PG_URI, if_table_exists="replace")
    df_rh.write_database   (config.PBI_RH_TABLE,   config.PG_URI, if_table_exists="replace")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") 
    create_markdown_artifact(
        key=f"pbi-transform-{ts}",
        markdown=(
            f"### Power BI transform summary\n"
            f"*{df_sport.shape[0]}* sport rows → **{config.PBI_SPORT_TABLE}**  \n"
            f"*{df_rh.shape[0]}* RH rows   → **{config.PBI_RH_TABLE}**"
        ),
    )
    logger.info("Data written to Postgres")


# ─────────────────────── flow ─────────────────────────────
@flow(name="Transform-for-PBI")
def prepare_pbi_flow():
    sport, rh = load_data()
    sport_clean, rh_ready = transform(sport, rh)
    write_to_postgres(sport_clean, rh_ready)


if __name__ == "__main__":
    prepare_pbi_flow()
