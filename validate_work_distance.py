from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import psycopg
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

import config

load_dotenv()

# ───────────────────────────── SQL ──────────────────────────────
QUERY_RH = f"""
    SELECT id_salarie,
           moyen_de_deplacement,
           work_distance
    FROM   {config.RH_TABLE};
"""

# ─────────────────────────── TASKS ──────────────────────────────
@task(retries=2, retry_delay_seconds=30, log_prints=True)
def load_data() -> pl.DataFrame:
    logger = get_run_logger()
    with psycopg.connect(config.PG_URI) as conn:
        df = pl.read_database(QUERY_RH, conn)
    logger.info(f"Loaded {df.shape[0]} RH rows")
    return df


@task(log_prints=True)
def validate_work_distance(df_rh: pl.DataFrame) -> pl.DataFrame:
    """
    Return a DataFrame of rows that violate the transport / distance rules.
    """
    logger = get_run_logger()

    df_bad = df_rh.filter(
        ((pl.col("work_distance") > 15_000) & (pl.col("moyen_de_deplacement") == "Marche/running"))
        | ((pl.col("work_distance") > 25_000) & (pl.col("moyen_de_deplacement") == "Vélo/Trottinette/Autres"))
    )

    n_bad = df_bad.shape[0]
    logger.info(f"Found {n_bad} rows with inconsistent transport")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    if n_bad == 0:
        create_markdown_artifact(
            key=f"rh-dist-check-{ts}",
            markdown="### ✅ RH distance check passed – no inconsistent rows found",
        )
    else:
        sample = df_bad.head(20).to_pandas()  # show only a few lines
        md_table = sample.to_markdown(index=False)
        create_markdown_artifact(
            key=f"rh-dist-check-{ts}",
            markdown=(
                f"### ❌ RH distance check failed  \n"
                f"**{n_bad}** inconsistent rows detected. Rows shown below:\n\n{md_table}"
            ),
        )

    return df_bad


# ─────────────────────────── FLOW ───────────────────────────────
@flow(name="Validate-RH-work_distance")
def validate_rh_flow():
    df_rh  = load_data()
    validate_work_distance(df_rh)

# ──────────────────────── CLI entry ──────────────────────────────
if __name__ == "__main__":
    validate_rh_flow()
