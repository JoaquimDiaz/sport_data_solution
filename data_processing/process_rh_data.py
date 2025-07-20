import logging
import polars as pl

import config
from distance_api import compute_distance

logger = logging.getLogger(__name__)

def main():

    df_rh = pl.read_excel(config.DATA_RH)
    df_sport = pl.read_excel(config.DATA_SPORT)
    
    df_rh = df_rh.rename({col:standardize_col_name(col) for col in df_rh.columns})
    df_sport = df_sport.rename({col:standardize_col_name(col) for col in df_sport.columns})

    df = df_rh.join(df_sport, on='id_salarie')
    
    df = df.with_columns(
        pl.col('adresse_du_domicile')
        .map_elements(
            lambda address:
                compute_distance(
                    origin = address,
                    destination = config.WORK_ADDRESS,
                    key = config.API_KEY_GOOGLE 
            ),
        return_dtype = pl.Int64
        )
        .alias('work_distance')
    )
    
    df.glimpse()
    
def standardize_col_name(col_name: str) -> str:
    return col_name.strip().lower().replace(' ', '_').replace('é', 'e').replace('è', 'e').replace('\'', '_')

if __name__ == "__main__":
    main()