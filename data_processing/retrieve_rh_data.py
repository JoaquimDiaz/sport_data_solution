import logging
from pathlib import Path
import polars as pl
from typing import Literal
from urllib.parse import urlparse

import config

logger = logging.getLogger(__name__)

def main():

    df_rh = retrieve_dataframe(source=config.DATA_RH_URL)
    df_sport = retrieve_dataframe(source=config.DATA_SPORT_URL)

    df_rh = standardize_columns(df_rh)
    df_sport = standardize_columns(df_sport)

    df = df_rh.join(df_sport, on='id_salarie')
    
    write_polars_file(df, config.DATA_FILE)

def retrieve_dataframe(
    source: str,
    **kwargs
) -> pl.DataFrame:
    """ """

    df = read_polars_file(source, **kwargs)

    if is_url(source):
        filename = get_filename_from_url(source)

    else:
        if not isinstance(source, Path):
            source = Path(source)
        filename = source.name

    if df.is_empty():
        raise ValueError(f"The dataframe returned from '{filename}' is empty")
    logger.info("File '%s': number of rows = '%i', number of columns = '%i'.", filename, df.shape[0], df.shape[1])

    return df

def is_url(source: str) -> bool:
    parsed = urlparse(str(source))
    return parsed.scheme in ("http", "https")

def get_filename_from_url(url: str) -> str:
    parsed_url = urlparse(url)
    return Path(parsed_url.path).name

def standardize_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({col:standardize_col_name(col) for col in df.columns})

def standardize_col_name(col_name: str) -> str:
    return (
        col_name.strip().lower()
        .replace('  ', ' ')
        .replace(' ', '_')
        .replace('é', 'e')
        .replace('è', 'e')
        .replace('\'', '_')
        .replace('/', '_')
    )

def read_polars_file(source: Path | str, **kwargs):
    """
    Read a file with Polars based on its extension using pathlib.Path.

    Parameters:
    - filepath (Path): A pathlib.Path object representing the file path.
    - **kwargs: Additional arguments passed to the respective read method.

    Returns:
    - pl.DataFrame: A Polars DataFrame containing the file data.
    """
    if is_url(source):
        file_extension = Path(urlparse(source).path).suffix.lower().lstrip('.')

    else:
        if not isinstance(source, Path):
            source = Path(source)
            file_extension = source.suffix.lower().lstrip('.')

    read_methods = {
        "csv": pl.read_csv,
        "json": pl.read_json,
        "parquet": pl.read_parquet,
        "xlsx": pl.read_excel,
        "xls": pl.read_excel,
    }

    if file_extension not in read_methods:
        raise ValueError(f"Unsupported file extension: '.{file_extension}'")

    return read_methods[file_extension](source, **kwargs)

def write_polars_file(df: pl.DataFrame, filepath: Path, **kwargs):
    """
    Write a Polars DataFrame to a file based on its extension using pathlib.Path.

    Parameters:
    - df (pl.DataFrame): The Polars DataFrame to write.
    - filepath (Path): A pathlib.Path object representing the output file path.
    - **kwargs: Additional arguments passed to the respective write method.
    """
    if not isinstance(filepath, Path):
        filepath = Path(filepath)

    file_extension = filepath.suffix.lower().lstrip('.')

    write_methods = {
        "csv": df.write_csv,
        "json": df.write_json,
        "parquet": df.write_parquet,
        "xlsx": df.write_excel,
        "xls": df.write_excel,
    }

    if file_extension not in write_methods:
        raise ValueError(f"Unsupported file extension: .{file_extension}")

    return write_methods[file_extension](filepath, **kwargs)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s [%(module)s|L%(lineno)d] %(asctime)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
    main()