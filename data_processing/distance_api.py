import json
import logging
import polars as pl
import requests

import config

logger = logging.getLogger(__name__)

def main():
    df = pl.read_csv(config.DATA_FILE)

    process_work_distance(
        df = df,
        address_column = 'adresse_du_domicile',
        work_address = config.WORK_ADDRESS,
        key = config.API_KEY_GOOGLE,
        destination_path = config.DISTANCE_FILE
    )

def process_work_distance(
    df: pl.DataFrame, 
    address_column: str, 
    work_address: str, 
    key: str,
    destination_path: str = None
) -> dict[str, int] | None:
    """
    Computes driving distances from each unique address in the DataFrame to a given work address using the Google Distance Matrix API.

    Parameters:
        df (pl.DataFrame): The input DataFrame containing address data.
        address_column (str): The name of the column with origin addresses.
        work_address (str): The destination address.
        key (str): Google Maps API key.
        destination_path (str, optional): Path to save the cached results as a JSON file. If not provided, returns the dictionary.

    Returns:
        dict[str, int] | None: Dictionary mapping address to distance in meters, or None if data was saved to disk.
    """
    list_address = df.get_column(address_column).unique().to_list()

    dict_work_distance = {}
    null_count = 0
    dict_length = 0

    for address in list_address:
        distance = compute_distance(
            origin=address,
            destination=work_address,
            key=key
        )
        if distance is None:
            logger.warning("No distance computed for address: '%s'.", address)
            null_count += 1
        dict_work_distance[address] = distance
        dict_length += 1

    logger.info("Number of API call: '%i'", dict_length)
    logger.info("Number of None return: '%i'", null_count)

    if destination_path:
        with open(destination_path, "w") as file:
            json.dump(dict_work_distance, file)
        logger.info("Result saved to '%s'.", destination_path)
        return None
    else:
        return dict_work_distance
            
def compute_distance(
    origin: str, 
    destination: str, 
    key: str,
    timeout: float = 5
) -> int | None:
    """
    Computes the driving distance (in meters) between two addresses using the Google Distance Matrix API.

    Parameters:
        origin (str): The starting address or coordinates (e.g., "128 Rue du Port, 34000" or "48.8566,2.3522").
        destination (str): The destination address or coordinates.
        key (str): Your Google Maps API key.
        timeout (float): Optional request timeout in seconds (default is 5).

    Returns:
        int | None: Distance in meters if successful, None otherwise.

    Example:
        distance = compute_distance(
            origin="128 Rue du Port, 34000",
            destination="10 Downing Street, London",
            key=API_KEY_GOOGLE
        )
    """
    url = 'https://maps.googleapis.com/maps/api/distancematrix/json'

    params = {
        'origins': origin,
        'destinations': destination,
        'key': key
    }

    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logger.info(f"Request failed: {e}")
        return None

    if data.get('status') != 'OK':
        return None

    try:
        element = data['rows'][0]['elements'][0]
        if element['status'] != 'OK':
            return None
        return element['distance']['value']
    except (KeyError, IndexError, TypeError):
        return None

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s [%(module)s|L%(lineno)d] %(asctime)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
    main()