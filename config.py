import logging
import os
from dotenv import load_dotenv
from pathlib import Path

logger = logging.getLogger(__name__)

load_dotenv()

API_KEY_GOOGLE = os.getenv("API_KEY_GOOGLE")

DATA = Path('./data')

DATA_RH = DATA / 'data_rh.xlsx'
DATA_SPORT = DATA / 'data_sport.xlsx'

DATA_RH_URL = 'https://s3.eu-west-1.amazonaws.com/course.oc-static.com/projects/922_Data+Engineer/1039_P12/Donne%CC%81es+RH.xlsx'
DATA_SPORT_URL = 'https://s3.eu-west-1.amazonaws.com/course.oc-static.com/projects/922_Data+Engineer/1039_P12/Donne%CC%81es+Sportive.xlsx'

DATA_FILE = DATA / 'data.csv'
HISTORIC_DATA_FILE = DATA / 'historic_data.csv'
SALARIE_SPORT_FILE = DATA / 'salarie_sport.csv'
DISTANCE_FILE = DATA / 'cached_work_distance.json'
WORK_ADDRESS = '1362 Av. des Platanes, 34970 Lattes'

#postgres db
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

PG_URI = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@127.0.0.1:{POSTGRES_PORT}/{POSTGRES_DB}"
)

RH_TABLE = 'rh_table'
SPORT_TABLE = 'sport_table'

PBI_RH_TABLE = 'pbi_rh_table'
PBI_SPORT_TABLE = 'pbi_sport_table'

ACTIVITY_THRESHOLD = 15
TAUX_PRIME = 0.05
NB_CP_PRIME = 5

ERROR_RATE = 0.10

SPORT_DISTANCE_RANGES = {
    "Voile": (5000, 50000),
    "Triathlon": (20000, 80000),
    "Runing": (3000, 15000),
    "Judo": (0, 0),
    "Randonnée": (5000, 25000),
    "Tennis": (0, 0),
    "Tennis de table": (0, 0),
    "Football": (3000, 10000),
    "Badminton": (0, 0),
    "Natation": (500, 3000),
    "Boxe": (0, 0),
    "Rugby": (3000, 8000),
    "Escalade": (0, 0),
    "Basketball": (3000, 7000),
    "Équitation": (2000, 20000)
}

