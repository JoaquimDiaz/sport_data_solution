import polars as pl
import uuid
import random
from datetime import datetime, timedelta
import config

def main():
    df = pl.read_csv(config.DATA_FILE)
    df = df.select(["id_salarie", "pratique_d_un_sport"]).filter(pl.col("pratique_d_un_sport").is_not_null())
    generated_df = generate_sport_history(
        df,
        max_activities_per_week=3,
        min_duration=45,
        max_duration=120
    )
    generated_df.glimpse()

    generated_df.write_csv(config.HISTORIC_DATA_FILE)

def generate_activity_dates(start_date: datetime, end_date: datetime, min_duration=30, max_duration=180):
    total_days = (end_date - start_date).days
    random_day = start_date + timedelta(days=random.randint(0, total_days))
    random_hour = random.randint(6, 20)
    random_minute = random.randint(0, 59)
    date_debut = datetime(random_day.year, random_day.month, random_day.day, random_hour, random_minute)
    duration_minutes = random.randint(min_duration, max_duration)
    date_fin = date_debut + timedelta(minutes=duration_minutes)
    if date_fin > end_date:
        date_fin = end_date
    return date_debut, date_fin


def generate_sport_distance(sport: str, sport_distance_ranges: dict):
    low, high = sport_distance_ranges.get(sport, (0, 0))
    return random.randint(low, high) if high > 0 else 0

def generate_sport_history(
    df: pl.DataFrame,
    start_date: datetime = None,
    end_date: datetime = None,
    max_activities_per_week: int = 5,
    min_duration: int = 30,
    max_duration: int = 180,
    sport_distance_ranges: dict = None
) -> pl.DataFrame:
    
    today = datetime.today()
    if start_date is None:
        start_year = today.year - 1 if today.month >= 6 else today.year - 2
        start_date = datetime(start_year, 6, 1)
    if end_date is None:
        end_date = datetime(start_date.year + 1, 6, 1)
    if sport_distance_ranges is None:
        sport_distance_ranges = config.SPORT_DISTANCE_RANGES

    records = []
    for row in df.iter_rows(named=True):
        id_salarie = row["id_salarie"]
        sport = row["pratique_d_un_sport"]
        if sport not in sport_distance_ranges:
            continue
        current_date = start_date
        while current_date < end_date:
            for _ in range(random.randint(0, max_activities_per_week)):
                date_debut, date_fin = generate_activity_dates(current_date, current_date + timedelta(days=6),
                                                               min_duration, max_duration)
                distance = generate_sport_distance(sport, sport_distance_ranges)
                records.append({
                    "id": str(uuid.uuid4()),
                    "id_salarie": id_salarie,
                    "date_debut": date_debut,
                    "date_fin": date_fin,
                    "sport_type": sport,
                    "distance_metre": distance,
                    "comment": ""
                })
            current_date += timedelta(weeks=1)
    return pl.DataFrame(records)

if __name__ == "__main__":
    main()
