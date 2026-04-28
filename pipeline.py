from data_fetch import fetch_data
from transform import clean
from features import build_features
from datetime import datetime, timezone, timedelta
from db import get_engine, save_to_db, get_latest_timestamp
import pandas as pd

def run_pipeline():
    engine = get_engine()

    # fetch data with latest timestamp from db or yesterday

    latest_time = get_latest_timestamp(engine, "cleaned_electricity_consumption")
    if latest_time is None:
        start_time = datetime.now(timezone.utc) - timedelta(days=1)
    else:
        start_time = latest_time

    end_time = datetime.now(timezone.utc)

    data = fetch_data(start_time, end_time)


    # RAW DATA
    raw_df = pd.DataFrame(data)
    raw_df["ingested_at"] = datetime.now(timezone.utc)
    save_to_db(raw_df, "raw_electricity_consumption", engine)


    # CLEAN DATA
    df = pd.json_normalize(data)    
    df = clean(df)
    save_to_db(df, "cleaned_electricity_consumption", engine)


    # FEATURES
    df_features = build_features(df)
    save_to_db(df_features, "features_electricity_consumption", engine)

# TODO: find out daily running / automated running
if __name__ == "__main__":
    run_pipeline()