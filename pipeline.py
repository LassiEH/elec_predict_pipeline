from data_fetch import fetch_data
from transform import clean
from features import build_features
from db import get_engine, save_to_db
import pandas as pd

def run_pipeline():
    data = fetch_data()

    df = pd.json_normalize(data)

    df = clean(df)
    df = build_features(df)

    engine = get_engine()
    save_to_db(df, engine)

# TODO: find out daily running / automated running
if __name__ == "__main__":
    run_pipeline()