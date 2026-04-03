import pandas as pd

def create_features_time(df: pd.DataFrame) -> pd.DataFrame:
    df["hour"] = df["time"].dt.hour
    df["week_day"] = df["time"].dt.day_of_week
    df["month"] = df["time"].dt.month

    return df

def create_features_lag(df: pd.DataFrame) -> pd.DataFrame:
    df["lag_1h"] = df["consumption_mwh"].shift(4)

    return df

def create_features_rolling_mean(df: pd.DataFrame) -> pd.DataFrame:
    df["rolling_mean_1h"] = df["consumption_mwh"].shift(1).rolling(window=4).mean()

    return df

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = create_features_time(df)
    df = create_features_lag(df)
    df = create_features_rolling_mean(df)

    return df