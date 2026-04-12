import pandas as pd

def clean(df: pd.DataFrame) -> pd.DataFrame:
    # convert and simplify dataframe

    df = df.rename(columns={
        "startTime": "time", 
        "value": "consumption_mwh"
        })
    
    df["time"] = pd.to_datetime(df["time"])

    df = df[["time", "consumption_mwh"]]

    df = df.dropna()

    return df