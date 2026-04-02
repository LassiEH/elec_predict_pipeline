import pandas as pd

def load(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(
        file_path,
        sep=";"
    )
    return df

def clean(df: pd.DataFrame) -> pd.DataFrame:
    # convert and simplify dataframe

    df = df.rename(columns={
        "startTime": "time", 
        "Sähkönkulutus Suomessa": "consumption_mwh"
        })
    
    df["time"] = pd.to_datetime(df["time"])

    df = df[["time", "consumption_mwh"]]

    df = df.dropna()

    return df

def save_data(df: pd.DataFrame, output_path: str):
    df.to_csv(output_path, index=False)

def run_pipeline(input_path: str, output_path: str):
    df = load(input_path)
    df = clean(df)
    save_data(df, output_path)

if __name__ == "__main__":
    run_pipeline("data/raw.csv", "data/clean.csv")