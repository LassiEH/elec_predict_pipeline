import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, root_mean_squared_error

def load_data(path: str) -> pd.DataFrame:
    return pd.read_csv(path, parse_dates=["time"])

def split_features_target(df: pd.DataFrame):
    # Remove timestamps and target answers
    X = df.drop(columns=["time", "consumption_mwh"])
    # Target values for prediction
    y = df["consumption_mwh"]

    return X, y

def time_split(df, split_ratio=0.8):
    # https://apxml.com/courses/time-series-analysis-forecasting/chapter-6-model-evaluation-selection/train-test-split-time-series
    # Integer index location based split from the data
    split_index = int(len(df) * split_ratio)
    train = df.iloc[:split_index]
    test =  df.iloc[split_index:]
    return train, test

def train_model(X_train, y_train):
    model = LinearRegression()
    model.fit(X_train, y_train)
    return model

def evaluate(model, X_test, y_test):
    preds = model.predict(X_test)

    mean_abs_err = mean_absolute_error(y_test, preds)
    mean_sqr_err = root_mean_squared_error(y_test, preds)

    print(mean_abs_err)
    print(mean_sqr_err)

def run_training(data_path: str):
    df = load_data(data_path)

    train_df, test_df = time_split(df)

    X_train, y_train = split_features_target(train_df)
    X_test, y_test = split_features_target(test_df)

    model = train_model(X_train, y_train)

    evaluate(model, X_test, y_test)

if __name__ == "__main__":
    run_training("data/clean.csv")