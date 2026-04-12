from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

def get_engine():
    
    return create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

def save_to_db(df: pd.DataFrame, engine):
    
    df.to_sql(DB_NAME, engine, if_exists='append', index=False)