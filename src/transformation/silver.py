'''
Transform raw data into a silver table.
'''


__author__ = "Ernest Rouyrre"


import pandas as pd
from pathlib import Path


def load_bronze() -> tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Load the bronze tables as a pandas dataframe.
    '''
    CURRENT_DIR = Path(__file__).parent
    BRONZE_FOLDER = CURRENT_DIR.parent.parent.joinpath("data", "bronze")
    CLIENTS_FILE = BRONZE_FOLDER.joinpath("clients.parquet")
    TRANSACTIONS_FILE = BRONZE_FOLDER.joinpath("transactions.parquet")
    df_clients = pd.read_parquet(CLIENTS_FILE)
    df_transactions = pd.read_parquet(TRANSACTIONS_FILE)
    return df_clients, df_transactions


def validate_trans