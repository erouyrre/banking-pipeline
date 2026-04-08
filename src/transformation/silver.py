'''
Transform raw data into a silver table.
'''


__author__ = "Ernest Rouyrre"


import pandas as pd
from pathlib import Path
from uuid import uuid4


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


def check_negative_montant(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Check if the montant is negative 0 and return the valid and invalid transactions.
    '''
    df_valid = df[df["montant"] > 0]
    df_invalid = df[df["montant"] <= 0]
    return df_valid, df_invalid


def check_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Check if the transactions have the same UUID and generate a new one.
    Check if the transactions are duplicated and return the valid and invalid transactions.
    '''
    

def validate_transactions(
        df: pd.DataFrame,
        df_clients: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    '''
    Validate the transactions table by verifying:
    - the emetteur_id and destinataire_id are in the clients table
    - the montant is greater than 0
    - transactions is not duplicated
    - timestamp is not in the future
    '''