'''
Transform raw data into a silver table.
'''


__author__ = "Ernest Rouyrre"


import pandas as pd
from utils.helpers import get_logger
from pathlib import Path
from uuid import uuid4


logger = get_logger(__name__)


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
    mask = df.duplicated(subset=["transaction_id"], keep=False)
    df.loc[mask, "transaction_id"] = [str(uuid4()) for _ in range(len(df[mask]))]

    CONTENT_COLS = ["emetteur_id", "destinataire_id", "montant", "timestamp"]
    df_invalid = df[df.duplicated(subset=CONTENT_COLS, keep="first")]
    df_valid = df[~df.duplicated(subset=CONTENT_COLS, keep="first")]
    return df_valid, df_invalid


def check_future_timestamp(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Check if the timestamp is in the future.
    '''
    df_valid = df[df["timestamp"] <= pd.Timestamp.now()]
    df_invalid = df[df["timestamp"] > pd.Timestamp.now()]
    return df_valid, df_invalid


def check_unknown_clients(
        df: pd.DataFrame,
        df_clients: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Check if the emetteur_id and destinataire_id are in the clients table.
    '''
    mask_emetteur = df["emetteur_id"].isin(df_clients["client_id"])
    mask_destinataire = df["destinataire_id"].isin(df_clients["client_id"])
    df_valid = df[mask_emetteur & mask_destinataire]
    df_invalid = df[~(mask_emetteur & mask_destinataire)]
    return df_valid, df_invalid


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
    We treat in fail-fast mode.
    '''
    quarantines = {}
    df_valid, df_invalid = check_negative_montant(df)
    quarantines["negative_montant"] = df_invalid
    df_valid, df_invalid = check_duplicates(df_valid)
    quarantines["duplicated"] = df_invalid
    df_valid, df_invalid = check_future_timestamp(df_valid)
    quarantines["future_timestamp"] = df_invalid
    df_valid, df_invalid = check_unknown_clients(df_valid, df_clients)
    quarantines["unknown_clients"] = df_invalid
    return df_valid, quarantines


def save_to_silver(df: pd.DataFrame, quarantines: dict[str, pd.DataFrame]):
    '''
    Save the valid transactions to a silver table as a parquet file.
    '''
    CURRENT_DIR = Path(__file__).parent

    SILVER_FOLDER = CURRENT_DIR.parent.parent.joinpath("data", "silver")
    SILVER_FOLDER.mkdir(parents=True, exist_ok=True)
    SILVER_FILE = SILVER_FOLDER.joinpath("transactions.parquet")
    df.to_parquet(SILVER_FILE, index=False)

    QUARANTINES_FOLDER = CURRENT_DIR.parent.parent.joinpath("data", "quarantines")
    QUARANTINES_FOLDER.mkdir(parents=True, exist_ok=True)
    for key, value in quarantines.items():
        QUARANTINES_FILE = QUARANTINES_FOLDER.joinpath(f"{key}.parquet")
        value.to_parquet(QUARANTINES_FILE, index=False)


def main():
    df_clients, df_transactions = load_bronze()
    df_valid, quarantines = validate_transactions(df_transactions, df_clients)
    save_to_silver(df_valid, quarantines)


if __name__ == "__main__":
    main()