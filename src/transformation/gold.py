'''
Transform silver table into a gold table with key metrics:
- average transaction per client
- fraud rate per client
- total received per client
'''


__author__ = "Ernest Rouyrre"


import pandas as pd
from pathlib import Path
from src.utils.helpers import get_logger


logger = get_logger(__name__)


def load_silver() -> pd.DataFrame:
    '''
    Load the bronze tables as a pandas dataframe.
    '''
    CURRENT_DIR = Path(__file__).parent
    SILVER_FOLDER = CURRENT_DIR.parent.parent.joinpath("data", "silver")
    
    TRANSACTIONS_FILE = SILVER_FOLDER.joinpath("transactions.parquet")
    
    logger.info(f"Loading transactions from {TRANSACTIONS_FILE}...")
    df_transactions = pd.read_parquet(TRANSACTIONS_FILE)
    logger.info(f"Loaded {len(df_transactions)} transactions")
    return df_transactions


def compute_avg_transaction_per_client(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Compute the average transaction per client.
    '''
    logger.info("Computing average transaction per client...")
    return df.groupby("emetteur_id").agg(avg_montant=("montant", "mean")).reset_index()


def compute_fraud_rate_per_client(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Compute the fraud rate per client.
    '''
    logger.info("Computing fraud rate per client...")
    return df.groupby("emetteur_id").agg(fraud_rate=("is_fraud", "mean")).reset_index()


def compute_total_received_per_client(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Compute the total received per client.
    '''
    logger.info("Computing total received per client...")
    return df.groupby("destinataire_id").agg(total_received=("montant", "sum")).reset_index()


def save_to_gold(dict_df: dict[str, pd.DataFrame]):
    '''
    Save the valid transactions to a silver table as a parquet file.
    '''
    CURRENT_DIR = Path(__file__).parent

    GOLD_FOLDER = CURRENT_DIR.parent.parent.joinpath("data", "gold")
    GOLD_FOLDER.mkdir(parents=True, exist_ok=True)
    
    for key, df in dict_df.items():
        logger.info(f"Saving {key} to {GOLD_FOLDER}...")
        GOLD_FILE = GOLD_FOLDER.joinpath(f"{key}.parquet")
        df.to_parquet(GOLD_FILE, index=False)


def main():
    df_transactions = load_silver()
    dict_df = {
        "avg_transaction_per_client": compute_avg_transaction_per_client(df_transactions),
        "fraud_rate_per_client": compute_fraud_rate_per_client(df_transactions),
        "total_received_per_client": compute_total_received_per_client(df_transactions),
    }
    save_to_gold(dict_df)


if __name__ == "__main__":
    main()