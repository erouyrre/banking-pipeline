'''
Transform silver table into a gold table with key metrics:
- average transaction per client
- fraud rate per client
- total received per client
SQL version
'''


__author__ = "Ernest Rouyrre"


import pandas as pd
from pathlib import Path
from src.utils.helpers import get_logger
import duckdb


logger = get_logger(__name__)


def compute_metrics(silver_path: str) -> dict[str, pd.DataFrame]:

    metrics = {}

    con = duckdb.connect()
    
    con.execute(f"CREATE VIEW transactions AS SELECT * FROM '{silver_path}'")
    
    logger.info("Computing average transaction per client...")
    metrics['avg_montant'] = con.execute(
        """
        SELECT emetteur_id, AVG(montant) AS avg_montant
        FROM transactions
        GROUP BY emetteur_id
        """
    ).df()
    
    logger.info("Computing fraud rate per client...")
    metrics['fraud_rate'] = con.execute(
        """
        SELECT emetteur_id, AVG(is_fraud) AS fraud_rate
        FROM transactions
        GROUP BY emetteur_id
        """
    ).df()
    
    logger.info("Computing total received per client...")
    metrics['total_received'] = con.execute(
        """
        SELECT destinataire_id, SUM(montant) AS total_received
        FROM transactions
        GROUP BY destinataire_id
        """
    ).df()
    return metrics


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
    CURRENT_DIR = Path(__file__).parent
    SILVER_FOLDER = CURRENT_DIR.parent.parent.joinpath("data", "silver")
    TRANSACTIONS_FILE = SILVER_FOLDER.joinpath("transactions.parquet")
    logger.info(f"Loading silver data from {TRANSACTIONS_FILE}...")
    dict_df = compute_metrics(TRANSACTIONS_FILE)
    save_to_gold(dict_df)


if __name__ == "__main__":
    main()