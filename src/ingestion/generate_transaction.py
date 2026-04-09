'''
Generate a list of clients and transactions with random data.
The tables are as follows:
Table CLIENTS
─────────────────────────────
client_id     : str (UUID)  PK
firstname     : str
lastname      : str
address       : str
account_balance: float

Table TRANSACTIONS
──────────────────────────────────
transaction_id  : str (UUID)  PK
timestamp       : datetime
emetteur_id     : str (UUID)  FK → CLIENTS
destinataire_id : str (UUID)  FK → CLIENTS
montant         : float
card_type       : enum (CREDIT, DEBIT, PREPAID)
location        : str
status          : enum (APPROVED, DECLINED, PENDING)
is_fraud        : bool
'''


__author__ = "Ernest Rouyrre"


import uuid
import random
import pandas as pd
from faker import Faker
from datetime import datetime
from enum import Enum
from pathlib import Path
from utils.helpers import get_logger


logger = get_logger(__name__)


fake = Faker()

class CardType(Enum):
    CREDIT = "CREDIT"
    DEBIT = "DEBIT"
    PREPAID = "PREPAID"

class TransactionStatus(Enum):
    APPROVED = "APPROVED"
    DECLINED = "DECLINED"
    PENDING = "PENDING"

def generate_clients(n: int) -> list[dict]:
    '''
    Generate a list of clients with random data.
    '''
    list_clients = []
    logger.info(f"Generating {n} clients...")
    for _ in range(n):
        client = {
            "client_id": str(uuid.uuid4()),
            "firstname": fake.first_name(),
            "lastname": fake.last_name(),
            "address": fake.address(),
            "account_balance": random.uniform(0, 2000),
        }
        list_clients.append(client)
    logger.info(f"Generated {len(list_clients)} clients")
    return list_clients
    
def get_different_client(clients, emetteur_id):
    '''
    Return a different client from the list of clients.
    '''
    client_id = random.choice(clients)["client_id"]
    while client_id == emetteur_id:
        client_id = random.choice(clients)["client_id"]
    return client_id

def generate_transactions(n: int, clients: list[dict]) -> list[dict]:
    '''
    Generate a list of transactions with random data.
    '''
    list_transactions = []
    logger.info(f"Generating {n} transactions...")
    for _ in range(n):
        emeteur_id = random.choice(clients)["client_id"]
        destinataire_id = get_different_client(clients, emeteur_id)
        is_fraud = random.random() < 0.02
        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "timestamp": fake.date_time_between(start_date="-1y", end_date="now"),
            "emetteur_id": emeteur_id,
            "destinataire_id": destinataire_id,
            "card_type": random.choice(list(CardType)).value,
            "location": fake.address(),
            "status": random.choice(list(TransactionStatus)).value,
            "is_fraud": is_fraud,
            "montant": random.uniform(0, 10000) if is_fraud else random.uniform(0, 1000)
        }
        list_transactions.append(transaction)
    logger.info(f"Generated {len(list_transactions)} transactions")
    return list_transactions

def save_to_bronze(clients: list[dict], transactions: list[dict]):
    '''
    Save the clients and transactions to a bronze table as a parquet file.
    '''
    CURRENT_DIR = Path(__file__).parent
    SAVE_FOLDER = CURRENT_DIR.parent.parent.joinpath("data", "bronze")
    SAVE_FOLDER.mkdir(parents=True, exist_ok=True)
    CLIENTS_FILE = SAVE_FOLDER.joinpath("clients.parquet")
    TRANSACTIONS_FILE = SAVE_FOLDER.joinpath("transactions.parquet")
    df_clients = pd.DataFrame(clients)
    df_transactions = pd.DataFrame(transactions)

    logger.info(f"Saving clients to {CLIENTS_FILE}...")
    df_clients.to_parquet(CLIENTS_FILE, index=False)

    logger.info(f"Saving transactions to {TRANSACTIONS_FILE}...")
    df_transactions.to_parquet(TRANSACTIONS_FILE, index=False)


def main():
    clients = generate_clients(1000)
    transactions = generate_transactions(10000, clients)
    save_to_bronze(clients, transactions)


if __name__ == "__main__":
    main()