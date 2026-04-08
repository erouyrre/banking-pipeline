'''
Test the silver table.
'''


__author__ = "Ernest Rouyrre"


import pandas as pd
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Ajoute le dossier src au path pour pouvoir importer
sys.path.append(str(Path(__file__).parent.parent / "src"))

from transformation.silver import check_negative_montant, check_future_timestamp, check_duplicates, check_unknown_clients


def test_check_negative_montant():
    '''
    Test the check_negative_montant function.
    '''
    df = pd.DataFrame({
        "transaction_id": ["uuid-1", "uuid-2", "uuid-3"],
        "montant": [35, -22, 0]
    })

    df_valid, df_invalid = check_negative_montant(df)

    assert len(df_valid) == 1
    assert len(df_invalid) == 2


def test_check_future_timestamp():
    '''
    Test the check_future_timestamp function.
    '''
    df = pd.DataFrame({
        "transaction_id": ["uuid-1", "uuid-2", "uuid-3"],
        "timestamp": [datetime.now(), datetime.now() + timedelta(days=1), datetime.now() - timedelta(days=1)]
    })

    df_valid, df_invalid = check_future_timestamp(df)

    assert len(df_valid) == 2
    assert len(df_invalid) == 1


def test_check_duplicates():
    '''
    Test the check_duplicates function.
    '''
    data = [
        {"transaction_id": "uuid-1", "emetteur_id": "A", "destinataire_id": "B", "montant": 100, "timestamp": datetime.now()},
        {"transaction_id": "uuid-1", "emetteur_id": "C", "destinataire_id": "D", "montant": 200, "timestamp": datetime.now() + timedelta(days=1)},
        {"transaction_id": "uuid-2", "emetteur_id": "E", "destinataire_id": "F", "montant": 300, "timestamp": datetime.now() - timedelta(days=1)},
        {"transaction_id": "uuid-3", "emetteur_id": "E", "destinataire_id": "F", "montant": 300, "timestamp": datetime.now() - timedelta(days=1)}
    ]
    df = pd.DataFrame(data)
    df_valid, df_invalid = check_duplicates(df)
    assert len(df_valid) == 3
    assert len(df_invalid) == 1


def test_check_unknown_clients():
    '''
    Test the check_unknown_clients function.
    '''
    data = [
        {"transaction_id": "uuid-1", "emetteur_id": "A", "destinataire_id": "B", "montant": 100, "timestamp": datetime.now()},
        {"transaction_id": "uuid-2", "emetteur_id": "D", "destinataire_id": "B", "montant": 200, "timestamp": datetime.now() + timedelta(days=1)},
        {"transaction_id": "uuid-3", "emetteur_id": "C", "destinataire_id": "D", "montant": 300, "timestamp": datetime.now() - timedelta(days=1)}
    ]
    df_clients = pd.DataFrame({
    "client_id": ["A", "B", "C"]
    })
    df = pd.DataFrame(data)
    df_valid, df_invalid = check_unknown_clients(df, df_clients)
    assert len(df_valid) == 1
    assert len(df_invalid) == 2