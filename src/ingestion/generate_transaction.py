'''
Génère des transactions selon la structure suivante :
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


from datetime import datetime
from uuid import uuid4

import faker as fk