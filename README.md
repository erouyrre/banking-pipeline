# Banking Pipeline

A data pipeline for processing banking transactions and generating fraud analytics. This project implements a **medallion architecture** (Bronze → Silver → Gold) to ensure data quality and produce meaningful business insights from transaction data.

## 🏦 Business Context

Banks process millions of transactions daily, generating vast amounts of data that can be leveraged for business value. Use cases include fraud detection, regular reporting, and customer segmentation. This pipeline simulates the real-world data pipeline of a bank, extracting insights from transaction data to inform business decisions.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Data Flow](#data-flow)
- [Quality Checks](#quality-checks)
- [Analytics](#analytics)
- [Testing](#testing)
- [License](#license)

## Overview

This pipeline processes banking transaction and client data through three stages:

1. **Ingestion**: Generate synthetic banking data (clients and transactions)
2. **Transformation**: Clean and validate data (Silver layer)
3. **Aggregation**: Compute analytical metrics (Gold layer)

It handles realistic data quality issues and produces fraud detection metrics for banking institutions.

## Technical Choices

- **Parquet** : chosen over CSV because of its
  - columnar storage format, which allows for efficient
    compression and querying of large datasets
  - ability to store and query large datasets in a distributed
    manner, making it suitable for big data analytics
  - support for data types such as nested records and arrays,
    which are commonly found in JSON and Avro data
  - performance benefits due to its ability to read and write data
    in a streaming fashion, reducing memory overhead
- **Medallion Architecture** : ensures immutability of raw data by separating data processing into distinct layers, each with its own set of rules and responsibilities, allowing for easier maintenance and debugging.
- **Fail-fast validation** : justified so that data quality issues are detected early and can be addressed before further processing.

## Architecture

The project follows the **Medallion Data Architecture** pattern:

### 🥉 Bronze Layer
- Raw, unprocessed data
- Includes clients and transactions tables
- Generated with synthetic data for testing
- Saved as Parquet files for efficiency

### 🥈 Silver Layer
- Cleaned and validated data
- Applies data quality checks:
  - Validates transaction amounts (must be positive)
  - Detects and handles duplicate transactions
  - Filters out future-dated transactions
  - Quarantines invalid records
- Single source of truth for operational data

### 🥇 Gold Layer
- Business-ready aggregated data
- Computed metrics:
  - **Average transaction per client**: Mean transaction amount by sender
  - **Fraud rate per client**: Proportion of fraudulent transactions by sender
  - **Total received per client**: Sum of received amounts by recipient
- **Two Gold scripts available**:
  - One using SQL queries (gold_sql.py) for aggregating data
  - Another using Pandas DataFrames (gold.py) for aggregating data

## Project Structure

```
banking-pipeline/
├── main.py                          # Main orchestrator script
├── requirements.txt                   # List of dependencies
├── README.md                         # This file
├── LICENSE                           # Project license
├── config/                           # Configuration files
├── data/                             # Data storage
│   ├── bronze/                       # Raw data
│   │   ├── clients.parquet
│   │   └── transactions.parquet
│   ├── silver/                       # Cleaned data
│   │   ├── transactions.parquet
│   │   └── quarantine/
│   ├── gold/                         # Aggregated analytics
│   │   ├── avg_transaction_per_client.parquet
│   │   ├── fraud_rate_per_client.parquet
│   │   └── total_received_per_client.parquet
│   └── quarantines/                  # Invalid records
├── src/                              # Source code
│   ├── ingestion/
│   │   └── generate_transaction.py   # Data generation
│   ├── transformation/
│   │   ├── silver.py                 # Silver layer logic
|   |   ├── gold.py                   # Gold layer logic
│   │   └── gold_sql.py               # SQL based gold layer 
│   └── utils/
│       └── helpers.py                # Utility functions
└── tests/
    └── test_silver.py                # Unit tests for silver transformation
```

## Orchestration

The pipeline is orchestrated using Prefect, a Python library for building robust, efficient, and adaptive data pipelines. Prefect provides a simple API for defining tasks, flows, and storage, allowing for easy integration with existing data processing tools. By using Prefect, the pipeline can be easily scaled, modified, and executed in a variety of environments.

## Scalability
This pipeline is designed for moderate data volumes.
For production-scale data (100M+ transactions), the following
improvements would be considered:
- **Apache Spark** : replace Pandas with PySpark for distributed processing
- **Chunked processing** : process data in batches with PyArrow
- **Parquet partitioning** : partition data by date for faster queries

## Installation

### Prerequisites
- Python 3.8+
- pip or conda

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd banking-pipeline
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Dependencies
- **pandas**: Data manipulation and analysis
- **faker**: Generate synthetic data
- **pytest**: Unit testing framework
- **pyarrow**: Parquet file support
- **prefect**: Workflow management library

## Usage

### Run the Complete Pipeline

Execute the main orchestrator to run all three stages:

```bash
python main.py
```

This will:
1. Generate synthetic client and transaction data (Bronze)
2. Apply quality checks and transformations (Silver)
3. Compute analytical metrics (Gold)

### Run Individual Stages

```python
from ingestion.generate_transaction import main as main_ingestion
from transformation.silver import main as main_silver
from transformation.gold import main as main_gold

# Ingestion only
main_ingestion()

# Silver transformation
main_silver()

# Gold aggregation
main_gold()
```

## Data Flow

### Clients Table (Bronze)
| Column | Type | Description |
|--------|------|-------------|
| client_id | UUID (PK) | Unique client identifier |
| firstname | String | Client first name |
| lastname | String | Client last name |
| address | String | Client address |
| account_balance | Float | Account balance |

### Transactions Table (Bronze → Silver)
| Column | Type | Description |
|--------|------|-------------|
| transaction_id | UUID (PK) | Unique transaction identifier |
| timestamp | DateTime | Transaction timestamp |
| emetteur_id | UUID (FK) | Sender client ID |
| destinataire_id | UUID (FK) | Recipient client ID |
| montant | Float | Transaction amount |
| card_type | Enum | CREDIT, DEBIT, or PREPAID |
| location | String | Transaction location |
| status | Enum | APPROVED, DECLINED, or PENDING |
| is_fraud | Boolean | Fraud indicator |

## Quality Checks

The Silver layer applies the following validation rules:

### ✅ Transaction Amount Validation
- **Rule**: `montant > 0`
- **Action**: Transfer invalid records to quarantine
- **Purpose**: Ensure positive transaction amounts

### ✅ Duplicate Detection
- **Rule**: Check for duplicate transaction IDs and content duplicates
- **Action**: Regenerate transaction IDs for duplicates; quarantine content duplicates
- **Purpose**: Ensure data integrity

### ✅ Future Timestamp Filtering
- **Rule**: Reject transactions with future timestamps
- **Action**: Move to quarantine
- **Purpose**: Ensure data consistency with current time

Invalid records are saved to `data/quarantines/` for investigation.

## Analytics

### Gold Layer Metrics

**1. Average Transaction per Client**
```
client_id | avg_montant
```
Average transaction amount sent by each client.

**2. Fraud Rate per Client**
```
client_id | fraud_rate
```
Proportion of fraudulent transactions (0-1 range) for each client.

**3. Total Received per Client**
```
client_id | total_received
```
Sum of all amounts received by each client.

## Testing

Run the test suite:

```bash
pytest tests/ -v
```

### Test Coverage
- Silver transformation validation logic
- Data quality checks for duplicates, negative amounts, and future timestamps

## License

See [LICENSE](LICENSE) file for details.