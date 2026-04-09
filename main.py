'''
Main script to run the pipeline.
'''


__author__ = "Ernest Rouyrre"


from src.ingestion.generate_transaction import main as main_ingestion
from src.transformation.silver import main as main_silver
from src.transformation.gold import main as main_gold
from prefect import flow, task

@task
def ingestion_task():
    main_ingestion()

@task
def silver_task():
    main_silver()

@task
def gold_task():
    main_gold()

@flow(name="banking-pipeline")
def main_flow():
    ingestion_task()
    silver_task()
    gold_task()

if __name__ == "__main__":
    main_flow()