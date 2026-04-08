'''
Main script to run the pipeline.
'''


__author__ = "Ernest Rouyrre"


from src.ingestion.generate_transaction import main as main_ingestion
from src.transformation.silver import main as main_silver
from src.transformation.gold import main as main_gold


if __name__ == "__main__":
    main_ingestion()
    main_silver()
    main_gold()