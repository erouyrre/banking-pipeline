'''
Main script to run the pipeline.
'''


__author__ = "Ernest Rouyrre"


from ingestion.generate_transaction import main as main_ingestion
from transformation.silver import main as main_silver
from transformation.gold import main as main_gold