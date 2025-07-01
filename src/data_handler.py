# src/data_handler.py

#----------------------------------------------------------------------
#Libraries
#----------------------------------------------------------------------
import logging
import pandas as pd
from pathlib import Path   
import csv

#----------------------------------------------------------------------
def create_dataset_environment(dataset_name: str) -> dict:
    base_path = Path("./datasets/processed") / dataset_name
    subdirs = {
        "stages": base_path / "stages",
        "process": base_path / "process",
        "garbage": base_path / "garbage",
        "chunks": base_path / "chunks"
    }

    for path in subdirs.values():
        path.mkdir(parents=True, exist_ok=True)

    return {
        "project": base_path,
        **subdirs
    }



logger = logging.getLogger(__name__)

def load_data(file_path):
    logger.info(f"Loading data from: {file_path}")
    try:
        df = pd.read_csv(file_path)
        logger.info("Data loaded successfully.")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return None








