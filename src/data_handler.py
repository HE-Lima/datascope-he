# src/data_handler.py

#----------------------------------------------------------------------
#Libraries
#----------------------------------------------------------------------
import logging
import pandas as pd
from pathlib import Path   
import csv
import os
#----------------------------------------------------------------------
def save_filepath(path):
    '''Save the file path to a global variable for later use.'''
    global saved_filepath
    saved_filepath = str(path)
#----------------------------------------------------------------------
def create_dataset_environment(dataset_name: str) -> dict:
    '''Create a structured directory environment for the dataset.'''
    documents_dir = Path.home() / "Documents"
    base_path = documents_dir / "ProtexxaDatascope" / dataset_name

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
    """
    Loads a dataset and returns the DataFrame or None if it fails.
    """
    try:
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file_path.endswith((".xls", ".xlsx")):
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Unsupported file format")

        return df
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        return None


def get_data_stats(df, file_path):
    """
    Returns a dict of human-readable info about the dataset.
    """
    try:
        row_count = len(df)
        file_size = os.path.getsize(file_path) / (1024 * 1024)

        return {
            "row_count": row_count,
            "file_size": file_size,
            "log1": f"[Data Handler] Loaded {row_count} rows.",
            "log2": f"[Data Handler] File size: {file_size:.2f} MB"
        }

    except Exception as e:
        logger.error(f"Error getting data stats: {e}")
        return {
            "row_count": 0,
            "file_size": 0,
            "log1": "[Data Handler] Failed to get row count.",
            "log2": "[Data Handler] Failed to calculate file size."
        }








