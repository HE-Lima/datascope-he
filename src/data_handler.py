# src/data_handler.py

#----------------------------------------------------------------------
# Libraries
#----------------------------------------------------------------------
import logging
import pandas as pd
from pathlib import Path

#----------------------------------------------------------------------

def save_filepath(path):
    """Save the file path to a global variable for later use."""
    global saved_filepath
    saved_filepath = str(path)

#----------------------------------------------------------------------

def create_dataset_environment(dataset_name: str) -> dict:
    """Create a structured directory environment for the dataset."""
    documents_dir = Path.home() / "Documents"
    base_path = documents_dir / "ProtexxaDatascope" / dataset_name

    subdirs = {
        "stages": base_path / "stages",
        "process": base_path / "process",
        "garbage": base_path / "garbage",
        "chunks": base_path / "chunks",
    }

    for path in subdirs.values():
        path.mkdir(parents=True, exist_ok=True)

    return {
        "project": base_path,
        **subdirs,
    }

logger = logging.getLogger(__name__)

def load_data(file_path):
    """Load CSV or Excel data depending on file extension."""
    logger.info(f"Loading data from: {file_path}")
    try:
        file_path = Path(file_path)
        if file_path.suffix.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)
        logger.info("Data loaded successfully.")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return None
