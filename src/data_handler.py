# src/data_handler.py

print("Will now set up Environment for processing")
#----------------------------------------------------------------------
#Libraries
#----------------------------------------------------------------------
import shutil
import pandas as pd
import os
import sys
import logging
import re
from datetime import datetime
import warnings
import csv
import unicodedata
import html
from collections import Counter
import base64
from io import BytesIO
import matplotlib.pyplot as plt
from jinja2 import Template
import numpy as np
import re
from tqdm import tqdm
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

#----------------------------------------------------------------------
def setup_directories():
    """
    Ensures all necessary directories for the Mexico Citizen dataset cleaner exist.
    """
    print("Will now set up directories")
    base_dirs = [
        "./datasets/records",
        "./datasets/processed",
        "./datasets/garbage",
        "./datasets/processed/validation",
        "./datasets/processed/converted",
        "./datasets/to-be-ingested",
        "./datasets/analysis",
        "./datasets/processed/stages",
        "./datasets/processed/preprocessed",
        "./datasets/logs",
        "./datasets/research",
        "./datasets/processed/chunks",
    ]

    for directory in base_dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created directory: {directory}")
        else:
            print(f"Directory already exists: {directory}")

if __name__ == "__main__":
    setup_directories()

# Global directories for the dataset cleaner
# raw_data = "./datasets/processed/preprocessed/hautelook_renamed.csv" 
print("CSV Editor must be run before raw_data is used within the code")

#---------------------------------------------------------

records_dir = "./datasets/records"
processed_dir = "./datasets/processed/preprocessed"
validation_dir = "./datasets/processed/validation"
ingestion_dir = "./datasets/to-be-ingested"
garbage_dir = "./datasets/garbage"
stages_dir = "./datasets/processed/stages"
analysis_dir = "./datasets/analysis"
logging_dir = "./datasets/logs"
chunked_dir = "./datasets/processed/chunks"
#---------------------------------------------------------

# Check if directories exist
required_dirs = [records_dir, processed_dir, validation_dir, ingestion_dir, garbage_dir, stages_dir, logging_dir, analysis_dir, chunked_dir]

# Empty the directories except records_dir
for directory in required_dirs:
    if directory != records_dir:
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                    print(f"Deleted file: {file_path}")
                    logging.info(f"Deleted file: {file_path}")
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    print(f"Deleted directory: {file_path}")
                    logging.info(f"Deleted directory: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")
                logging.error(f"Failed to delete {file_path}. Reason: {e}")

for directory in required_dirs:
    if not os.path.exists(directory):
        raise FileNotFoundError(f"Required directory does not exist: {directory}")
    else:
        print(f"Directory exists: {directory}")
        logging.info(f"Directory exists: {directory}")



#----------------------------------------------------------------------
# Configure logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(logging_dir + "/dataset_cleaner.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

#----------------------------------------------------------------------
print("")











import pandas as pd
import logging

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
