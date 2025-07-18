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


def get_data_stats(df, file_path):
    """Return row count and file size information for the dataset."""
    try:
        row_count = len(df)
        file_size = Path(file_path).stat().st_size / (1024 * 1024)
        return {
            "row_count": row_count,
            "file_size": file_size,
            "log1": f"[Data Handler] Loaded {row_count} rows.",
            "log2": f"[Data Handler] File size: {file_size:.2f} MB",
        }
    except Exception as e:
        logger.error(f"Error getting data stats: {e}")
        return {
            "row_count": 0,
            "file_size": 0,
            "log1": "[Data Handler] Failed to get row count.",
            "log2": "[Data Handler] Failed to calculate file size.",
        }


def split_into_chunks(dataset_name: str, file_path: str, chunk_size_mb: int = 256, logger_fn=None):
    """Split a CSV file into smaller chunks by approximate size."""
    logger_fn = logger_fn or (lambda msg: None)
    try:
        df = pd.read_csv(file_path)
        total_rows = len(df)
        bytes_per_row = df.memory_usage(index=True, deep=True).sum() / total_rows
        rows_per_chunk = max(1, int((chunk_size_mb * 1024 * 1024) / bytes_per_row))

        chunk_dir = Path(file_path).parent / f"{dataset_name}_chunks"
        chunk_dir.mkdir(parents=True, exist_ok=True)

        total_chunks = 0
        for start in range(0, total_rows, rows_per_chunk):
            chunk = df.iloc[start:start + rows_per_chunk]
            chunk_file = chunk_dir / f"{dataset_name}_{total_chunks + 1}.csv"
            chunk.to_csv(chunk_file, index=False)
            logger_fn(f"Saved {chunk_file}")
            total_chunks += 1

        return {"total_rows": total_rows, "total_chunks": total_chunks}
    except Exception as e:
        logger.error(f"Error splitting file: {e}")
        return None


def run_analysis(df, analysis_type: str, column: str | None, rows: int, descending: bool):
    """Perform simple data analysis tasks."""
    if df is None:
        return "[Error] No data provided."

    try:
        if analysis_type == "Data Preview":
            subset = df if column is None else df[[column]]
            data = subset.tail(rows) if descending else subset.head(rows)
            return data.to_string()
        elif analysis_type == "Missing Values":
            if column:
                count = df[column].isna().sum()
                return f"Missing values in {column}: {count}"
            else:
                return df.isna().sum().to_string()
        elif analysis_type == "Duplicate Detection":
            dups = df[df.duplicated(subset=column)] if column else df[df.duplicated()]
            return dups.head(rows).to_string()
        elif analysis_type == "Placeholder Detection":
            placeholders = {"N/A", "NA", "-", "--", "null"}
            if column:
                mask = df[column].astype(str).isin(placeholders)
            else:
                mask = df.astype(str).isin(placeholders).any(axis=1)
            return df[mask].head(rows).to_string()
        elif analysis_type == "Special Character Analysis":
            import re
            pattern = re.compile(r"[^\w\s]")
            if column:
                mask = df[column].astype(str).str.contains(pattern)
            else:
                mask = df.apply(lambda s: s.astype(str).str.contains(pattern)).any(axis=1)
            return df[mask].head(rows).to_string()
    except Exception as e:
        logger.error(f"Analysis error: {e}")
        return "[Error] Analysis failed."

    return "[Error] Unknown analysis type."
