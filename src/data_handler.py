# src/data_handler.py

# ----------------------------------------------------------------------
# Libraries
# ----------------------------------------------------------------------
import logging
import pandas as pd
from pathlib import Path
import csv
import os
from tqdm import tqdm
from tabulate import tabulate

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
def save_filepath(path):
    """Save the file path to a module level variable for reuse.

    This helper allows other functions to easily retrieve the most
    recent file path.  A log entry and print statement are emitted so
    the user can trace when the path is set.
    """
    global saved_filepath
    saved_filepath = str(path)
    logger.info("File path saved: %s", saved_filepath)
    print(f"[Data Handler] Saved file path -> {saved_filepath}")


# ----------------------------------------------------------------------
def create_dataset_environment(dataset_name: str) -> dict:
    """Create a structured directory environment for the dataset."""
    documents_dir = Path.home() / "Documents"
    base_path = documents_dir / "ProtexxaDatascope" / dataset_name

    subdirs = {
        "stages": base_path / "stages",
        "process": base_path / "process",
        "garbage": base_path / "garbage",
        "chunks": base_path / "chunks",
        "converted": base_path / "converted",
    }

    for path in subdirs.values():
        path.mkdir(parents=True, exist_ok=True)

    return {"project": base_path, **subdirs}


def convert_to_csv(df: pd.DataFrame, original_path: str) -> Path:
    """Convert a DataFrame to CSV next to the original file.

    Parameters
    ----------
    df : pd.DataFrame
        Data to write out.
    original_path : str
        Location of the source file. The CSV will share this directory.

    Returns
    -------
    Path
        Path to the newly written CSV file.
    """
    csv_path = Path(original_path).with_suffix(".csv")
    df.to_csv(csv_path, index=False)
    logger.info("Converted %s to CSV -> %s", original_path, csv_path)
    print(f"[Data Handler] Converted {original_path} -> {csv_path}")
    return csv_path


def convert_txt_to_csv(txt_path: str) -> Path:
    """Convert a whitespace delimited TXT file to CSV.

    Parameters
    ----------
    txt_path : str
        Path to the TXT file to convert.

    Returns
    -------
    Path
        Location of the written CSV file.
    """
    df = pd.read_csv(txt_path, sep=r"\s+", engine="python")
    logger.info("Read TXT file %s with shape %s", txt_path, df.shape)
    print(f"[Data Handler] Loaded TXT file -> {txt_path}")
    return convert_to_csv(df, txt_path)


def convert_file_to_csv(input_path: str, output_dir: str) -> Path:
    """Convert various input formats to CSV in a chosen directory.

    This is a convenience wrapper for the GUI file conversion tool. It reads
    the ``input_path`` using pandas according to the file extension and
    writes a ``.csv`` file inside ``output_dir``.  A log entry and a print
    statement are emitted so that human operators can trace the action.
    """
    suffix = Path(input_path).suffix.lower()

    if suffix == ".csv":
        df = pd.read_csv(input_path)
    elif suffix in {".xls", ".xlsx"}:
        df = pd.read_excel(input_path)
    elif suffix == ".json":
        df = pd.read_json(input_path)
    elif suffix in {".parquet", ".pq"}:
        df = pd.read_parquet(input_path)
    elif suffix == ".tsv":
        df = pd.read_csv(input_path, sep="\t")
    elif suffix == ".txt":
        df = pd.read_csv(input_path, sep=r"\s+", engine="python")
    else:
        raise ValueError(f"Unsupported file format: {suffix}")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / (Path(input_path).stem + ".csv")
    df.to_csv(output_path, index=False)

    logger.info("Converted %s to CSV -> %s", input_path, output_path)
    print(f"[Data Handler] Converted {input_path} -> {output_path}")
    return output_path


def load_data(file_path, progress_fn=None):
    """Load a dataset and emit incremental progress notifications.

    Parameters
    ----------
    file_path : str
        Location of the dataset to load.
    progress_fn : callable, optional
        Callback accepting ``(percent, message)`` for UI updates.

    Returns
    -------
    pd.DataFrame | None
        The loaded data, or ``None`` on failure.

    Notes
    -----
    ``pandas`` does not provide native progress callbacks.  For text based
    formats we therefore read the file in chunks and approximate progress
    based on the number of rows processed.  This approach keeps the UI
    responsive while large files are being loaded.
    """
    try:
        if progress_fn:
            progress_fn(0, "Starting load")

        suffix = Path(file_path).suffix.lower()

        if suffix in {".csv", ".tsv", ".txt"}:
            # ------------------------------------------------------------------
            # For line-based text files we stream the data in chunks so that
            # the progress bar can be updated incrementally.  We first count the
            # lines to determine the total number of rows.  Progress is then
            # calculated from the proportion of processed rows.
            # ------------------------------------------------------------------
            total_rows = sum(1 for _ in open(file_path, "r", encoding="utf-8"))
            logger.info("Total rows detected: %s", total_rows)
            print(f"[Data Handler] Total rows detected: {total_rows}")

            sep = "," if suffix == ".csv" else "\t" if suffix == ".tsv" else r"\s+"
            reader = pd.read_csv(
                file_path,
                sep=sep,
                chunksize=10000,
                engine="python" if suffix == ".txt" else "c",
            )

            chunks = []
            rows_read = 0
            for chunk in reader:
                chunks.append(chunk)
                rows_read += len(chunk)
                if progress_fn and total_rows > 0:
                    progress = min(rows_read / total_rows * 100, 99)
                    progress_fn(progress, "Loading data")
                    logger.debug("Loaded %s/%s rows", rows_read, total_rows)
                    print(f"[Data Handler] Loaded {rows_read}/{total_rows} rows")

            df = pd.concat(chunks, ignore_index=True)

        elif suffix in {".xls", ".xlsx"}:
            if progress_fn:
                progress_fn(10, "Reading Excel")
            df = pd.read_excel(file_path)
        elif suffix == ".json":
            if progress_fn:
                progress_fn(10, "Reading JSON")
            df = pd.read_json(file_path)
        elif suffix in {".parquet", ".pq"}:
            if progress_fn:
                progress_fn(10, "Reading Parquet")
            df = pd.read_parquet(file_path)
        else:
            raise ValueError("Unsupported file format")

        if suffix != ".csv":
            csv_path = convert_to_csv(df, file_path)
            save_filepath(csv_path)
        else:
            save_filepath(file_path)

        if progress_fn:
            progress_fn(100, "Load complete")

        return df
    except Exception as e:
        logger.error("Failed to load data: %s", e)
        print(f"[Data Handler] Error loading {file_path}: {e}")
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


def split_into_chunks(
    dataset_name, input_file, chunk_size_mb=256, logger_fn=None, progress_fn=None
):
    """Split a CSV into smaller chunks with optional progress updates.

    Parameters
    ----------
    dataset_name : str
        Name of the dataset. Used to create output folders.
    input_file : str
        Path to the CSV file to split.
    chunk_size_mb : int, optional
        Desired chunk size in megabytes. Defaults to ``256``.
    logger_fn : callable, optional
        Function used for log messages. ``print`` is used when omitted.
    progress_fn : callable, optional
        Callback invoked with ``(percent, message)`` as the file is processed.
    """

    try:
        paths = create_dataset_environment(dataset_name)
        output_dir = paths["chunks"]

        chunk_size_bytes = chunk_size_mb * 1024 * 1024
        base_filename = os.path.splitext(os.path.basename(input_file))[0]

        def log(msg):
            if logger_fn:
                logger_fn(msg)
            else:
                print(msg)

        log(f"Reading from: {input_file}")
        log(f"Writing chunks to: {output_dir}")
        log(f"Chunk size: {chunk_size_mb} MB")
        if progress_fn:
            progress_fn(0, "Starting chunking")

        total_bytes = os.path.getsize(input_file)

        with open(input_file, "r", encoding="utf-8") as infile:
            reader = csv.reader(infile)
            try:
                header = next(reader)
            except StopIteration:
                log("Error: File is empty or missing a header.")
                return {
                    "total_rows": 0,
                    "total_chunks": 0,
                    "output_dir": str(output_dir),
                }

            chunk_index = 0
            current_chunk = []
            current_chunk_size = 0
            row_count = 0
            bytes_read = 0

            for row in tqdm(reader, desc="Splitting CSV", unit="rows"):
                row_size = len(",".join(row).encode("utf-8"))

                if current_chunk_size + row_size > chunk_size_bytes:
                    output_file = os.path.join(
                        output_dir, f"{base_filename}_chunk_{chunk_index}.csv"
                    )
                    with open(
                        output_file, "w", encoding="utf-8", newline=""
                    ) as outfile:
                        writer = csv.writer(outfile)
                        writer.writerow(header)
                        writer.writerows(current_chunk)
                    log(f"Chunk {chunk_index} written: {len(current_chunk)} rows")

                    chunk_index += 1
                    current_chunk = []
                    current_chunk_size = 0

                current_chunk.append(row)
                current_chunk_size += row_size
                row_count += 1
                bytes_read += row_size
                if progress_fn and total_bytes > 0:
                    progress = bytes_read / total_bytes * 100
                    progress_fn(min(progress, 99), "Chunking")

            # Final chunk
            if current_chunk:
                output_file = os.path.join(
                    output_dir, f"{base_filename}_chunk_{chunk_index}.csv"
                )
                with open(output_file, "w", encoding="utf-8", newline="") as outfile:
                    writer = csv.writer(outfile)
                    writer.writerow(header)
                    writer.writerows(current_chunk)
                log(f"Final chunk {chunk_index} written: {len(current_chunk)} rows")
                bytes_read += sum(
                    len(",".join(r).encode("utf-8")) for r in current_chunk
                )

        log(f"All chunks written. Total rows: {row_count}")
        log(f"Output directory contents: {os.listdir(output_dir)}")

        if progress_fn:
            progress_fn(100, "Chunking complete")

        return {
            "total_rows": row_count,
            "total_chunks": chunk_index + 1,
            "output_dir": str(output_dir),
        }

    except FileNotFoundError as e:
        logging.error(f"File not found: {e}")
        if logger_fn:
            logger_fn(f"Error: {e}")
    except PermissionError as e:
        logging.error(f"Permission error: {e}")
        if logger_fn:
            logger_fn(f"Error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        if logger_fn:
            logger_fn(f"Unexpected error: {e}")

    return {"total_rows": 0, "total_chunks": 0, "output_dir": ""}


# SOURCE APP FUNCTIONALITY BUILDOUT (SEAN)
PLACEHOLDERS = {
    "N/A",
    "NA",
    "None",
    "none",
    "unknown",
    "Unknown",
    "-",
    "TBD",
    "tbd",
    "0000",
    "",
    "null",
    "NULL",
    "n/a",
}


def run_analysis(
    df: pd.DataFrame,
    analysis_type: str,
    column: str = None,
    num_rows: int = 10,
    sort_desc: bool = False,
) -> str:
    """
    Dispatch to one of:
      - Data Preview
      - Missing Values
      - Duplicate Detection
      - Placeholder Detection
      - Special Character Analysis
    Returns a formatted string.
    """
    # subset + sort
    working = df[[column]] if column and column in df.columns else df.copy()
    if sort_desc:
        working = working.iloc[::-1]

    if analysis_type == "Data Preview":
        preview = working.head(num_rows)
        dtypes = [(c, str(t)) for c, t in working.dtypes.items()]
        return (
            "[Data Types]\n"
            + tabulate(dtypes, headers=["Column", "Dtype"], tablefmt="fancy_grid")
            + "\n\n[Preview]\n"
            + tabulate(preview, headers="keys", tablefmt="fancy_grid")
        )

    if analysis_type == "Missing Values":
        miss = working.isnull().sum()
        total = len(working)
        rows = [
            (c, int(cnt), f"{cnt/total*100:.2f}%") for c, cnt in miss.items() if cnt > 0
        ]
        if not rows:
            return "No missing values detected."
        return (
            "=== Missing Values ===\n"
            + tabulate(rows, headers=["Column", "Count", "%"], tablefmt="fancy_grid")
            + f"\n\nTotal rows: {total}"
        )

    if analysis_type == "Duplicate Detection":
        dups = working[working.duplicated(keep=False)]
        if dups.empty:
            return f"No duplicates. Checked {len(working)} rows."
        unique = working[working.duplicated()]
        report = [
            ["Total Rows", len(working)],
            ["Duplicate entries", len(dups)],
            ["Unique duplicate rows", len(unique)],
        ]
        body = tabulate(dups.head(num_rows), headers="keys", tablefmt="fancy_grid")
        return (
            "🔍 Duplicate Report\n"
            + tabulate(report, headers=["Metric", "Value"], tablefmt="fancy_grid")
            + "\n\n"
            + body
        )

    if analysis_type == "Placeholder Detection":
        rec = []
        total = len(working)
        for c in working.columns:
            col_ser = working[c].astype(str).str.strip()
            cnt = col_ser.isin(PLACEHOLDERS).sum()
            if cnt > 0:
                rec.append([c, cnt, f"{cnt/total*100:.2f}%"])
        if not rec:
            return "No placeholders found."
        return tabulate(rec, headers=["Column", "Count", "%"], tablefmt="fancy_grid")

    if analysis_type == "Special Character Analysis":
        pat = r"[^\w\s]"
        rec = []
        for c in working.columns:
            ser = working[c].astype(str)
            mask = ser.str.contains(pat, regex=True)
            cnt = int(mask.sum())
            if cnt:
                chars = set("".join(ser[mask]))
                rec.append([c, cnt, "".join(sorted(chars))])
        if not rec:
            return "No special characters found."
        return tabulate(
            rec, headers=["Column", "Count", "Chars"], tablefmt="fancy_grid"
        )

    return f"[Notice] {analysis_type} not recognized."
