"""Downloads a semiannual ANP dataset ZIP file based on the provided year and semester.
If the file is not available for the given semester, attempts a fallback to the first semester
of the same year.
"""

import logging
import shutil
import urllib.error
import urllib.request
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def validate_sem(sem: str) -> str:
    """Validates and normalizes the semester input to '01' or '02'.

    Args:
        sem (str): Semester string or number (e.g., '01', '02').

    Returns:
        str: Normalized semester string, either '01' or '02'.

    Raises:
        ValueError: If the semester is not '01' or '02'.
    """
    # Ensure two-digit format
    sem = str(sem).zfill(2)
    if sem not in {"01", "02"}:
        raise ValueError("SEM must be '01' (1st semester) or '02' (2nd semester)")
    return sem


def build_paths(
    source_name: str,
    incoming_path: Path,
    target_year: str,
    target_sem: str,
    ingestion_date: str,
) -> Path:
    """Builds the full output file path based on the source name, incoming path, and ingestion date.

    Args:
        source_name (str): Identifier for the data source (e.g., 'anp_glp').
        incoming_path (Path): Base directory where download files are stored.
        target_year (str): The target year as a string (e.g., '2025').
        target_sem (str): The target semester string, either '01' or '02'.
        ingestion_date (str): The ingestion date string in 'YYYY-MM-DD_HHMMSS' format.

    Returns:
        Path: Full output file path as a `pathlib.Path` object
            (incoming_path/source_name/partition=YYYY-SS/<file>.zip).
    """
    # Construct partitioned directory and final file path
    partition_path = (
        incoming_path / source_name / f"partition={target_year}-{target_sem}"
    )
    final_file = (
        partition_path / f"govbr-anpglp-ca-{target_year}-{target_sem}-"
        f"{ingestion_date}.zip"
    )
    return final_file


def download_to_work_path(
    url: str, final_file: Path, work_path: Path, source_name: str
) -> None:
    """Download to a deterministic `.part` temp file under `work_path/source_name`, then move it to
    the final destination in `incoming_path`.

    Args:
        url (str): The URL to download from.
        final_file (Path): The final output file path as a `pathlib.Path` object.
        work_path (Path): Base directory used for temporary downloads as a `pathlib.Path` object.
        source_name (str): Source identifier used to organize temporary files under `work_path`.

    Raises:
        urllib.error.HTTPError: If the HTTP request returns an error status.
        urllib.error.URLError: If the URL is invalid or a network error occurs.
        OSError: If there are issues with file operations (mkdir, move, unlink).
        ValueError: If the URL is invalid.
    """
    # Construct deterministic temp file path
    tmp_file = work_path / source_name / (final_file.name + ".part")
    # Create work directory if it doesn't exist
    try:
        # Create parent directories for the temp file
        tmp_file.parent.mkdir(parents=True, exist_ok=True)
        logging.info("Work directory ensured: %s", tmp_file.parent)
    except OSError as e:
        logging.error("Failed to create work directory %s: %s", tmp_file, e)
        raise

    # Remove leftover .part file
    if tmp_file.exists():
        try:
            tmp_file.unlink()
        except OSError as e:
            logging.warning("Could not remove temporary file %s: %s", tmp_file, e)
            raise

    # Download into deterministic .part temp file
    try:
        urllib.request.urlretrieve(url, str(tmp_file))
    except urllib.error.HTTPError as e:
        logging.error("HTTP error %s while downloading %s", e.code, url)
        raise
    except (urllib.error.URLError, ValueError) as e:
        logging.error("URL problem while downloading %s: %s", url, e)
        raise
    except Exception as e:
        logging.error("Unexpected error during download %s: %s", url, e)
        raise

    # Move to final destination
    try:
        final_file.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logging.error("Failed to create final directory %s: %s", final_file.parent, e)
        raise

    # Atomic move from temp file to final destination
    try:
        shutil.move(str(tmp_file), str(final_file))
        logging.info("File moved to final destination: %s", final_file)
    except OSError as e:
        logging.error("Failed to move file %s to %s: %s", tmp_file, final_file, e)
        raise
    finally:
        # Cleanup temp file if it still exists
        if tmp_file.exists():
            try:
                tmp_file.unlink()
                logging.info("Temporary .part file cleaned up: %s", tmp_file)
            except OSError as e:
                logging.warning("Could not clean up temporary file %s: %s", tmp_file, e)


def extract_data_from_anp(
    year: str,
    sem: str,
    ingestion_date: str,
    source_name: str,
    incoming_path: Path,
    work_path: Path,
    dataset_base_url: str,
) -> dict:
    """Downloads a semiannual ANP dataset ZIP file based on the provided year and semester.
    If the file is not available for the given semester, attempts a fallback to the first
     semester ('01').

    Args:
        year (str): The target year as a string, e.g., '2025'.
        sem (str): The target semester as a string ('01' or '02').
        ingestion_date (str): The ingestion date string in 'YYYY-MM-DD_HHMMSS' format.
        source_name (str): Identifier for the source (e.g. 'anp_glp').
        incoming_path (Path): The base directory where final files are stored.
        work_path (Path): The temporary staging directory used for downloads.
        dataset_base_url (str): The base URL for downloading the dataset.

    Returns:
        dict: {
            "final_file" (Path): Path to the downloaded file,
            "year" (str): The year for which the file was downloaded,
            "sem" (str): The semester used for the download,
            "ingestion_date" (str): The ingestion date string,
            "source_name" (str): The source identifier used,
            "incoming_path" (Path): The base output directory,
            "work_path" (Path): The temporary staging directory,
            "base_url" (str) : The base URL used for the download,
            "size_kb" (float): The size of the downloaded file in kilobytes,
        }
    """
    sem = validate_sem(sem)
    target_year, target_sem = str(year), sem
    final_file = build_paths(
        source_name, incoming_path, target_year, target_sem, ingestion_date
    )

    if final_file.exists():
        logging.info("File already exists: %s; skipping download.", final_file)
        size = final_file.stat().st_size

        return {
            "final_file": final_file,
            "year": target_year,
            "sem": target_sem,
            "ingestion_date": ingestion_date,
            "source_name": source_name,
            "incoming_path": incoming_path,
            "work_path": work_path,
            "dataset_base_url": dataset_base_url,
            "size_kb": round(size / 1024, 1),
        }

    logging.info(
        "Downloading: %s/ca-%s-%s.zip",
        dataset_base_url,
        target_year,
        target_sem,
    )

    try:
        download_to_work_path(
            f"{dataset_base_url}/ca-{target_year}-{target_sem}.zip",
            final_file,
            work_path,
            source_name,
        )

    except urllib.error.HTTPError as e:
        if e.code == 404:
            if sem == "02":
                logging.warning(
                    "File not found for %s-%s, falling back to first semester (01)...",
                    year,
                    sem,
                )
                target_sem = "01"
                final_file = build_paths(
                    source_name, incoming_path, target_year, target_sem, ingestion_date
                )
                logging.info(
                    "Downloading: %s/ca-%s-%s.zip",
                    dataset_base_url,
                    target_year,
                    target_sem,
                )
                try:
                    download_to_work_path(
                        f"{dataset_base_url}/ca-{target_year}-{target_sem}.zip",
                        final_file,
                        work_path,
                        source_name,
                    )
                except urllib.error.HTTPError as e2:
                    if e2.code == 404:
                        logging.error(
                            "File not found for %s-%s even after fallback to '01'.",
                            year,
                            target_sem,
                        )
                    raise
            else:
                logging.error(
                    "File not found for %s-%s and no earlier semester to fallback to.",
                    year,
                    sem,
                )
                raise
        else:
            raise

    size = final_file.stat().st_size
    logging.info("Downloaded successfully: %.1f KB saved", round(size / 1024, 1))

    return {
        "final_file": final_file,
        "year": target_year,
        "sem": target_sem,
        "ingestion_date": ingestion_date,
        "source_name": source_name,
        "incoming_path": incoming_path,
        "work_path": work_path,
        "dataset_base_url": dataset_base_url,
        "size_kb": round(size / 1024, 1),
    }
