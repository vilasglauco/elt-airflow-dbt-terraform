#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Downloads a semiannual ANP dataset ZIP file based on the provided year and semester.
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
    """
    Validates and normalizes the semester input to '01' or '02'.

    Args:
        sem: Semester string or number.

    Returns:
        A string '01' or '02'.

    Raises:
        ValueError: If the semester is invalid.
    """
    # Pad with leading zero if necessary
    sem = str(sem).zfill(2)
    if sem not in {"01", "02"}:
        raise ValueError("SEM must be '01' (1st semester) or '02' (2nd semester)")

    return sem


def build_paths(
    dataset: str, outdir: Path, target_year: str, target_sem: str, ingestion_date: str
) -> Path:
    """
    Builds the full output file path based on dataset, partition, and ingestion date.

    Args:
        dataset: The dataset name.
        outdir: The base output directory.
        target_year: The target year as a string, e.g., '2025'.
        target_sem: The target semester as a string, ('01' or '02').
        ingestion_date: The ingestion date string in 'YYYY-MM-DD_HHMMSS' format.

    Returns:
        Path: The output file location.
    """
    partition_dir = outdir / dataset / f"partition={target_year}-{target_sem}"
    outfile = (
        partition_dir
        / f"govbr-anpglp-ca-{target_year}-{target_sem}-{ingestion_date}.zip"
    )

    return outfile


def staging_download(url: str, outfile: Path, staging_dir: Path, dataset: str) -> None:
    """
    Download to a deterministic `.part` temp file in `staging_dir`, then move it to
    the final destination.
    """

    dataset_tmp_dir = staging_dir / dataset
    dataset_tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = dataset_tmp_dir / (outfile.name + ".part")

    try:
        # Remove any leftover from previous failed attempt
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError as e:
                logging.warning("Could not remove temporary file %s: %s", tmp_path, e)

        # Download into deterministic .part temp file
        urllib.request.urlretrieve(url, str(tmp_path))

        # Only create the partition directory after successful download
        outfile.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(tmp_path), str(outfile))

    finally:
        # Ensure no leftover temp remains if something goes wrong
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError as e:
                logging.warning("Could not clean up temporary file %s: %s", tmp_path, e)


def run_extract(
    year: str,
    sem: str,
    ingestion_date: str,
    dataset: str,
    outdir: Path,
    outdir_tmp: Path,
    base_url: str,
) -> dict:
    """
    Downloads a semiannual ANP dataset ZIP file based on the provided year and semester.

    If the file is not available for the given semester, attempts a fallback.

    Args:
        year: The target year as a string, e.g., '2025'.
        sem: The target semester as a string, ('01' or '02').
        ingestion_date: The ingestion date string in 'YYYY-MM-DD_HHMMSS' format.
        dataset: The dataset name.
        outdir: The base output directory.
        outdir_tmp: The temporary staging directory for downloads.
        base_url: The base URL for downloading the dataset.

    Returns:
        dict: A dictionary containing:
            - "file_path" (str): Path to the downloaded file.
            - "year" (str): The year for which the file was downloaded.
            - "sem" (str): The semester used for the download.
            - "ingestion_date" (str): The ingestion date string.
            - "dataset" (str): The dataset name.
            - "outdir" (str): The base output directory.
            - "outdir_tmp" (str): The temporary staging directory.
            - "base_url" (str): The base URL used for the download.
            - "size_kb" (float): The size of the downloaded file in kilobytes.
    """
    sem = validate_sem(sem)
    target_year, target_sem = str(year), sem
    outfile = build_paths(dataset, outdir, target_year, target_sem, ingestion_date)

    if outfile.exists():
        logging.info("File already exists: %s; skipping download.", outfile)
        size = outfile.stat().st_size

        return {
            "file_path": outfile,
            "year": target_year,
            "sem": target_sem,
            "ingestion_date": ingestion_date,
            "dataset": dataset,
            "outdir": outdir,
            "outdir_tmp": outdir_tmp,
            "base_url": base_url,
            "size_kb": size / 1024,
        }

    logging.info("Downloading: %s/ca-%s-%s.zip", base_url, target_year, target_sem)

    try:
        staging_download(
            f"{base_url}/ca-{target_year}-{target_sem}.zip",
            outfile,
            outdir_tmp,
            dataset,
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
                outfile = build_paths(
                    dataset, outdir, target_year, target_sem, ingestion_date
                )
                logging.info(
                    "Downloading: %s/ca-%s-%s.zip",
                    base_url,
                    target_year,
                    target_sem,
                )
                try:
                    staging_download(
                        f"{base_url}/ca-{target_year}-{target_sem}.zip",
                        outfile,
                        outdir_tmp,
                        dataset,
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

    size = outfile.stat().st_size
    logging.info("%.1f KB saved", size / 1024)

    return {
        "file_path": outfile,
        "year": target_year,
        "sem": target_sem,
        "ingestion_date": ingestion_date,
        "dataset": dataset,
        "outdir": outdir,
        "outdir_tmp": outdir_tmp,
        "base_url": base_url,
        "size_kb": size / 1024,
    }
