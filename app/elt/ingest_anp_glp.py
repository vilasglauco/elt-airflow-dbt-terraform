#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Downloads a semiannual ANP dataset ZIP file based on the provided year and semester.
If the file is not available for the given semester, attempts a fallback to the first semester
of the same year.
"""

import datetime as dt
import logging
import os
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
        A Path object pointing to the output file location.
    """
    partition_key = f"{target_year}-{target_sem}"
    partition_dir = outdir / dataset / f"partition={partition_key}"
    filename = f"govbr-anpglp-ca-{target_year}-{target_sem}-{ingestion_date}.zip"
    outfile = partition_dir / filename
    return outfile


def staging_download(url: str, outfile: Path, staging_dir: Path) -> None:
    """
    Download to a deterministic `.part` temp file in `staging_dir`, then move it to
    the final destination.
    """
    staging_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = staging_dir / (outfile.name + ".part")
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
) -> Path:
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
        Path to the downloaded file.
    """
    sem = validate_sem(sem)
    target_year, target_sem = str(year), sem
    outfile = build_paths(dataset, outdir, target_year, target_sem, ingestion_date)

    if outfile.exists():
        logging.info("[skip] File already exists: %s; skipping download.", outfile)
        return outfile

    logging.info(
        "[extract] Downloading: %s/ca-%s-%s.zip", base_url, target_year, target_sem
    )

    try:
        staging_download(
            f"{base_url}/ca-{target_year}-{target_sem}.zip", outfile, outdir_tmp
        )

    except urllib.error.HTTPError as e:
        if e.code == 404:
            if sem == "02":
                logging.warning(
                    "[warn] File not found for %s-%s, falling back to first semester (01)...",
                    year,
                    sem,
                )
                target_sem = "01"
                outfile = build_paths(
                    dataset, outdir, target_year, target_sem, ingestion_date
                )
                logging.info(
                    "[extract] Downloading: %s/ca-%s-%s.zip",
                    base_url,
                    target_year,
                    target_sem,
                )
                try:
                    staging_download(
                        f"{base_url}/ca-{target_year}-{target_sem}.zip",
                        outfile,
                        outdir_tmp,
                    )
                except urllib.error.HTTPError as e2:
                    if e2.code == 404:
                        logging.error(
                            "[error] File not found for %s-%s even after fallback to '01'.",
                            year,
                            target_sem,
                        )
                    raise
            else:
                logging.error(
                    "[error] File not found for %s-%s and no earlier semester to fallback to.",
                    year,
                    sem,
                )
                raise
        else:
            raise

    size = outfile.stat().st_size
    logging.info("[done] %.1f KB saved", size / 1024)

    return outfile


if __name__ == "__main__":
    # Defaults for CLI execution
    year_arg = os.environ.get("YEAR", dt.date.today().strftime("%Y"))
    sem_arg = os.environ.get("SEM", "01" if dt.date.today().month <= 6 else "02")
    ingestion_date_arg = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    dataset_arg = os.environ.get("DATASET", "anp_glp")
    outdir_arg = Path(
        os.environ.get(
            "OUTDIR", str(Path(__file__).resolve().parents[2] / "data" / "raw")
        )
    )
    outdir_tmp_arg = Path(
        os.environ.get(
            "OUTDIR_TMP", str(Path(__file__).resolve().parents[2] / "data" / "staging")
        )
    )
    base_url_arg = os.environ.get(
        "BASE_URL",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca",
    )

    outfile_path = run_extract(
        year=year_arg,
        sem=sem_arg,
        ingestion_date=ingestion_date_arg,
        dataset=dataset_arg,
        outdir=outdir_arg,
        outdir_tmp=outdir_tmp_arg,
        base_url=base_url_arg,
    )
    logging.info("Output file: %s", outfile_path)
