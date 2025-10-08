"""Module to extract a ZIP file containing CSV files and load them into a DuckDB database."""

import hashlib
import logging
import os
import shutil
import zipfile
from pathlib import Path

import duckdb

# Constants
CONTROL_SCHEMA = "control_file"
PROCESSED_FILES_TABLE = f'"{CONTROL_SCHEMA}"."processed_files"'

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def prepare_schema(conn: duckdb.DuckDBPyConnection, schema: str) -> None:
    """Ensure the specified schema exists in the DuckDB database.

    Creates the schema if it does not already exist, allowing subsequent
    operations to safely assume the schema's presence.

    Args:
        conn (duckdb.DuckDBPyConnection): Active DuckDB connection object.
        schema (str): Name of the schema to create or verify.

    Raises:
        duckdb.Error: If there is an error executing the schema creation query.
    """
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')


def create_control_files_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Ensure the 'processed_files' control table exists within the control schema.

    The table tracks ingested files by recording their source filename, ingestion timestamp,
    number of rows, file checksum, and load status. The primary key on `source_file`
    prevents reprocessing the same file path, and a unique constraint on `file_checksum`
    prevents reprocessing identical file content.

    Args:
        conn (duckdb.DuckDBPyConnection): Active DuckDB connection object.

    Raises:
        duckdb.Error: If there is an error creating the control table.
    """
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {PROCESSED_FILES_TABLE} (
            source_file VARCHAR NOT NULL
            ,ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ,rows_ingested BIGINT NOT NULL
            ,file_checksum VARCHAR NOT NULL
            ,load_status VARCHAR DEFAULT 'loaded'
            ,PRIMARY KEY (source_file)
            ,CONSTRAINT uq_checksum UNIQUE (file_checksum)
        );
        """
    )


def calculate_checksum(file_path: Path) -> str:
    """Calculate SHA-256 checksum for the given file path.

    Args:
        file_path (Path): Path to the file for which to calculate the checksum.

    Returns:
        str: Hexadecimal string representation of the file's SHA-256 checksum.

    Raises:
        OSError: If there are I/O issues reading the file.
    """
    h = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            # Read and update hash string value in blocks of 8K
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
    except OSError as e:
        logging.error("Error reading file %s for checksum: %s", file_path, e)
        raise
    return h.hexdigest()


def extract_zip(source_zip: Path) -> Path:
    """Extract a ZIP archive into a dedicated directory named after the ZIP file's stem.

    The extracted files are placed inside a subdirectory 'extracted/<zip_stem>' relative
    to the ZIP file location. This organization helps manage multiple extractions.

    Args:
        source_zip (Path): Path to the ZIP file to extract.

    Returns:
        Path: Directory path where the ZIP contents were extracted.

    Raises:
        FileNotFoundError: If the ZIP file does not exist.
        zipfile.BadZipFile: If the file is not a valid ZIP archive.
        OSError: If there are I/O or permission issues during extraction.
    """
    # Ensure the ZIP file path is a Path object
    source_zip = Path(source_zip)

    if not source_zip.exists():
        raise FileNotFoundError(f"ZIP not Found: {source_zip}")

    # Create extraction directory based on ZIP file name
    zip_stem = source_zip.stem
    extracted_path = source_zip.parent / "extracted" / zip_stem
    try:
        os.makedirs(extracted_path, exist_ok=True)
    except OSError as e:
        logging.error("Failed to create extraction directory %s: %s", extracted_path, e)
        raise

    with zipfile.ZipFile(source_zip, "r") as zf:
        # Extract all contents into the extraction directory
        zf.extractall(extracted_path)
        logging.info(
            "Extracted ZIP file %s to directory %s", source_zip, extracted_path
        )
    return extracted_path


def does_table_exist(
    conn: duckdb.DuckDBPyConnection, schema: str, table_name: str
) -> bool:
    """Check whether a table exists within a specific schema in the DuckDB database.

    Args:
        conn (duckdb.DuckDBPyConnection): Active DuckDB connection object.
        schema (str): Schema name to check within.
        table_name (str): Name of the table to verify.

    Returns:
        bool: True if the table exists, False otherwise.

    Raises:
        duckdb.Error: If there is an error querying the information schema.
    """
    result = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_name = ?;
        """,
        (schema, table_name),
    ).fetchone()[0]
    return result > 0


def move_zip_to_processed(source_zip: Path, processed_path: Path) -> None:
    """Move the original ZIP file to the 'processed' directory, including the partition directory.

    Args:
        source_zip (Path): Path to the original ZIP file.
        processed_path (Path): Base directory path where the ZIP file will be moved.

    Raises:
        OSError: If the move operation fails due to I/O or permission errors.
    """
    # Ensure the processed directory exists
    destination_path = processed_path / source_zip.parent.name
    try:
        os.makedirs(destination_path, exist_ok=True)
    except OSError as e:
        logging.error(
            "Failed to create processed directory %s: %s", destination_path, e
        )
        raise

    if source_zip.exists() and source_zip.is_file():
        # Move the ZIP file to the processed directory
        shutil.move(str(source_zip), str(destination_path / source_zip.name))
        logging.info(
            "Moved original ZIP file %s to processed directory %s",
            source_zip,
            destination_path,
        )
    else:
        logging.warning(
            "Original ZIP file not found at expected location: %s", source_zip
        )


def cleanup_extracted_path(extracted_path: Path) -> None:
    """Remove the subdirectory corresponding to the processed ZIP file inside the
    extracted directory.

    Args:
        extracted_path (Path): Path to the extracted directory corresponding to the ZIP file.

    Raises:
        OSError: If removal of the directory fails due to I/O or permission errors.
    """
    if extracted_path.exists() and extracted_path.is_dir():
        try:
            shutil.rmtree(extracted_path)
            logging.info("Removed extracted subdirectory %s", extracted_path)
        except OSError as e:
            logging.error(
                "Failed to remove extracted subdirectory %s: %s", extracted_path, e
            )
    else:
        logging.warning(
            "Extracted subdirectory %s does not exist or is not a directory",
            extracted_path,
        )


def load_data_to_duckdb(
    final_file: Path,
    warehouse_path: Path,
    raw_schema: str,
    staging_schema: str,
    table_name: str,
    processed_path: Path,
) -> None:
    """Load CSV files extracted from a ZIP archive into a DuckDB database using a staging approach.

    The Load flow is as follows:
        1. Extract the ZIP archive to a temporary directory.
        2. Verify the extracted directory contains CSV files.
        3. Connect to the DuckDB database located at 'warehouse_path'.
        4. Ensure the final schema, staging schema, and control schema exist.
        5. Ensure the 'processed_files' control table exists to track ingested files.
        6. For each CSV file:
            - Skip if already ingested in the current batch.
            - Begin a transaction.
            - Load data into a staging table, adding columns for source_file and ingestion 
                timestamp.
            - If the final table does not exist, create it from the staging table. Otherwise, insert
                staging data into the existing final table.
            - Record ingestion metadata in the control table.
            - Commit the transaction.
        7. After processing all CSVs, move the original ZIP file to the 'processed' directory.
        8. Clean up extracted files.
        9. Close the database connection.

    Args:
        final_file (Path): Path to the original ZIP file.
        warehouse_path (Path): Path to the DuckDB database file.
        raw_schema (str): Target schema name for the final table.
        staging_schema (str): Schema name for staging tables.
        table_name (str): Name of the final table to load data into.
        processed_path (Path): Directory path where the original ZIP file will be moved 
         after processing.

    Raises:
        FileNotFoundError: If the ZIP file or extracted directory does not exist or contains no
         CSV files.
        duckdb.Error: If any DuckDB-related errors occur during connection or queries.
        OSError: If any I/O errors occur during reading files, extracting, or moving files.

    Notes:
        - Each CSV file is loaded in its own transaction to isolate failures.
        - The control table prevents reprocessing files based on file path and content checksum.
        - Moves the original ZIP file after processing, not the extracted CSVs.
        - Raises OSError if there are I/O or permission issues during extraction, file moving, or
            directory cleanup.
    """
    source_zip = Path(final_file)

    if not source_zip.exists():
        raise FileNotFoundError(f"ZIP file not found: {source_zip}")

    # Extract ZIP contents
    extracted_path = extract_zip(source_zip)

    # Ensure the extracted directory exists
    if not extracted_path.exists():
        raise FileNotFoundError(f"Extracted directory not found: {extracted_path}")

    # Connect to DuckDB
    try:
        conn = duckdb.connect(database=warehouse_path, read_only=False)
        logging.info("Connected successfully to DuckDB at %s", warehouse_path)
    except duckdb.Error as e:
        logging.error("Failed to connect to DuckDB at %s: %s", warehouse_path, e)
        raise

    # Prepare final, staging, and control schemas
    prepare_schema(conn, raw_schema)
    prepare_schema(conn, staging_schema)
    prepare_schema(conn, CONTROL_SCHEMA)
    logging.info(
        "Schemas ensured: %s, %s, %s", raw_schema, staging_schema, CONTROL_SCHEMA
    )

    # Ensure processed_files ingestion table exists
    create_control_files_table(conn)
    logging.info("Control table ensured: %s", PROCESSED_FILES_TABLE)

    # Find all CSV files in the extracted directory
    csv_files = list(extracted_path.glob("*.csv"))
    logging.info(
        "Found %d CSV files in extracted directory %s", len(csv_files), extracted_path
    )
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in directory: {extracted_path}")

    staging_table = f"staging_{table_name}"
    if not does_table_exist(conn, staging_schema, staging_table):
        # Create staging table based on the first CSV file's structure
        template_csv = str(csv_files[0])
        conn.execute(
            f"""
            CREATE TABLE "{staging_schema}"."{staging_table}" AS
            SELECT * 
                ,''::VARCHAR AS source_file
                ,NULL::TIMESTAMP AS ingestion_ts
            FROM read_csv_auto('{template_csv}', HEADER=True)
            WHERE 1=0; -- No data, just structure
            """
        )

    # Truncate staging table to ensure it's empty before loading
    conn.execute(f'TRUNCATE TABLE "{staging_schema}"."{staging_table}";')
    logging.info(
        "Truncated staging table %s.%s before loading new batch",
        staging_schema,
        staging_table,
    )

    # Load each CSV file into the staging table with a transaction per file
    for csv_file in csv_files:
        csv_file_path = str(csv_file)

        # Compute checksum and skip if the exact file content was already processed
        checksum = calculate_checksum(csv_file)
        file_already_processed = (
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {PROCESSED_FILES_TABLE}
                WHERE file_checksum = ?
                """,
                (checksum,),
            ).fetchone()[0]
            > 0
        )

        if file_already_processed:
            logging.info(
                "Skipping file %s as it was already ingested (checksum match).",
                csv_file,
            )
            continue

        try:
            conn.execute("BEGIN")
            # Insert data from CSV into staging table with ingestion columns
            conn.execute(
                f"""
                INSERT INTO "{staging_schema}"."{staging_table}" 
                    SELECT *
                    ,'{csv_file_path}' AS source_file 
                    ,CURRENT_TIMESTAMP AS ingestion_ts
                FROM read_csv_auto('{csv_file_path}', HEADER=True);
                """
            )
            logging.info(
                "Loaded data from file %s into staging table %s.%s",
                csv_file,
                staging_schema,
                staging_table,
            )

            if not does_table_exist(conn, raw_schema, table_name):
                # Create raw table from staging
                conn.execute(
                    f"""
                    CREATE TABLE "{raw_schema}"."{table_name}" AS
                        SELECT * 
                    FROM "{staging_schema}"."{staging_table}";
                    """
                )
                logging.info(
                    "Created final table %s.%s from staging data",
                    raw_schema,
                    table_name,
                )
            else:
                # Insert into existing raw table
                conn.execute(
                    f"""
                    INSERT INTO "{raw_schema}"."{table_name}"
                        SELECT * 
                    FROM "{staging_schema}"."{staging_table}";
                    """
                )
                logging.info(
                    "Inserted staging rows into existing table %s.%s",
                    raw_schema,
                    table_name,
                )

            # Count rows ingested from the CSV file in staging table
            rows_ingested = conn.execute(
                f"""
                SELECT COUNT(*) 
                FROM "{staging_schema}"."{staging_table}"
                WHERE source_file = ?;
                """,
                (csv_file_path,),
            ).fetchone()[0]

            # Register the file as ingested with rows_ingested and checksum
            conn.execute(
                f"""
                INSERT INTO {PROCESSED_FILES_TABLE} (
                    source_file                    
                    ,rows_ingested
                    ,file_checksum
                    ,load_status
                )
                VALUES (?, ?, ?, 'loaded');
                """,
                (csv_file_path, rows_ingested, checksum),
            )
            logging.info(
                "Registered file %s in processed_files with %d rows ingested",
                csv_file,
                rows_ingested,
            )

            conn.execute("COMMIT")
            logging.info("Committed transaction successfully for file %s", csv_file)

        except duckdb.Error as e:
            conn.execute("ROLLBACK")
            logging.error(
                "Error loading data for file %s (transaction rolled back): %s",
                csv_file,
                e,
            )
            raise
        except OSError as e:
            conn.execute("ROLLBACK")
            logging.error(
                "I/O error during loading for file %s (transaction rolled back): %s",
                csv_file,
                e,
            )
            raise

    # Move the original ZIP file to the 'processed' directory including the partition directory
    move_zip_to_processed(source_zip, processed_path)

    # Cleanup extracted subdirectory corresponding to the ZIP file
    cleanup_extracted_path(extracted_path)

    conn.commit()
    logging.info("ETL load process completed successfully for ZIP file %s", final_file)
    conn.close()
    logging.info("Closed DuckDB connection.")
