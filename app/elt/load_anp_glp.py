"""Module to load file into a DuckDB database."""

import hashlib
import logging
import os
import shutil
from pathlib import Path

import duckdb

# Constants
CONTROL_SCHEMA = "control_file"
PROCESSED_FILES_TABLE = f'"{CONTROL_SCHEMA}"."processed_files"'
INCOMING_PATH = "/data/incoming_path"

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


def move_file_to_processed(source_file: Path, processed_path: Path) -> None:
    """Move the original file to the 'processed' directory, including the partition directory.

    Args:
        source_file (Path): Path to the original file under INCOMING_PATH.
        processed_path (Path): Base directory path where the file will be moved.

    Raises:
        OSError: If the move operation fails due to I/O or permission errors.
    """
    # Determine the relative path from the INCOMING_PATH to preserve directory structure
    relative_path = Path(os.path.relpath(source_file, start=Path(INCOMING_PATH)))

    # Ensure the processed directory exists (preserving the full relative path)
    destination_path = processed_path / relative_path.parent
    try:
        os.makedirs(destination_path, exist_ok=True)
    except OSError as e:
        logging.error(
            "Failed to create processed directory %s: %s", destination_path, e
        )
        raise

    if source_file.exists() and source_file.is_file():
        # Move the file to the processed directory
        shutil.move(str(source_file), str(destination_path / source_file.name))
        logging.info(
            "Moved original file %s to processed directory %s",
            source_file,
            destination_path,
        )
    else:
        logging.warning(
            "Original file not found at expected location: %s", source_file
        )




def load_data_to_duckdb(
    final_file: Path,
    warehouse_path: Path,
    raw_schema: str,
    staging_schema: str,
    table_name: str,
    processed_path: Path,
) -> None:
    """Load CSV files directly into a DuckDB database using a staging approach.

    The Load flow is as follows:
        1. Verify the directory containing the CSV files.
        2. Connect to the DuckDB database located at 'warehouse_path'.
        3. Ensure the final schema, staging schema, and control schema exist.
        4. Ensure the 'processed_files' control table exists to track ingested files.
        5. For each CSV file:
            - Skip if already ingested in the current batch.
            - Begin a transaction.
            - Load data into a staging table, adding columns for source_file and ingestion 
                timestamp.
            - If the final table does not exist, create it from the staging table. Otherwise, insert
                staging data into the existing final table.
            - Record ingestion metadata in the control table.
            - Commit the transaction.
        6. Each CSV file is moved to the 'processed' directory immediately after successful processing or if already previously ingested.

    Args:
        final_file (Path): Path to the original CSV file.
        warehouse_path (Path): Path to the DuckDB database file.
        raw_schema (str): Target schema name for the final table.
        staging_schema (str): Schema name for staging tables.
        table_name (str): Name of the final table to load data into.
        processed_path (Path): Directory path where the original CSV file will be moved 
         after processing.

    Raises:
        FileNotFoundError: If the CSV file or its directory does not exist or contains no
         CSV files.
        ValueError: If the file type is not supported.
        duckdb.Error: If any DuckDB-related errors occur during connection or queries.
        OSError: If any I/O errors occur during reading files or moving files.

    Notes:
        - Each CSV file is loaded in its own transaction to isolate failures.
        - Each file is processed and committed independently to ensure isolated recovery.
        - The control table prevents reprocessing files based on file path and content checksum.
        - Moves each CSV file after successful processing or if already processed.
        - Raises OSError if there are I/O or permission issues during file moving.
    """
    source_file = Path(final_file)

    if not source_file.exists():
        raise FileNotFoundError(f"File not found: {source_file}")

    # Connect to DuckDB
    try:
        conn = duckdb.connect(database=str(warehouse_path), read_only=False)
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

    # Find all CSV files in the CSV file's directory
    csv_files = list(source_file.parent.glob("*.csv"))
    logging.info(
        "Found %d CSV files in directory %s",
        len(csv_files),
        source_file.parent,
    )
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in directory: {source_file.parent}"
        )

    staging_table_name = table_name
    if not does_table_exist(conn, staging_schema, table_name):
        # Create staging table based on the first CSV file's structure
        template_csv = str(csv_files[0])
        conn.execute(
            f"""
            CREATE TABLE "{staging_schema}"."{staging_table_name}" AS
            SELECT * 
                ,''::VARCHAR AS source_file
                ,NULL::TIMESTAMP AS ingestion_ts
            FROM read_csv_auto('{template_csv}', HEADER=True, hive_partitioning=1)
            WHERE 1=0; -- No data, just structure
            """
        )

    # Truncate staging table to ensure it's empty before loading
    conn.execute(f'TRUNCATE TABLE "{staging_schema}"."{staging_table_name}";')
    logging.info(
        "Truncated staging table %s.%s before loading new batch",
        staging_schema,
        staging_table_name,
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
                "Skipping file %s as it was already ingested (checksum match). Moving to processed_path.",
                csv_file,
            )
            # Move the original file to the 'processed' directory including the partition directory
            move_file_to_processed(csv_file, processed_path)
            continue

        try:
            conn.execute("BEGIN")
            # Insert data from CSV into staging table with ingestion columns
            conn.execute(
                f"""
                INSERT INTO "{staging_schema}"."{staging_table_name}" 
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
                staging_table_name,
            )

            if not does_table_exist(conn, raw_schema, table_name):
                # Create raw table from staging
                conn.execute(
                    f"""
                    CREATE TABLE "{raw_schema}"."{table_name}" AS
                        SELECT * 
                    FROM "{staging_schema}"."{staging_table_name}";
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
                    FROM "{staging_schema}"."{staging_table_name}";
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
                FROM "{staging_schema}"."{staging_table_name}"
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

            # Move the original file to the 'processed' directory including the partition directory
            move_file_to_processed(csv_file, processed_path)

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

    conn.commit()
    logging.info("ETL load process completed successfully for file %s", final_file)
    conn.close()
    logging.info("Closed DuckDB connection.")
