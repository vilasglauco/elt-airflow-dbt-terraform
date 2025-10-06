"""DAG for semiannual ingestion of ANP GLP using a Python task."""

from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils import timezone
from elt.ingest_anp_glp import extract_data_from_anp
from elt.load_anp_glp import load_data_to_duckdb


@dag(
    dag_id="anp_glp_ingestion_semianual",
    description="Semiannual ingestion of ANP GLP using a Python task.",
    start_date=timezone.datetime(2025, 1, 1),
    schedule="0 0 1 1,7 *",  # January 1st and July 1st (cron)
    catchup=False,
    max_active_runs=1,
    params={
        "DATASET_BASE_URL": (
            "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/"
            "shpc/dsas/ca"
        ),
        "INCOMING_PATH": "/data/incoming_path",
        "WORK_PATH": "/data/work_path",
        "PROCESSED_PATH": "/data/processed_path",
        "SOURCE_NAME": "anp_glp",
        "WAREHOUSE_PATH": "/database/warehouse.duckdb",
        "RAW_SCHEMA": "raw",
        "STAGING_SCHEMA": "staging",
        "TABLE_NAME": "historico_precos_combustiveis_glp",
    },
    tags=["anp", "glp", "ingestion", "semiannual", "python"],
)
def anp_glp_ingestion_semianual():
    """DAG for semiannual ingestion of ANP GLP data.

    This DAG orchestrates the extraction and loading of ANP GLP data twice a year,
    specifically on January 1st and July 1st, using Python tasks.

    Tasks:
        - extract: Extracts data from the ANP dataset for the specified year and semester.
        - load: Loads the extracted data into a DuckDB warehouse, organizing it into raw and
            staging schemas.

    Scheduling:
        The DAG runs semiannually, triggered on the first day of January and July,
        corresponding to the first and second semesters respectively.

    Parameters:
        - DATASET_BASE_URL: Base URL for the ANP dataset files.
        - INCOMING_PATH: Directory path for incoming raw data files.
        - WORK_PATH: Directory path for intermediate processing files.
        - PROCESSED_PATH: Directory path for processed data files.
        - SOURCE_NAME: Identifier for the data source.
        - WAREHOUSE_PATH: File path to the DuckDB warehouse database.
        - RAW_SCHEMA: Database schema name for raw data.
        - STAGING_SCHEMA: Database schema name for staging data.
        - TABLE_NAME: Target table name in the warehouse.

    Notes:
        The extract task returns the path to the final extracted file via XCom,
        which is then consumed by the load task to perform data loading.
    """

    @task(task_id="extract")
    def run_extract(
        year: str,
        sem: str,
        ingestion_date: str,
        source_name: str,
        incoming_path: str,
        work_path: str,
        dataset_base_url: str,
    ) -> dict[str, str]:
        # Convert string parameters to Path objects
        incoming_path = Path(incoming_path)
        work_path = Path(work_path)

        # Call the extraction function
        extraction_result = extract_data_from_anp(
            year=year,
            sem=sem,
            ingestion_date=ingestion_date,
            source_name=source_name,
            incoming_path=incoming_path,
            work_path=work_path,
            dataset_base_url=dataset_base_url,
        )

        # Return the path to the final extracted file for the next task
        return {
            "final_file": str(extraction_result["final_file"]),
        }

    @task(task_id="load")
    def run_load(
        final_file_path: str,
        warehouse_path: str,
        processed_path: str,
        raw_schema: str,
        staging_schema: str,
        table_name: str,
    ):
        final_file = Path(final_file_path)
        warehouse_path = Path(warehouse_path)
        processed_path = Path(processed_path)

        load_data_to_duckdb(
            final_file=final_file,
            warehouse_path=warehouse_path,
            raw_schema=raw_schema,
            staging_schema=staging_schema,
            table_name=table_name,
            processed_path=processed_path,
        )

    # Define the task execution with parameters from the DAG context
    extract_task = run_extract(
        # Extract year and semester from the data interval start date
        year="{{ data_interval_start.year }}",
        # First semester for months 1–6, second semester for months 7–12
        sem="{{ '01' if data_interval_start.month <= 6 else '02' }}",
        # Current date and time for ingestion timestamp
        ingestion_date="{{ macros.datetime.now().strftime('%Y-%m-%d_%H%M%S') }}",
        # DAG params
        source_name="{{ params.SOURCE_NAME }}",
        incoming_path="{{ params.INCOMING_PATH }}",
        work_path="{{ params.WORK_PATH }}",
        dataset_base_url="{{ params.DATASET_BASE_URL }}",
    )

    # The load task automatically receives the output of extract via XCom
    load_task = run_load(
        extract_task["final_file"],
        warehouse_path="{{ params.WAREHOUSE_PATH }}",
        processed_path="{{ params.PROCESSED_PATH }}",
        raw_schema="{{ params.RAW_SCHEMA }}",
        staging_schema="{{ params.STAGING_SCHEMA }}",
        table_name="{{ params.TABLE_NAME }}",
    )

    # Define the dependency explicitly
    extract_task >> load_task


# Instantiate the DAG
dag = anp_glp_ingestion_semianual()
