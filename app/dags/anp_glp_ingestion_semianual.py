"""
DAG for semiannual ingestion of ANP GLP using a Python task.
This DAG extracts ANP GLP data twice a year, on January 1st and July 1st.
It uses the run_extract function defined in elt/ingest_anp_glp.py to perform the extraction.
Configuration parameters, such as output directories and base URL, are defined in the DAG params.
"""

from pathlib import Path

from airflow.utils import timezone
from airflow.decorators import dag, task

from elt.ingest_anp_glp import run_extract


@dag(
    dag_id="anp_glp_ingestion_semianual",
    description="Semiannual ingestion of ANP GLP using a Python task.",
    start_date=timezone.datetime(2025, 1, 1),
    schedule="0 0 1 1,7 *",  # January 1st and July 1st (cron)
    catchup=False,
    max_active_runs=1,
    params={
        "OUTDIR": "/data/raw",
        "OUTDIR_TMP": "/data/staging",
        "DATASET": "anp_glp",
        "BASE_URL": (
            "https://www.gov.br/anp/pt-br/centrais-de-conteudo/"
            "dados-abertos/arquivos/shpc/dsas/ca"
        ),
    },
    tags=["anp", "glp", "ingestion", "semiannual", "python"],
)
def anp_glp_ingestion_semianual():
    """DAG for semiannual ingestion of ANP GLP using a Python task."""

    @task(task_id="extract")
    def run_ingestion(
        year: str,
        sem: str,
        ingestion_date: str,
        dataset: str,
        outdir: str,
        outdir_tmp: str,
        base_url: str,
    ):
        # Convert string parameters to Path objects
        outdir = Path(outdir)
        outdir_tmp = Path(outdir_tmp)

        # Call the run_extract function with parameters from the DAG context
        result = run_extract(
            year=year,
            sem=sem,
            ingestion_date=ingestion_date,
            dataset=dataset,
            outdir=outdir,
            outdir_tmp=outdir_tmp,
            base_url=base_url,
        )

        return {
            "file_path": str(result["file_path"]),  # Path convertido para string
            "dataset": result["dataset"],
        }

    # Define the task execution with parameters from the DAG context
    run_ingestion(
        # Extract year and semester from the data interval start date
        year="{{ data_interval_start.year }}",
        # First semester for months 1–6, second semester for months 7–12
        sem="{{ '01' if data_interval_start.month <= 6 else '02' }}",
        # Current date and time for ingestion timestamp
        ingestion_date="{{ macros.datetime.now().strftime('%Y-%m-%d_%H%M%S') }}",
        # DAG params
        dataset="{{ params.DATASET }}",
        outdir="{{ params.OUTDIR }}",
        outdir_tmp="{{ params.OUTDIR_TMP }}",
        base_url="{{ params.BASE_URL }}",
    )


dag = anp_glp_ingestion_semianual()
