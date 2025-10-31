# Data Platform Portfolio Project

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
![Python](https://img.shields.io/badge/Python-3.10-informational)
![Terraform](https://img.shields.io/badge/Terraform-Docker%20provider-informational)
![dbt](https://img.shields.io/badge/dbt-duckdb-informational)

This repository demonstrates how to build a **Local data platform** using **Airflow**, **dbt**, **DuckDB**, and **Terraform**, focusing on **reproducibility, reliability, and DataOps best practices**.

The project uses public data (e.g., [ANP GLP](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis)) to illustrate data ingestion, staging, modeling, and pipeline orchestration.

---

## Overview

- **Python (3.10)** – Ingestion and warehouse loading scripts.
- **dbt + DuckDB** – Local analytical data modeling.
- **Airflow 3.0+** – Pipeline orchestration (standalone mode).
- **Docker** – Base for custom images and local execution.
- **Terraform (docker provider)** – Infrastructure as code, managing containers and local networks.

---

## Architecture

Main flow:

1. **Ingestion** – Python scripts (`app/elt`) download and save files in (`data/incoming_path/`).
2. **Working** – Temporary processing in (`data/work_path/`).
3. **Processing** – Files already ingested into the database are moved to (`data/processed_path/`).
4. **Warehouse** – Persistence in (`database/warehouse.duckdb`).
5. **Modeling** – dbt (`app/dbt`) transforms data into analytical models.
6. **Orchestration** – Airflow runs DAGs (`app/dags`).
7. **Images** – Docker (`docker/`) contains custom image definitions.
8. **Infrastructure** – Terraform (`infra/`) manages containers and local networks.

---

## Folder Structure

```bash
.
├── app/                        # Main application code (Airflow DAGs, dbt project, and ELT scripts)
│   ├── dags/                   # Airflow DAGs (pipeline orchestration)
│   │   ├── anp_glp_ingestion_semianual.py  # Ingests and loads ANP GLP semiannual dataset
│   │   └── example_test_env.py             # Example DAG for testing local environment
│   ├── dbt/                    # dbt + DuckDB project (data modeling layer)
│   │   ├── dbt_project.yml
│   │   ├── models/             # SQL transformations and tests
│   │   │   └── example_test_env.sql         # Example SQL for testing local environment
│   │   ├── profiles.example.yml              # Template dbt profile (copy as profiles.yml)
│   │   ├── profiles.yml                      # Local dbt configuration (ignored in git)
│   │   └── target/             # dbt output (compiled SQL, run results, docs)
│   └── elt/                    # Ingestion and load scripts (Extract + Load)
│       ├── ingest_anp_glp.py  # Downloads and stages ANP GLP data
│       └── load_anp_glp.py    # Loads staged data into DuckDB
│
├── data/                       # Local storage for data files (consistent with Airflow & Python configs)
│   ├── incoming_path/          # Raw downloaded files
│   ├── processed_path/         # Files already loaded into the warehouse
│   └── work_path/              # Temporary area for cleaning/transformation
│
├── database/                   # Local DuckDB warehouse (*gitignored)
│   └── warehouse.duckdb
│
├── docker/                     # Custom Docker images for Airflow and dbt
│   ├── airflow/
│   │   ├── Dockerfile          # Custom Airflow image
│   │   └── requirements.txt    # Extra Python dependencies for Airflow
│   └── dbt/
│       ├── Dockerfile          # Custom dbt image
│       └── requirements.txt    # Extra Python dependencies for dbt
│
├── infra/                      # Terraform IaC (Docker provider) for local infrastructure
│   ├── main.tf                # Defines containers (Airflow, dbt-runner, volumes, networks)
│   ├── outputs.tf             # Exposes outputs (URL)
│   ├── providers.tf           # Docker provider configuration
│   └── variables.tf           # Input variables (ports, image names, etc.)
│
├── LICENSE
├── Makefile                   # Automation for common tasks (start, stop, docs, logs)
└── README.md
```

---
## Local Setup

### Prerequisites

- **Infrastructure**
  - [Docker](https://www.docker.com/) installed and running.
  - [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli) installed.

- **Language**
  - [Python 3.10+](https://www.python.org/) installed.

### 1. Clone the repository

Clone the repository and navigate to the project folder.

```bash
git clone https://github.com/vilasglauco/elt-airflow-dbt-terraform.git
cd elt-airflow-dbt-terraform
```

### 2. Create dbt Profile

Navigate to the dbt project folder and create your local profiles.yml from the example template. This file is ignored by git.

```bash
cp app/dbt/profiles.example.yml app/dbt/profiles.yml
```

### 3. Start the Infrastructure

Use the Makefile to build and start all services (Airflow, dbt-runner) with a single command.

```bash
make up
```

> This command initializes Terraform and applies the configuration, creating the necessary Docker containers, volumes, and networks.

### 4. Access Airflow

The Airflow UI will be available at [http://localhost:8080](http://localhost:8080).

No authentication enabled (Airflow 3.0 standalone with SimpleAuthManager disabled).

> **Security note:** In production environments, enabling authentication is recommended.
> Do not expose ports publicly. In production, enable authentication and use secure variables/secrets.

### 5. Run dbt Models

To run the data transformations, get a shell inside the dbt-runner container using the Makefile.

```bash
make dbt-shell
# Now, inside the container's shell:
dbt debug # Test the connection to DuckDB
dbt deps  # Install dependencies
dbt run   # Execute the models
dbt test  # Execute the models tests
```

### 6. Explore dbt Docs

Generate and serve dbt documentation locally.

```bash
make dbt-docs
```

> Access at: [http://localhost:8081](http://localhost:8081)

### 7. Access DuckDB

You can directly interact with the DuckDB database using the CLI

```bash
make duck-db
```

> **Note:** DuckDB does not support multiple concurrent writers. 
> If another process or container is connected to the same database with write permissions, 
> attempts to open it (even in read-only mode) may fail due to file locks. 

---

## Makefile Commands

For convenience, a `Makefile` is provided at the root of the project with common automation tasks:

```bash
make up           # Start infrastructure (Terraform apply)
make down         # Destroy infrastructure (Terraform destroy)
make plan         # Show Terraform execution plan
make dbt-shell    # Open a bash shell inside the dbt-runner container
make dbt-docs     # Generate and serve dbt documentation
make duck-db      # Open interactive DuckDB shell inside dbt-runner container
make logs-airflow # Stream Airflow logs
make help         # Show all available make commands with descriptions
```

---

## Ignored Directories and Files

- (`data/`) – Contains raw and intermediate files, ignored by git.
- (`database/`) – Local DuckDB, ignored by git.
- (`.gitkeep`) files maintain empty folder structure.
- (`infra/*.tfstate*`), (`.terraform/`), (`*.tfvars`), (`*.log`) – Terraform state/log files and directories.
- (`app/dbt/profiles.yml`), (`app/dbt/.user.yml`) – Local dbt user configurations.
- (`app/dbt/target/`), (`app/dbt/dbt_packages/`), (`app/dbt/logs/`), (`app/dbt/dbt_modules/`) – Temporary directories and dbt packages.
- Python: (`__pycache__/`), (`*.py[cod]`), (`.venv/`), (`venv/`), (`.env/`), (`env/`) – Cache files/directories and virtual environments.
- Docker: (`**/.dockerignore`), (`**/Dockerfile~`), (`**/docker-compose.override.yml`).

---

## Author

**Glauco Vilas**  
[LinkedIn](https://www.linkedin.com/in/vilasglauco/)  
[vilasglauco@gmail.com](mailto:vilasglauco@gmail.com)  
