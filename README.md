# Data Platform Portfolio Project

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
![Python](https://img.shields.io/badge/Python-3.12-informational)
![Terraform](https://img.shields.io/badge/Terraform-Docker%20provider-informational)
![dbt](https://img.shields.io/badge/dbt-duckdb-informational)

This repository demonstrates how to build a **Local data platform** using **Airflow**, **dbt**, **DuckDB**, and **Terraform**, focusing on **reproducibility, reliability, and DataOps best practices**.

The project uses public data (e.g., [ANP GLP](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis)) to illustrate data ingestion, staging, modeling, and pipeline orchestration.

---

## Overview

- **Python (3.12)** – Ingestion and warehouse loading scripts.
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
├── app/                     # Main code
│   ├── dags/                # Airflow DAGs
│   │   ├── anp_glp_ingestion_semianual.py
│   │   └── example_test_env.py
│   ├── dbt/                 # dbt + DuckDB project
│   │   ├── dbt_project.yml
│   │   ├── models/
│   │   │   └── example_test_env.sql
│   │   ├── profiles.example.yml
│   │   ├── profiles.yml
│   │   └── target/          # dbt output (compiled, run results, docs)
│   └── elt/                 # Ingestion and load scripts
│       ├── ingest_anp_glp.py
│       └── load_anp_glp.py
├── data/                    # Local storage for all project files
│   ├── incoming_path/       
│   ├── processed_path/      
│   └── work_path/           
├── database/                # DuckDB warehouse (*gitignored)
│   └── warehouse.duckdb
│
├── docker/                  # Custom images
│   ├── airflow/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── dbt/
│       ├── Dockerfile       
│       └── requirements.txt
│
├── infra/                   # Terraform infrastructure
│   ├── main.tf
│   ├── outputs.tf
│   ├── providers.tf
│   └── variables.tf
├── LICENSE
├── Makefile
└── README.md
```

---
## Local Setup

### Prerequisites

- **Infrastructure**
  - [Docker](https://www.docker.com/) installed and running.
  - [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli) installed.

- **Language**
  - [Python 3.12+](https://www.python.org/) installed.

### 1. Clone the repository

Clone the repository and navigate to the project folder:

```bash
git clone https://github.com/vilasglauco/elt-airflow-dbt-terraform.git
cd elt-airflow-dbt-terraform
```

### 2. Initialize local infrastructure with Terraform

Initialize Docker containers, volumes, and networks using Terraform:

```bash
cd infra
terraform init
terraform apply
```
> This will create Airflow containers, Docker volumes and local networks.

### 3. Initialize dbt

Run dbt inside the `dbt-runner` container to set up and execute transformations:

```bash
docker exec -it dbt-runner bash
dbt debug
dbt run
```

### 4. Generate and access dbt documentation

Generate and serve dbt documentation locally:

```bash
docker exec -it dbt-runner bash
dbt docs generate
dbt docs serve --host 0.0.0.0 --port 8081
```
> Access at: [http://localhost:8081](http://localhost:8081)

### 5. Access duckdb

Open an interactive DuckDB shell inside the `dbt-runner` container:

```bash
docker exec -it dbt-runner duckdb /database/warehouse.duckdb
```

> **Note:** DuckDB does not support multiple concurrent writers. 
> If another process or container is connected to the same database with write permissions, 
> attempts to open it (even in read-only mode) may fail due to file locks. 

### 6. Access Airflow

- Web Interface: [http://localhost:8080](http://localhost:8080)
- No authentication enabled (Airflow 3.0 standalone with SimpleAuthManager disabled).

> **Security note:** In production environments, enabling authentication is recommended.
> Do not expose ports publicly. In production, enable authentication and use secure variables/secrets.

---

## Makefile Commands

For convenience, a `Makefile` is provided at the root of the project with common automation tasks:

```bash
make up           # Start infrastructure (Terraform apply)
make down         # Destroy infrastructure (Terraform destroy)
make dbt-docs     # Generate and serve dbt documentation
make duck-db      # Open interactive DuckDB shell inside dbt-runner container
make logs-airflow # Stream Airflow logs
make logs-dbt     # Stream dbt-runner logs
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

## Next Steps

- [ ] Run dbt models to deduplicate raw data (dbt run)
- [ ] Apply basic data cleaning transformations (e.g., trim strings, fix nulls)

---

## Author

Glauco Vilas 
[LinkedIn](https://www.linkedin.com/in/vilasglauco/)  
[vilasglauco@gmail.com](mailto:vilasglauco@gmail.com)