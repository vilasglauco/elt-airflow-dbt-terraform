# Data Platform Portfolio Project

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
![Python](https://img.shields.io/badge/Python-3.12-informational)
![Terraform](https://img.shields.io/badge/Terraform-Docker%20provider-informational)
![dbt](https://img.shields.io/badge/dbt-duckdb-informational)

This repository demonstrates how to build a **Local-to-Cloud data platform** using **Airflow**, **dbt**, **DuckDB**, and **Terraform**, focusing on **reproducibility, reliability, and DataOps best practices**.

The project uses public data (e.g., [ANP GLP](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis)) to illustrate data ingestion, staging, modeling, and pipeline orchestration.

---

## Overview

- **Python (3.12)** – Ingestion and warehouse loading scripts.
- **dbt + DuckDB** – Local analytical data modeling.
- **Airflow 3.0+** – Pipeline orchestration (standalone mode).
- **Docker** – Base for custom images and local execution.
- **Terraform (docker provider)** – Infrastructure as code, managing containers and local networks.
- **Local-to-Cloud ready** – Structure prepared for future migration to **BigQuery** and **Google Cloud Storage** (GCP).

---

## Architecture

Main flow:

1. **Ingestion** – Python scripts (`app/elt`) download and save files in (`data/raw/`).
2. **Staging** – Temporary location for file downloads in (`data/staging/`).
3. **Warehouse** – Persistence in (`database/warehouse.duckdb`).
4. **Modeling** – dbt (`app/dbt`) transforms data into analytical models.
5. **Orchestration** – Airflow runs DAGs (`app/dags`).
6. **Images** – Docker (`docker/`) contains custom image definitions.
7. **Infrastructure** – Terraform (`infra/`) manages containers and local networks.

---

## Folder Structure

```bash
.
├── app/                     # Main code
│   ├── dags/                # Airflow DAGs
│   │   └── example_test_env.py
│   ├── dbt/                 # dbt + DuckDB project
│   │   ├── dbt_project.yml  # Project configuration (entrypoint)
│   │   ├── models/
│   │   │   └── example_test_env.sql
│   │   ├── profiles.example.yml  # Example dbt profile configuration
│   └── elt/                 # Ingestion scripts
│       └── ingest_anp_glp.py
│
├── data/                    # Local data (*gitignored)
│   ├── raw/                 
│   └── staging/             
│
├── database/                # DuckDB warehouse (*gitignored)
│   └── warehouse.duckdb
│
├── docker/                  # Custom images
│   └── dbt/
│       ├── Dockerfile       
│       └── requirements.txt
│
└── infra/                   # Terraform IaC
    ├── main.tf              # Project configuration (entrypoint)
    ├── providers.tf
    ├── variables.tf
    └── terraform.tfstate*   # (*gitignored)
```

## Local Setup

### Prerequisites

- **Infrastructure**
  - [Docker](https://www.docker.com/) installed and running.
  - [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli) installed.

- **Language**
  - [Python 3.12+](https://www.python.org/) installed.

### 1. Clone the repository

```bash
git clone https://github.com/vilasglauco/elt-airflow-dbt-terraform.git
cd elt-airflow-dbt-terraform
```

### 2. Initialize local infrastructure with Terraform

```bash
cd infra
terraform init
terraform apply
```
> This will create Airflow containers, local networks via Docker.

### 3. Initialize dbt

```bash
docker exec -it dbt-runner bash
dbt debug
dbt run
```

### 4. Generate and access dbt documentation

```bash
docker exec -it dbt-runner bash
dbt docs generate
dbt docs serve --host 0.0.0.0 --port 8081
```
> Access at: [http://localhost:8081](http://localhost:8081)

### 5. Data ingestion (Example via CLI)

```bash
python3 app/elt/ingest_anp_glp.py
```

### 6. Access Airflow

- Web Interface: [http://localhost:8080](http://localhost:8080)
- No authentication enabled (Airflow 3.0 standalone with SimpleAuthManager disabled).

> **Security note:**: In production environments, enabling authentication is recommended.
> Do not expose ports publicly. In production, enable authentication and use secure variables/secrets.

---

## Makefile Commands

For convenience, a `Makefile` is provided at the root of the project with common automation tasks:

```bash
make up           # Start infrastructure (Terraform apply)
make down         # Destroy infrastructure (Terraform destroy)
make dbt-docs     # Generate and serve dbt documentation
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

- [ ] Load data into the DuckDB warehouse.

---

## Author

Glauco Vilas 
[LinkedIn](https://www.linkedin.com/in/vilasglauco/)  
[vilasglauco@gmail.com](mailto:vilasglauco@gmail.com)