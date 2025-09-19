# Local path to the app directory.
locals {
  app_dags_dir   = abspath("${path.root}/../app/dags")
  app_dbt_dir    = abspath("${path.root}/../app/dbt")
  database_dir       = abspath("${path.root}/../database")
  docker_dbt_dir = abspath("${path.root}/../docker/dbt")
}

# Creates a isolated Docker network for Airflow.
resource "docker_network" "airflow_net" {
  name = "airflow_net"
}

# Creates the Docker container that will run Airflow.
resource "docker_container" "airflow" {
  name  = "airflow"
  image = var.airflow_image
  restart = "always"

  # Mapping of ports: exposes the Airflow UI on the port defined in variables.tf.
  ports {
    internal = 8080
    external = var.airflow_port
  }

  # Variables from Airflow environment:
  # - Disable example DAGs.
  # - Setup SimpleAuthManager so that all users are admins (only for local development to remove User authentication).
  env = [
    "AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags",
    "AIRFLOW__CORE__LOAD_EXAMPLES=False",
    "AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
    "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True",
    "AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=15",
    "AIRFLOW__SCHEDULER__DAG_DISCOVERY_REFRESH_INTERVAL=15",
  ]

  # Mounts dags directory from local filesystem to the container.
  mounts {
    type = "bind"
    source = local.app_dags_dir
    target = "/opt/airflow/dags"
  }

  # Connects the container to the previously created Docker network.
  networks_advanced {
    name = docker_network.airflow_net.name
  }

  # Init command to start Airflow in standalone mode.
  command = ["airflow", "standalone"]
}

# Builds a custom Docker image for dbt with the DuckDB adapter.
resource "docker_image" "dbt-duckdb-custom" {
  name = "dbt-duckdb-custom:stable"
  build {
    context    = local.docker_dbt_dir
    dockerfile = "${local.docker_dbt_dir}/Dockerfile"
  }
}

# Creates the Docker container that will run dbt commands.
resource "docker_container" "dbt-runner" {
  name  = "dbt-runner"
  image = docker_image.dbt-duckdb-custom.name
  command = ["tail", "-f", "/dev/null"]

  # Variables from dbt environment.
  env = [
    "DBT_PROJECT_DIR=/opt/dbt",
    "DBT_PROFILES_DIR=/opt/dbt",
  ]

  # Mapping of ports: exposes the dbt service on the port defined in variables.tf.
  ports {
    internal = 8081
    external = var.dbt_port
  }

  # Mounts app directory from local filesystem to the container.
  mounts {
    type = "bind"
    source = local.app_dbt_dir
    target = "/opt/dbt"
  }

  # Mounts database directory from local filesystem to the container.
  mounts {
    type = "bind"
    source = local.database_dir
    target = "/database"
  }

  # Connects the container to the previously created Docker network.
  networks_advanced {
    name = docker_network.airflow_net.name
  }

}