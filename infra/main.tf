# Local path to the app directory.
locals {
  app_dags_dir   = abspath("${path.root}/../app/dags")
  app_dbt_dir    = abspath("${path.root}/../app/dbt")
  app_elt_dir    = abspath("${path.root}/../app/elt")
  data_dir       = abspath("${path.root}/../data")
  database_dir   = abspath("${path.root}/../database")
  docker_dbt_dir = abspath("${path.root}/../docker/dbt")
}

# Creates a isolated Docker network for Airflow.
resource "docker_network" "airflow_net" {
  name = "airflow_net"
}

# Ensures that the data directory exists on the host filesystem.
resource "null_resource" "ensure_host_data_dir" {
  triggers = {
    path = local.data_dir
  }
  provisioner "local-exec" {
    command = "mkdir -p ${local.data_dir}"
  }
}

# Creates a Docker volume to persist Airflow data.
resource "docker_volume" "airflow_data" {
  name = "airflow_data"
  driver_opts = {
    type   = "none"
    device = local.data_dir
    o      = "bind"
  }
}

# Ensures that the database directory exists on the host filesystem.
resource "null_resource" "ensure_database_dir" {
  triggers = { path = local.database_dir }
  provisioner "local-exec" {
    command = "mkdir -p ${local.database_dir}"
  }
}

# Creates a Docker volume to persist database data.
resource "docker_volume" "database_data" {
  name = "database_data"
  driver_opts = {
    type   = "none"
    device = local.database_dir
    o      = "bind"
  }
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
    "PYTHONPATH=/opt/airflow",
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

  # Mounts elt directory from local filesystem to the container.
  mounts {
    type = "bind"
    source = local.app_elt_dir
    target = "/opt/airflow/elt"
  }

  # Mounts data directory using named volume from host directory.
  mounts {
    type = "volume"
    source = docker_volume.airflow_data.name
    target = "/data"
  }

  # Connects the container to the previously created Docker network.
  networks_advanced {
    name = docker_network.airflow_net.name
  }

  # Init command to start Airflow in standalone mode.
  command = [
    "bash", "-c",
    "chown -R 50000:0 /data && airflow standalone"] # Change ownership of /data to airflow user (uid 50000) and group (gid 0).
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

  # Mounts database directory using named volume from host directory.
  mounts {
    type = "volume"
    source = docker_volume.database_data.name
    target = "/database"
  }

  # Connects the container to the previously created Docker network.
  networks_advanced {
    name = docker_network.airflow_net.name
  }

}