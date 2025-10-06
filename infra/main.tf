# Local path to the app directory.
locals {
  app_dags_dir   = abspath("${path.root}/${var.app_dags_dir}")
  app_dbt_dir    = abspath("${path.root}/${var.app_dbt_dir}")
  app_elt_dir    = abspath("${path.root}/${var.app_elt_dir}")
  data_dir       = abspath("${path.root}/${var.data_dir}")
  database_dir   = abspath("${path.root}/${var.database_dir}")
  docker_airflow_dir = abspath("${path.root}/${var.docker_airflow_dir}")
  docker_dbt_dir = abspath("${path.root}/${var.docker_dbt_dir}")
}

# Creates a isolated Docker network for Airflow.
resource "docker_network" "airflow_net" {
  name = var.docker_network_name
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
resource "docker_volume" "airflow_volume" {
  name = var.airflow_volume_name
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
resource "docker_volume" "database_volume" {
  name = var.database_volume_name
  driver_opts = {
    type   = "none"
    device = local.database_dir
    o      = "bind"
  }
}

# Builds a custom Docker image for Airflow with necessary dependencies.
resource "docker_image" "airflow-custom" {
  name = "airflow-custom:${var.airflow_image_tag}"
  build {
    context    = local.docker_airflow_dir
    dockerfile = "${local.docker_airflow_dir}/Dockerfile"
  }
  
}

# Creates the Docker container that will run Airflow.
resource "docker_container" "airflow" {
  name  = var.airflow_container_name
  image = docker_image.airflow-custom.name
  restart = "always"

  # Mapping of ports: exposes the Airflow UI on the port defined in variables.tf.
  ports {
    internal = var.airflow_external_port
    external = var.airflow_external_port
  }

  # Variables from Airflow environment:
  # - Disable example DAGs.
  # - Setup SimpleAuthManager so that all users are admins (only for local development to remove User authentication).
  env = [
    "PYTHONPATH=${var.airflow_pythonpath}",
    "AIRFLOW__CORE__DAGS_FOLDER=${var.airflow_dags_folder}",
    "AIRFLOW__CORE__LOAD_EXAMPLES=${var.airflow_load_examples}",
    "AIRFLOW__CORE__AUTH_MANAGER=${var.airflow_auth_manager}",
    "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=${var.airflow_all_admins}",
    "AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=${var.scheduler_dag_dir_list_interval}",
    "AIRFLOW__SCHEDULER__DAG_DISCOVERY_REFRESH_INTERVAL=${var.scheduler_dag_discovery_refresh_interval}",
  ]

  # Mounts dags directory from local filesystem to the container.
  mounts {
    type = "bind"
    source = local.app_dags_dir
    target = "${var.airflow_container_dags_dir}"
  }

  # Mounts elt directory from local filesystem to the container.
  mounts {
    type = "bind"
    source = local.app_elt_dir
    target = "${var.airflow_container_elt_dir}"
  }

  # Mounts data directory using named volume from host directory.
  mounts {
    type = "volume"
    source = docker_volume.airflow_volume.name
    target = "${var.airflow_container_data_dir}"
  }

  # Mounts database directory using named volume from host directory.
  mounts {
    type = "volume"
    source = docker_volume.database_volume.name
    target = "${var.dbt_container_database_dir}"
  }

  # Connects the container to the previously created Docker network.
  networks_advanced {
    name = docker_network.airflow_net.name
  }

  # Init command to start Airflow in standalone mode.
  command = [
    "bash", "-c",
    "chown -R ${var.airflow_uid}:${var.airflow_gid} /data && airflow standalone"] # Change ownership of /data to airflow user (uid 50000) and group (gid 0).
}

# Builds a custom Docker image for dbt with the DuckDB adapter.
resource "docker_image" "dbt-duckdb-custom" {
  name = "dbt-duckdb-custom:${var.dbt_image_tag}"
  build {
    context    = local.docker_dbt_dir
    dockerfile = "${local.docker_dbt_dir}/Dockerfile"
  }
}

# Creates the Docker container that will run dbt commands.
resource "docker_container" "dbt-runner" {
  name  = var.dbt_runner_container_name
  image = docker_image.dbt-duckdb-custom.name
  command = ["tail", "-f", "/dev/null"]

  # Variables from dbt environment.
  env = [
    "DBT_PROJECT_DIR=${var.dbt_project_dir}",
    "DBT_PROFILES_DIR=${var.dbt_profiles_dir}",
  ]

  # Mapping of ports: exposes the dbt service on the port defined in variables.tf.
  ports {
    internal = var.dbt_internal_port
    external = var.dbt_external_port
  }

  # Mounts app directory from local filesystem to the container.
  mounts {
    type = "bind"
    source = local.app_dbt_dir
    target = "${var.dbt_container_project_dir}"
  }

  # Mounts database directory using named volume from host directory.
  mounts {
    type = "volume"
    source = docker_volume.database_volume.name
    target = "${var.dbt_container_database_dir}"
  }

  # Connects the container to the previously created Docker network.
  networks_advanced {
    name = docker_network.airflow_net.name
  }

}