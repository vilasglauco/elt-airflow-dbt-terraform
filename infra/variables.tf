# Variable for the DAGs directory.
variable "app_dags_dir" {
  type    = string
  default = ("/../app/dags")
}

# Variable for the dbt project directory.
variable "app_dbt_dir" {
  type    = string
  default = ("/../app/dbt")
}

# Variable for the ELT scripts directory.
variable "app_elt_dir" {
  type    = string
  default = ("/../app/elt")
}

# Variable for the data directory.
variable "data_dir" {
  type    = string
  default = ("/../data")
}

# Variable for the database directory.
variable "database_dir" {
  type    = string
  default = ("/../database")
}

# Variable for the Airflow Docker context directory.
variable "docker_airflow_dir" {
  type    = string
  default = ("/../docker/airflow")
}

# Variable for the dbt Docker context directory.
variable "docker_dbt_dir" {
  type    = string
  default = ("/../docker/dbt")
}

# Docker network name.
variable "docker_network_name" {
  type    = string
  default = "airflow_net"
}

# Docker volume name for Airflow data persistence.
variable "airflow_volume_name" {
  type    = string
  default = "airflow_data"
}

# Docker volume name for database data persistence.
variable "database_volume_name" {
  type    = string
  default = "database_data"
}

# Docker container name for the Airflow service.
variable "airflow_container_name" {
  type    = string
  default = "airflow"
}

# Docker container name for the dbt runner service.
variable "dbt_runner_container_name" {
  type    = string
  default = "dbt-runner"
}

# Docker image tag for the Airflow image.
variable "airflow_image_tag" {
  type    = string
  default = "3.0.6-python3.12"
}

# Docker image tag for the dbt image.
variable "dbt_image_tag" {
  type    = string
  default = "1.10.11-duckdb1.1.3"
}

# Port to expose the Airflow UI (default: 8080).
variable "airflow_internal_port" {
  type    = number
  default = 8080
}

# Port to expose the Airflow UI (default: 8080).
variable "airflow_external_port" {
  type    = number
  default = 8080
}

# Port to expose the dbt service (default: 8081).
variable "dbt_internal_port" {
  type    = number
  default = 8081
}

# Port to expose the dbt service (default: 8081).
variable "dbt_external_port" {
  type    = number
  default = 8081
}

# Variable for the dags directory inside the Airflow container.
variable "airflow_container_dags_dir" {
  type    = string
  default = "/opt/airflow/dags"
}

# Variable for the ELT scripts directory inside the Airflow container.
variable "airflow_container_elt_dir" {
  type    = string
  default = "/opt/airflow/elt"
}

# Variable for the data directory inside the Airflow container.
variable "airflow_container_data_dir" {
  type    = string
  default = "/data"
}

# Variable for the Python path inside the Airflow container.
variable "airflow_pythonpath" {
  type    = string
  default = "/opt/airflow"
}

# Variable for the DAGs folder inside the Airflow container.
variable "airflow_dags_folder" {
  type    = string
  default = "/opt/airflow/dags"
}

# Whether to load example DAGs in Airflow.
variable "airflow_load_examples" {
  type    = bool
  default = false
}

# Authentication manager for Airflow's API.
variable "airflow_auth_manager" {
  type    = string
  default = "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
}

# Whether all users are admins in Airflow (only for local development).
variable "airflow_all_admins" {
  type    = bool
  default = true
}

# Scheduler DAG directory list interval in seconds.
variable "scheduler_dag_dir_list_interval" {
  type    = number
  default = 15
}

# Scheduler DAG discovery refresh interval in seconds.
variable "scheduler_dag_discovery_refresh_interval" {
  type    = number
  default = 15
}

# User ID for running Airflow processes inside the container.
variable "airflow_uid" {
  type    = number
  default = 50000
}

# Group ID for running Airflow processes inside the container.
variable "airflow_gid" {
  type    = number
  default = 0
}

# Variable for the dbt project directory inside the container.
variable "dbt_project_dir" {
  type    = string
  default = "/opt/dbt"
}

# Variable for the dbt profiles directory inside the container.
variable "dbt_profiles_dir" {
  type    = string
  default = "/opt/dbt"
}

# Variable for the database directory inside the dbt container.
variable "dbt_container_database_dir" {
  type    = string
  default = "/database"
}

# Variable for the dbt project directory inside the dbt container.
variable "dbt_container_project_dir" {
  type    = string
  default = "/opt/dbt"
}