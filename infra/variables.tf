# Docker images used to run Airflow.
variable "airflow_image" {
  type    = string
  default = "apache/airflow:3.0.6-python3.12"
}

# Port to expose the Airflow UI (default: 8080).
variable "airflow_port" {
  type    = number
  default = 8080
}

# Port to expose the dbt service (default: 8081).
variable "dbt_port" {
  type    = number
  default = 8081
}