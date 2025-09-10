# Mounting the variables to expose the Airflow URL in the terminal.
output "airflow_url" {
  value = "http://localhost:${var.airflow_port}"
}