# Montando e expondo a URL do Airflow via output.
output "airflow_url" {
  value = "http://localhost:${var.airflow_port}"
}