# Imagem Docker que será utilizada para executar o Airflow.
variable "airflow_image" {
  type    = string
  default = "apache/airflow:3.0.6-python3.12"
}

# Porta onde a interface web do Airflow ficará disponível.
variable "airflow_port" {
  type    = number
  default = 8080
}