# Criação de uma rede Docker isolada para o Airflow
resource "docker_network" "airflow_net" {
  name = "airflow_net"
}

# Criação do container Docker que executará o Airflow
resource "docker_container" "airflow" {
  name  = "airflow"
  image = var.airflow_image
  restart = "always"

  # Mapeamento de portas: expõe a UI do Airflow na porta definida em variables.tf
  ports {
    internal = 8080
    external = var.airflow_port
  }

  /* Variáveis de ambiente do Airflow:
   - Desabilita DAGs de exemplo
   - Configura o SimpleAuthManager para que todos os usuários sejam admin 
  */
  env = [
    "AIRFLOW__CORE__LOAD_EXAMPLES=False",
    "AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
    "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True",
  ]

  # Conecta o container à rede Docker criada anteriormente
  networks_advanced {
    name = docker_network.airflow_net.name
  }

  # Comando de inicialização do Airflow em modo standalone
  command = ["airflow", "standalone"]
}