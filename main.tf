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
    "AIRFLOW__CORE__LOAD_EXAMPLES=False",
    "AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
    "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True",
  ]

  # Connects the container to the previously created Docker network
  networks_advanced {
    name = docker_network.airflow_net.name
  }

  # Init command to start Airflow in standalone mode
  command = ["airflow", "standalone"]
}