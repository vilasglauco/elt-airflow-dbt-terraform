# Providers utilizados no projeto.
terraform {
    required_providers {
        # Provider Docker para gerenciar containers Docker localmente.
        docker = {
            source  = "kreuzwerker/docker"
            version = "~> 3.6.2"
        }
    }
}

# Configuração do provider Docker
provider "docker" {}