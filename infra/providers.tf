# Providers used in this Terraform configuration.
terraform {
    required_version = "~> 1.13.0"
    
    required_providers {
        # Provider Docker to manage Docker resources locally.
        docker = {
            source  = "kreuzwerker/docker"
            version = "~> 3.6.2"
        }
    }
}
# Setup of the Docker provider.
provider "docker" {}