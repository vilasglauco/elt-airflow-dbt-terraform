# Use a lightweight Python 3.12 base image.
FROM python:3.12-slim

# Set environment variables:
# - PIP_NO_CACHE_DIR=1: avoid caching pip packages.
# - PYTHONDONTWRITEBYTECODE=1: prevent creation of .pyc files.
# - PYTHONUNBUFFERED=1: force unbuffered output for logs.
# - DBT_PROFILES_DIR=/opt/dbt: dbt will look for profiles.yml here.
ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/opt/dbt

# Install system dependencies and DuckDB CLI for ARM64.
# - git, ca-certificates, curl, unzip are required tools.
# - Download DuckDB CLI binary and place it in /usr/local/bin
# - Clean up unnecessary files to keep the image small.
RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        ca-certificates \
        curl \
        unzip \
        passwd \
    && curl -L "https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-aarch64.zip" -o /tmp/duckdb.zip \
    && unzip /tmp/duckdb.zip -d /usr/local/bin \
    && rm /tmp/duckdb.zip \
    && chmod +x /usr/local/bin/duckdb \
    && rm -rf /var/lib/apt/lists/*

# Copy Python dependencies file into the image.
COPY requirements.txt requirements.txt

# Install Python dependencies and remove the temporary requirements file.
RUN pip install -r requirements.txt \
    && rm requirements.txt

# Create dbt user with high UID/GID
RUN groupadd -g 50001 dbt \
    && useradd -m -u 50001 -g 50001 -d /opt/dbt dbt

# Switch to dbt user
USER dbt