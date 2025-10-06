# Use the official Apache Airflow image as the base image.
FROM apache/airflow:3.0.6-python3.12

# Set environment variables:
# - PIP_NO_CACHE_DIR=1: avoid caching pip packages.
# - PYTHONDONTWRITEBYTECODE=1: prevent creation of .pyc files.
# - PYTHONUNBUFFERED=1: force unbuffered output for logs.
ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Switch to airflow user
USER airflow

# Copy Python dependencies file into the image.
COPY requirements.txt requirements.txt

# Install Python dependencies and remove the temporary requirements file.
RUN pip install -r requirements.txt \
    && rm requirements.txt

