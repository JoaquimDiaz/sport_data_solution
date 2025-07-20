#!/bin/bash

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
  echo "Error: Docker is not running. Please start Docker first."
  exit 1
fi

# Check if container already exists
if docker ps -a --format '{{.Names}}' | grep -q '^postgres-db$'; then
  echo "Container 'postgres-db' already exists."
  echo "If you want to reset it, run:"
  echo "    docker stop postgres-db && docker rm postgres-db"
  echo "Then rerun this script."
  exit 1
fi

# Load .env file from parent directory
set -a
source "$(dirname "$0")/../.env"
set +a

# Define file to store psql version
VERSION_FILE="$(dirname "$0")/psql-version.txt"

# Pull latest Postgres image
docker pull postgres:${POSTGRES_VERSION:-latest}

echo "Postgres image pulled successfully."

# Write psql version to file
docker run --rm postgres psql --version | tee "$VERSION_FILE"

echo "Postgres version written to: $VERSION_FILE"

# Create volume for Postgres data
docker volume create postgres-data

# Start Postgres container
docker run --name postgres-db \
    -e POSTGRES_USER="${POSTGRES_USER}" \
    -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
    -e POSTGRES_DB="${POSTGRES_DB}" \
    -v postgres-data:/var/lib/postgresql/data \
    -p 127.0.0.1:"${POSTGRES_PORT}":5432 \
    -d postgres:${POSTGRES_VERSION:-latest}

echo "Postgres container 'postgres-db' is running."
echo "Postgres is available at: 127.0.0.1:${POSTGRES_PORT}"
