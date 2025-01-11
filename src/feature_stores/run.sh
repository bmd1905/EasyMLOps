#!/bin/bash

# Exit on error
set -e

echo "Starting Feature Store Setup..."

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Setting up virtual environment..."
    python -m venv .venv
fi

# Activate virtual environment and install dependencies
echo "Installing dependencies..."
source .venv/bin/activate
pip install -r requirements.txt

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
max_retries=30
counter=0
while ! pg_isready -h ${POSTGRES_HOST:-localhost} -p ${POSTGRES_PORT:-5433}; do
    counter=$((counter + 1))
    if [ $counter -eq $max_retries ]; then
        echo "Failed to connect to PostgreSQL after $max_retries attempts"
        exit 1
    fi
    echo "Waiting for PostgreSQL to start... ($counter/$max_retries)"
    sleep 1
done

echo "PostgreSQL is ready!"
echo "Setting up feature store..."

# Initialize feature store
python setup.py
