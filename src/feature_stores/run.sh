#!/bin/bash

# Exit on error
set -e

echo "Starting Feature Store Setup..."

# Create and activate virtual environment
echo "Setting up virtual environment..."
# python -m venv .venv
source .venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
# pip install --upgrade pip
# pip install -r requirements.txt

# Setup feature store
echo "Setting up feature store..."
python setup.py

# Materialize initial features
echo "Materializing features..."
python materialize_features.py

# Start FastAPI service
echo "Starting FastAPI service..."
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
