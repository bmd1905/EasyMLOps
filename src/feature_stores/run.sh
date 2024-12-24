#!/bin/bash

# Exit on error
set -e

echo "Starting Feature Store Setup..."

# Create and activate virtual environment
echo "Setting up virtual environment..."
python -m venv .venv
source .venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Setup feature store
echo "Setting up feature store..."
python setup.py
