FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Remove the feature_store.yaml file
RUN rm feature_store.yaml

# Rename the feature_store.docker.yaml file to be used as the main config
RUN mv feature_store.docker.yaml feature_store.yaml
