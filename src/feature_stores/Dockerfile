FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Rename the Docker config file to be used as the main config
RUN mv feature_store.docker.yaml feature_store.yaml

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8001"]
