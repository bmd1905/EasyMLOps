FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

ENV RAY_ADDRESS="ray://ray-head:10001"

EXPOSE 8000

# Run the FastAPI application
CMD ["serve", "run", "main:entrypoint"]
