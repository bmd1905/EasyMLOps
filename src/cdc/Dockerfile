FROM python:3.9-slim

# Install curl
RUN apt-get update && apt-get install -y curl

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY . /app
WORKDIR /app

CMD ["sh", "-c", "bash start.sh"]
