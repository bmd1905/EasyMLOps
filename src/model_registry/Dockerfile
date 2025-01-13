FROM python:3.10-slim

RUN apt-get update && apt-get install -y curl
RUN pip install mlflow==2.19.0 psycopg2-binary==2.9.10 boto3==1.35.74 cryptography==41.0.7 pymysql==1.1.1

EXPOSE 5000
