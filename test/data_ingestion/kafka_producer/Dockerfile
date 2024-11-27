FROM python:3.8-slim

WORKDIR /app

# Copy app handler code
COPY ./kafka_producer/produce_json.py /app/produce.py

# Copy the schema folder
COPY ./kafka_producer/avro_schemas /app/avro_schemas

# Install dependencies
RUN pip3 install kafka-python==2.0.2
# RUN pip3 install avro==1.11.1
RUN pip3 install pandas==1.3.0
RUN pip3 install pyarrow==8.0.0
RUN pip3 install python-schema-registry-client==2.4.1
RUN pip3 install pymongo==3.8.0

CMD [ "python", "-u", "produce.py", "--mode", "setup", "--bootstrap_servers", "broker:29092"]
