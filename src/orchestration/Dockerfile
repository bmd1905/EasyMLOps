FROM apache/airflow:2.10.3-python3.9

USER root

# Install system dependencies required for Ray
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        libgl1-mesa-glx \
        libglib2.0-0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# First install setuptools to ensure pkg_resources is available
RUN pip install --no-cache-dir setuptools==69.0.3

# Then install Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy the include directory to make it available as a package
COPY include /opt/airflow/include/
