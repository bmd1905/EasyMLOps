FROM rayproject/ray:2.40.0-py39

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*


# Install specific versions of packages known to work together
RUN pip install --no-cache-dir \
    'pyOpenSSL==23.2.0' \
    'cryptography==41.0.7' \
    'urllib3<2.0.0' \
    'tensorboardX==2.6.2'

# Install ray-serve
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER ray
