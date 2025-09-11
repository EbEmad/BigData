FROM docker.io/bitnami/spark:3

# Install system dependencies for psycopg2-binary build (and general utilities)
USER root
RUN install_packages build-essential gcc python3-dev libpq-dev

# Copy requirements and install Python packages in the image
COPY requirements.txt /tmp/requirements.txt
RUN pip install  -r /tmp/requirements.txt

# Set back to the default user used by the Bitnami Spark image
USER 1001

# Default workdir contains our mounted app in docker-compose
WORKDIR /opt/bitnami/spark


