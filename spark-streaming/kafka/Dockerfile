# Use the Bitnami Kafka image that supports ZooKeeper
FROM bitnami/kafka:3.6.1

# Switch to root user for installations
USER root

# Install Python and required packages
RUN apt update && apt install -y python3 python3-pip python3-venv && \
    python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install kafka-python pandas

ENV PATH="/opt/venv/bin:$PATH"


# Switch back to non-root Kafka user
USER 1001
