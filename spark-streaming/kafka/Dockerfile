# Use the Bitnami Kafka image as the base
FROM bitnami/kafka:latest

# Switch to root user for installations
USER root


RUN apt update && apt install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install kafka-python --break-system-packages && \
    pip3 install pandas --break-system-packages



# Switch back to the kafka user for security
USER 1001