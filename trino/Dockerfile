FROM trinodb/trino:413

USER root

# List existing plugins to check if delta_lake is already there
RUN ls -la /usr/lib/trino/plugin/

# Create the metastore directory
RUN mkdir -p /tmp/trino-metastore && \
    chown -R trino:trino /tmp/trino-metastore

# Copy configuration files
COPY etc /etc/trino

# Enable debugging if needed
ENV JAVA_OPTS="-Xmx8G -Xms1G"

USER trino