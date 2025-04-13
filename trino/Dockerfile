FROM trinodb/trino:413

USER root

# List existing plugins to check if delta_lake is already there
RUN ls -la /usr/lib/trino/plugin/

# Copy configuration files
COPY etc /etc/trino

USER trino