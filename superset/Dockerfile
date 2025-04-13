FROM apache/superset:latest

USER root

# Install additional dependencies including psql client
RUN apt-get update && \
    apt-get install -y build-essential libssl-dev libffi-dev postgresql-client && \
    pip install --no-cache-dir \
    psycopg2-binary \
    sqlalchemy-trino \
    pyhive==0.7.0 \
    pyspark==3.2.3 \
    delta-spark==1.2.0 \
    thrift==0.16.0 \
    thrift-sasl==0.4.3 \
    pillow \
    pure-sasl==0.6.2 \
    s3fs

# Create directories
RUN mkdir -p /app/docker-init.d /app/superset_home

# Generate a random secret key and store it
RUN SECRET_KEY=$(openssl rand -base64 42) && \
    echo "SECRET_KEY = '${SECRET_KEY}'" > /app/superset/custom_secret_key.py 

# Copy configuration and scripts
COPY ./superset_config.py /app/superset/
COPY ./docker/docker-entrypoint.sh /app/docker-entrypoint.sh
COPY ./docker-init.d/ /app/docker-init.d/

# Fix permissions
RUN chmod +x /app/docker-entrypoint.sh && \
    chmod +x /app/docker-init.d/* && \
    chown -R superset:superset /app/superset /app/docker-init.d /app/superset_home

USER superset

# Set environment variables
ENV PYTHONPATH=/app
ENV FLASK_APP=superset.app:create_app()
ENV FLASK_ENV=production
ENV SUPERSET_HOME=/app/superset_home

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["gunicorn", "--bind", "0.0.0.0:8088", "--workers", "2", "--timeout", "120", "--limit-request-line", "0", "--limit-request-field_size", "0", "superset.app:create_app()"]