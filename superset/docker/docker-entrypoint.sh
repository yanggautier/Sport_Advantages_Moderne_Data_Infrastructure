#!/bin/bash
set -eo pipefail

# Ensure SECRET_KEY is properly configured
if [ -z "$SUPERSET_SECRET_KEY" ]; then
  echo "No SUPERSET_SECRET_KEY provided, using the one generated during build"
elif [ ${#SUPERSET_SECRET_KEY} -lt 32 ]; then
  echo "WARNING: SUPERSET_SECRET_KEY is too short. Generating a new one..."
  SECRET_KEY=$(openssl rand -base64 42)
  echo "SECRET_KEY = '${SECRET_KEY}'" > /app/superset/custom_secret_key.py
fi

# Install additional dependencies
pip install --no-cache psycopg2-binary sqlalchemy-trino

# Initialize the database
echo "Upgrading database..."
superset db upgrade

# Create admin user if it doesn't exist already
if ! superset fab list-users | grep -q 'admin'; then
  echo "Creating admin user..."
  superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.com \
    --password admin
else
  echo "Admin user already exists"
fi

# Initialize Superset if needed
echo "Initializing Superset..."
superset init

# Initialisation terminée
echo "Initialization complete!"

# Start Superset
echo "Starting Superset..."
exec "$@"