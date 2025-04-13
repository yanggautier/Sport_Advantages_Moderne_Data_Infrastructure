#!/bin/sh

# Remplacer les variables dans postgresql.properties
envsubst < /etc/trino/templates/postgresql.properties.template > /etc/trino/catalog/postgresql.properties

# Exécuter le entrypoint original de Trino
exec /usr/lib/trino/bin/run-trino