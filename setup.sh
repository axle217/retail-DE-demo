#!/usr/bin/env bash

set -e

# Secret values (In production, these should be stored securely and not hardcoded)
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
AIRFLOW__API_AUTH__JWT_SECRET=airflow
AIRFLOW__API_AUTH__JWT_ISSUER=airflow
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Input to .env file for Docker Compose
echo "AIRFLOW_UID=$(id -u)" > .env
FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
echo "FERNET_KEY=$FERNET_KEY" >> .env
echo "_AIRFLOW_WWW_USER_USERNAME=$_AIRFLOW_WWW_USER_USERNAME" >> .env
echo "_AIRFLOW_WWW_USER_PASSWORD=$_AIRFLOW_WWW_USER_PASSWORD" >> .env

echo "AIRFLOW__API_AUTH__JWT_SECRET=$AIRFLOW__API_AUTH__JWT_SECRET" >> .env
echo "AIRFLOW__API_AUTH__JWT_ISSUER=$AIRFLOW__API_AUTH__JWT_ISSUER" >> .env

echo "POSTGRES_USER=$POSTGRES_USER" >> .env
echo "POSTGRES_PASSWORD=$POSTGRES_PASSWORD" >> .env
echo "POSTGRES_DB=$POSTGRES_DB" >> .env

# Cleanup to reset the environment (Delete schema and data since they are persisted in volumes)
docker compose down -v --remove-orphans

# Delete ALL volumes (including those not related to this project):
# docker volume rm $(docker volume ls -q)

docker volume ls

# docker compose up airflow-init
# docker compose up --build -d
docker compose up -d
# docker compose up -d --force-recreate
# docker compose ps
# docker compose logs airflow-init
# docker compose logs airflow-api-server
# docker compose logs airflow-scheduler


# Wait for PostgreSQL to be ready before executing the SQL script to create the star schema

CONTAINER="retail-data-engineering-postgres-1"

echo "Waiting for PostgreSQL..."

until docker exec "$CONTAINER" pg_isready -U $POSTGRES_USER >/dev/null 2>&1; do
    sleep 2
done

echo "PostgreSQL is ready."

docker exec -i "$CONTAINER" \
    psql -U $POSTGRES_USER -d $POSTGRES_DB \
    < postgres/create_star_schema.sql


docker exec -i "$CONTAINER" \
    psql -U $POSTGRES_USER -d $POSTGRES_DB \
    < postgres/create_oltp.sql

# docker exec -it "$CONTAINER" psql -U $POSTGRES_USER -d $POSTGRES_DB

# sudo chown -R $(id -u):$(id -g) dags logs plugins config