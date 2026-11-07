#!/bin/bash

wait_for_service() {
host="$1"
port="$2"

echo "Waiting for $host:$port..."

while ! nc -z "$host" "$port"; do
sleep 1
done

echo "$host:$port is available."
}

wait_for_service "$POSTGRES__DB_HOST" "$POSTGRES__DB_PORT"

echo "Running Alembic migrations..."
alembic upgrade head

echo "Starting the application..."
exec python3 bot/main.py