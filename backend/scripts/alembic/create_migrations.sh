# shellcheck disable=SC2162
read -p "Enter name of migration: " message
docker exec bot alembic revision --autogenerate -m "$message"
sudo chmod +777 backend/infrastructure/migrations/versions/*

# sudo sh scripts/alembic/create_migrations.sh