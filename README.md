# Finder Bot

Finder Bot is a Python and TypeScript application for collecting cryptocurrency market data, comparing prices across markets, and delivering potential spread signals through Telegram. This repository is a portfolio snapshot; it has not been verified against live exchange accounts or production infrastructure.

## Components

- `backend/infrastructure/services/collectors/` contains token and price collectors, spread analysis, and delivery services.
- `backend/infrastructure/database/` defines PostgreSQL models and repositories; Alembic migrations are under `backend/infrastructure/migrations/`.
- `backend/api/` exposes a FastAPI service. The API uses PostgreSQL and Redis.
- `backend/bot/` contains the Telegram bot.
- `backend/webapp/` is a Next.js dashboard backed by PostgreSQL and Redis.
- `backend/tests/` contains existing tests. Some historical integration scripts require a populated database and must not be run against a valuable dataset.
- `docs/` contains historical implementation notes. Their operational details and performance figures have not been independently verified for this snapshot.

The data path is broadly: exchange collectors → Redis and PostgreSQL → spread analysis → Telegram and dashboard. Exchange APIs, available symbols, schemas and rate limits can change.

## Local setup

Python 3.12, Poetry, PostgreSQL, Redis, Node.js 20.9+ (20.x) or 22+ and npm are required for the respective components. Start with isolated, disposable services and your own credentials. Production deployment files, proxy lists, and environment files from the original installation are intentionally excluded.

```sh
poetry install --no-root
cp .env.example backend/.env.prod
# Edit backend/.env.prod for a disposable local environment.
cd backend
PYTHONPATH=. poetry run alembic upgrade head
PYTHONPATH=. poetry run uvicorn api.main:app --host 127.0.0.1 --port 8000
```

Settings are loaded from `.env.prod` in the current working directory or from environment variables; for the command above, place the edited file in `backend/.env.prod`, or export the settings in your shell. The API needs reachable PostgreSQL and Redis services at startup. Telegram bot tokens and topic IDs are necessary for bot and notification functionality. Never use real credentials in a public repository. Proxy configuration is optional and is loaded separately if present.

The Python images use the repository root as their build context:

```sh
docker build -f backend/api/Dockerfile --target prod-build -t finder-api .
docker build -f backend/bot/Dockerfile --target prod-build -t finder-bot .
```

The bot image runs migrations at startup; provide an isolated database and bot configuration before running it.

To work on the dashboard:

```sh
cd backend/webapp
npm ci
npm run lint
npm run typecheck
npm run dev
```

The dashboard reads database connection settings from `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, and `DB_NAME`; Redis and other configuration should be reviewed in the relevant `lib/` files before launch. The snapshot does not include a validated one-command deployment or seeded demo data.

## Tests

The GitHub Actions workflow runs migrations, backend tests, and dashboard checks against disposable PostgreSQL and Redis services.

```sh
cd backend
PYTHONPATH=. poetry run pytest tests/unit
```

Some tests and scripts outside `tests/unit` are historical or integration-oriented and may need additional services and data. Review them before execution; migration and cleanup scripts can modify a database.

The integration tests are opt-in. After migrating a disposable database and configuring both services via environment variables, run `FINDER_INTEGRATION=1 PYTHONPATH=. poetry run pytest tests/integration` from `backend/`. They check the API, its resource lifecycle, and whether the migrated schema matches the model metadata.

To also exercise the dashboard, start it against an empty disposable database and Redis and set `FINDER_WEBAPP_URL` to its local URL when running the integration tests. The database test creates and removes a test token; use isolated services only.

## Limitations

This is an archived snapshot, not a trading system recommendation. Live exchange access, Telegram delivery, data migrations, end-to-end operation, and historical performance claims are not verified in this repository. Do not run collectors or notification services against live accounts without reviewing their configuration and behavior.
