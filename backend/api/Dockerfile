FROM python:3.12 AS base-build

ARG APP_PATH=/app
ARG APP_VERSION
LABEL version=$APP_VERSION
ENV APP_VERSION=$APP_VERSION

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.4.2 \
    POETRY_HOME="/usr/local" \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR="/var/cache/pypoetry" \
    POETRY_NO_INTERACTION=1

# Install apt packages and poetry
RUN apt-get update && apt-get install --no-install-recommends -y \
    python3-pip  \
    build-essential \
    libpq-dev \
    && pip install "poetry==$POETRY_VERSION" \
    && pip install setuptools


WORKDIR $APP_PATH
COPY poetry.lock pyproject.toml $APP_PATH/

# Development build stage
FROM base-build AS dev-build

RUN poetry install --with dev

COPY backend $APP_PATH

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# Production build stage
FROM base-build AS prod-build

RUN poetry install --no-dev

COPY backend $APP_PATH

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]

# Test build stage
FROM base-build AS test-build

RUN poetry install --with dev

COPY backend $APP_PATH

CMD ["pytest", "tests/"]

