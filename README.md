[![CI](https://github.com/INTERSECT-SDK/campaign-orchestrator/actions/workflows/ci.yaml/badge.svg)](https://github.com/INTERSECT-SDK/campaign-orchestrator/actions/workflows/ci.yaml)

# Campaign orchestrator

WIP

The INTERSECT Campaign orchestrator is responsible for executing INTERSECT campaigns.

## Development setup

### Installing

Make sure you have UV installed ([Instructions](https://docs.astral.sh/uv/#installation))

- `uv venv .venv`
- `source .venv/bin/activate`
- `uv sync --all-groups --all-extras --all-packages`
- `uv run pre-commit install`
- `cp .env.example .env` - will need to do this each time `.env.example` updates from remote

### Running

- `docker compose up -d` - configures a message broker setup if you don't already have one
- `uv run python -m intersect_orchestrator`

## Running the full integration test suite locally

The CI workflow (`.github/workflows/full-test-suite.yaml`) runs integration tests
against RabbitMQ, MongoDB, PostgreSQL, the orchestrator, and the random-number-service.
You can reproduce this locally in three commands using docker-compose.

### Prerequisites

- Docker & Docker Compose
- [uv](https://docs.astral.sh/uv/#installation)
- `libpq-dev` (or equivalent) for the PostgreSQL driver:
  ```bash
  # Debian / Ubuntu
  sudo apt-get install -y libpq-dev
  # macOS
  brew install libpq
  ```

### 1. Install dependencies (one-time)

```bash
uv sync --dev --extra mongo --extra postgres
```

### 2. Start all services

```bash
docker compose up -d --build --wait
```

This builds and starts the broker, MongoDB, PostgreSQL, the orchestrator, and
the random-number-service. The `--wait` flag blocks until every service passes
its health check (equivalent to the CI `timeout 120 … until healthy` loop).

### 3. Run the integration tests

```bash
set -a && source .env.test && set +a && uv run pytest tests/integration --cov=intersect_orchestrator --cov-report=term-missing --cov-report=html
```

The `.env.test` file contains the environment variables that point the test
runner at the local docker-compose services (broker credentials, DB URIs,
orchestrator host/port, etc.). It mirrors the CI job exactly.

### Cleanup

```bash
docker compose down -v
```

### Quick reference

| Step | Command |
|------|---------|
| Install deps | `uv sync --dev --extra mongo --extra postgres` |
| Start services | `docker compose up -d --build --wait` |
| Run tests | `set -a && source .env.test && set +a && uv run pytest tests/integration --cov=intersect_orchestrator --cov-report=term-missing` |
| Tear down | `docker compose down -v` |
