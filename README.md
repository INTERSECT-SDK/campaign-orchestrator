# Campaign orchestrator

WIP

The INTERSECT Campaign orchestrator is responsible for executing INTERSECT campaigns.

## Development setup

### Installing

Make sure you have UV installed ([Instructions](https://docs.astral.sh/uv/#installation))

- `uv sync`
- `uv run pre-commit install`
- `cp .env.example .env` - will need to do this each time `.env.example` updates from remote

### Running

- `uv run python -m intersect_orchestrator`
