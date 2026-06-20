FROM python:3.13-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml uv.lock /app/

FROM base AS builder

RUN uv sync --locked --no-editable --no-install-project

COPY src /app/src

RUN uv sync --locked --no-editable

FROM base AS builder-full

RUN uv sync --locked --no-editable --extra mongo --extra postgres --no-install-project

COPY src /app/src

RUN uv sync --locked --no-editable --extra mongo --extra postgres

FROM python:3.13-slim AS runner

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-m", "intersect_orchestrator"]

FROM python:3.13-slim AS full

WORKDIR /app
COPY --from=builder-full /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-m", "intersect_orchestrator"]
