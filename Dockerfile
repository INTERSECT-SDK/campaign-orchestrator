# see https://docs.astral.sh/uv/guides/integration/docker/#optimizations and https://www.joshkasuboski.com/posts/distroless-python-uv/

FROM ghcr.io/astral-sh/uv:debian-slim AS builder

ARG PYTHON_VERSION=3.12

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_INSTALL_DIR=/python
ENV UV_PYTHON_PREFERENCE=only-managed

RUN uv python install ${PYTHON_VERSION}

WORKDIR /app

# Install (required) dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --no-dev --frozen --no-install-project --no-editable

# Sync the project
COPY intersect_orchestrator intersect_orchestrator
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --no-dev --frozen --no-editable

FROM gcr.io/distroless/cc:nonroot AS runner

COPY --from=builder --chown=python:python /python /python

WORKDIR /app
COPY --from=builder --chown=app:app /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

# override CMD at container runtime for tests
# note that you generally will NOT want to set UVICORN_WORKERS if running in a Kubernetes cluster, let Kubernetes handle that for you
CMD ["python", "-m", "intersect_orchestrator"]

