# see https://docs.astral.sh/uv/guides/integration/docker/#optimizations and https://www.joshkasuboski.com/posts/distroless-python-uv/
# Modified to work without BuildKit (no --mount syntax)

FROM ghcr.io/astral-sh/uv:debian-slim AS builder

ARG PYTHON_VERSION=3.13

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_INSTALL_DIR=/python
ENV UV_PYTHON_PREFERENCE=only-managed

RUN uv python install ${PYTHON_VERSION}

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies (without project itself)
RUN uv sync --locked --no-install-project --no-editable

# Copy source and install project
COPY src src
RUN uv sync --locked --no-editable

# Full dependency builder includes optional repository drivers for mongo + postgres.
FROM builder AS builder-full
RUN uv sync --locked --no-editable --extra mongo --extra postgres

# Use distroless for minimal image size (no shell, no package manager)
# For debugging, change to gcr.io/distroless/cc:nonroot-debug (includes busybox shell)
FROM gcr.io/distroless/cc:nonroot AS runner

COPY --from=builder /python /python

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-m", "intersect_orchestrator"]

FROM gcr.io/distroless/cc:nonroot AS full

COPY --from=builder-full /python /python

WORKDIR /app
COPY --from=builder-full /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-m", "intersect_orchestrator"]
