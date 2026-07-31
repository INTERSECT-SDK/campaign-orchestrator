# see https://docs.astral.sh/uv/guides/integration/docker/#optimizations and https://www.joshkasuboski.com/posts/distroless-python-uv/
# Modified to work without BuildKit (no --mount syntax)

FROM ghcr.io/astral-sh/uv:debian-slim AS builder

ARG PYTHON_VERSION=3.13
# set UV_NO_DEV to empty string if you want to include the "dev" group in dependency-groups
ARG UV_NO_DEV="1"
# any extra groups to include with the image - use empty string if you want no extra groups
ARG EXTRA_BUILD_ARGS="--extra mongo --extra postgres"

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_INSTALL_DIR=/python
ENV UV_PYTHON_PREFERENCE=only-managed
ENV UV_NO_DEV=${UV_NO_DEV}

RUN uv python install ${PYTHON_VERSION}

WORKDIR /app

# Copy dependency files
# NOTE: cannot use --mount syntax because we want to keep this build compatible for systems without buildx
COPY pyproject.toml uv.lock ./

# Install dependencies (without project itself)
RUN eval "uv sync --locked --no-install-project --no-editable $EXTRA_BUILD_ARGS"

# Copy source and install project
COPY src src
RUN eval "uv sync --locked --no-editable $EXTRA_BUILD_ARGS"

# Use distroless for minimal image size (no shell, no package manager)
# For debugging, change to gcr.io/distroless/cc:nonroot-debug (includes busybox shell)
FROM gcr.io/distroless/cc:nonroot AS runner

COPY --from=builder /python /python

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-m", "intersect_orchestrator"]
