FROM python:3.13-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.8.21 /uv /uvx /bin/

WORKDIR /build

# Copy dependency files from project root
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --no-dev --frozen

FROM python:3.13-slim AS runner

WORKDIR /app

# Copy the service file
COPY services/random_number_service/random_number_generator.py .

# Copy venv from builder
COPY --from=builder /build/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "random_number_generator.py"]
