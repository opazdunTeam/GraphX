# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.13.9 AS uv

FROM python:3.15-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1

RUN useradd --create-home --uid 10001 graphx \
    && install -d -o graphx -g graphx /var/lib/graphx/worker-replay
WORKDIR /app
COPY --from=uv /uv /uvx /bin/
COPY workers/pyproject.toml workers/uv.lock workers/README.md ./
RUN uv sync --locked --no-dev --no-install-project
COPY workers/src ./src
COPY contracts/schemas ./contracts/schemas
RUN uv sync --locked --no-dev --no-editable

ENV CONTRACT_SCHEMA_ROOT=/app/contracts/schemas
ENV TERMINAL_REPLAY_ROOT=/var/lib/graphx/worker-replay
ENV PATH=/app/.venv/bin:$PATH

USER graphx
ENTRYPOINT ["graphx-source-worker"]
