# syntax=docker/dockerfile:1

# --- 1-bosqich: bog'liqliklarni alohida virtual muhitga o'rnatish ---
FROM python:3.13-slim AS builder
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY requirements.txt .
RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install -r requirements.txt

# --- 2-bosqich: yengil runtime image (build vositalarisiz) ---
FROM python:3.13-slim
LABEL org.opencontainers.image.title="xarid-audit-platform" \
      org.opencontainers.image.description="Davlat xaridlari audit-analitika platformasi: ETL, DuckDB DWH, audit qoidalari" \
      org.opencontainers.image.source="https://github.com/devSeeK1/xarid-audit-platform"

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    XARID_DATA_DIR=/app/data

# Root bo'lmagan foydalanuvchi
RUN groupadd --system app && useradd --system --gid app --home-dir /app app

WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY config ./config
COPY mock_api ./mock_api
COPY pipeline ./pipeline
COPY sql ./sql
RUN mkdir -p data logs reports && chown -R app:app /app

USER app
EXPOSE 8765

ENTRYPOINT ["python", "-m", "pipeline"]
CMD ["--help"]
