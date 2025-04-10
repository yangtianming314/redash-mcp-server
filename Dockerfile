FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src

RUN pip install uv && \
    uv pip install -e .

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s \
  CMD curl -f http://localhost:8000/healthz || exit 1

CMD ["uv", "redash_mcp.main:app", "--host", "0.0.0.0", "--port", "8000"]
