FROM python:3.11-slim AS base

WORKDIR /app

COPY pyproject.toml ./
RUN pip install --no-cache-dir .

FROM base AS api
COPY api/ ./api/
COPY db/ ./db/
COPY data/ ./data/
EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM base AS scraper
COPY scraper/ ./scraper/
COPY data/ ./data/
CMD ["python", "-m", "scraper.pipeline"]
