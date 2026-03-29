"""FastAPI application entry point."""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import albums, bands, members, stats


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """App lifespan: verify DuckDB file exists on startup.

    Args:
        app: FastAPI instance.
    """
    path = os.environ.get("DUCKDB_PATH", "./data/metal.duckdb")
    if not Path(path).exists():
        raise RuntimeError(f"DuckDB file not found: {path}")
    yield


app = FastAPI(
    title="metal-stats API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(bands.router)
app.include_router(albums.router)
app.include_router(members.router)
app.include_router(stats.router)


@app.get("/health")
def health() -> dict[str, str]:
    """Health check endpoint.

    Returns:
        Status dict.
    """
    return {"status": "ok"}
