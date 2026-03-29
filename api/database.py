"""Database connection helper for DuckDB (read-only)."""

import os

import duckdb


def get_duckdb() -> duckdb.DuckDBPyConnection:
    """Open a read-only DuckDB connection.

    Returns:
        Read-only DuckDB connection.
    """
    path = os.environ.get("DUCKDB_PATH", "./data/metal.duckdb")
    return duckdb.connect(path, read_only=True)
