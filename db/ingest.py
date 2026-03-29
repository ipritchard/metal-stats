"""Ingest scraped NDJSON + CSV rankings into DuckDB."""

import logging
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from scraper.checkpoint import read_ndjson

log = logging.getLogger(__name__)

DATA_DIR = Path("data")
NDJSON_PATH = DATA_DIR / "checkpoints" / "raw_bands.ndjson"
CSV_PATH = DATA_DIR / "bandlist.csv"
DUCKDB_PATH = DATA_DIR / "metal.duckdb"
SCHEMA_DIR = Path("db")

# Auto-increment counter for members table (DuckDB has no SERIAL)
_member_id_seq = 0


def _init_duckdb(con: duckdb.DuckDBPyConnection) -> None:
    """Run DuckDB DDL from duckdb_schema.sql.

    Args:
        con: DuckDB connection.
    """
    ddl = (SCHEMA_DIR / "duckdb_schema.sql").read_text()
    con.execute(ddl)


def _load_rankings(csv_path: Path) -> pd.DataFrame:
    """Parse bandlist.csv into a rankings DataFrame.

    Args:
        csv_path: Path to bandlist.csv.

    Returns:
        DataFrame with band, album, source, rank, csv_rank, score columns.
    """
    df = pd.read_csv(csv_path)
    # The first unnamed column is the original row index / composite rank
    rank_col = df.columns[0]
    df = df.rename(columns={rank_col: "csv_rank"})

    # Identify ranking source columns (everything after Score)
    score_idx = list(df.columns).index("Score")
    source_cols = [c for c in df.columns[score_idx + 1:] if c != "csv_rank"]

    rows = []
    for _, row in df.iterrows():
        for src in source_cols:
            val = row[src]
            if pd.notna(val):
                rows.append({
                    "band": row["Band"],
                    "album": row["Album"],
                    "source": src.strip(),
                    "rank": int(val) if str(val).strip().isdigit() else None,
                    "csv_rank": int(row["csv_rank"]) if pd.notna(row["csv_rank"]) else None,
                    "score": float(row["Score"]) if pd.notna(row["Score"]) else None,
                })
    return pd.DataFrame(rows)


def _ingest_band_duckdb(con: duckdb.DuckDBPyConnection, rec: dict) -> None:
    """Insert a single band record into DuckDB.

    Args:
        con: DuckDB connection.
        rec: Band record dict from NDJSON.
    """
    global _member_id_seq

    con.execute(
        """INSERT OR REPLACE INTO bands
           (id, name, country, location_raw, lat, lon, geo_precision,
            formed_year, status, genres)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            rec["id"], rec["name"], rec.get("country"), rec.get("location"),
            rec.get("lat"), rec.get("lon"), rec.get("geo_precision"),
            rec.get("formed_year"), rec.get("status"), rec.get("genres", []),
        ],
    )

    for album in rec.get("albums", []):
        con.execute(
            "INSERT OR REPLACE INTO albums (id, band_id, title, year, type) VALUES (?,?,?,?,?)",
            [album["id"], rec["id"], album["title"], album.get("year"), album.get("type")],
        )

    for m in rec.get("members_current", []):
        _member_id_seq += 1
        con.execute(
            "INSERT OR REPLACE INTO members (id, band_id, name, role, is_current) "
            "VALUES (?, ?, ?, ?, TRUE)",
            [_member_id_seq, rec["id"], m["name"], m.get("role")],
        )

    for m in rec.get("members_past", []):
        _member_id_seq += 1
        con.execute(
            "INSERT OR REPLACE INTO members (id, band_id, name, role, is_current) "
            "VALUES (?, ?, ?, ?, FALSE)",
            [_member_id_seq, rec["id"], m["name"], m.get("role")],
        )

    lfm = rec.get("lastfm")
    if lfm:
        con.execute(
            """INSERT OR REPLACE INTO lastfm_stats
               (band_id, listeners, play_count, tags, similar)
               VALUES (?, ?, ?, ?, ?)""",
            [rec["id"], lfm.get("listeners"), lfm.get("play_count"),
             lfm.get("tags", []), lfm.get("similar", [])],
        )


def ingest(
    ndjson_path: Optional[Path] = None,
    csv_path: Optional[Path] = None,
    duckdb_path: Optional[Path] = None,
) -> None:
    """Run full ingest: NDJSON + CSV rankings into DuckDB.

    Args:
        ndjson_path: Path to raw_bands.ndjson.
        csv_path: Path to bandlist.csv.
        duckdb_path: Path to DuckDB file.
    """
    ndjson_path = ndjson_path or NDJSON_PATH
    csv_path = csv_path or CSV_PATH
    duckdb_path = duckdb_path or DUCKDB_PATH

    records = read_ndjson(ndjson_path)
    log.info("Ingesting %d band records", len(records))

    con = duckdb.connect(str(duckdb_path))
    _init_duckdb(con)
    for rec in records:
        _ingest_band_duckdb(con, rec)
    con.close()
    log.info("DuckDB ingest complete")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    ingest()
