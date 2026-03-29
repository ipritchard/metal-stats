"""Async three-phase scraping pipeline for metal band data."""

import argparse
import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from scraper import checkpoint, geo_client, lastfm_client, ma_client

log = logging.getLogger(__name__)

DATA_DIR = Path("data")
CHECKPOINTS = DATA_DIR / "checkpoints"
IDS_PATH = CHECKPOINTS / "ids.json"
UNRESOLVED_PATH = CHECKPOINTS / "unresolved.json"
NDJSON_PATH = CHECKPOINTS / "raw_bands.ndjson"
DUCKDB_PATH = DATA_DIR / "metal.duckdb"
CSV_PATH = DATA_DIR / "bandlist.csv"

MAX_WORKERS = 4


def phase0_dedup(csv_path: Optional[Path] = None) -> pd.DataFrame:
    """Phase 0: Deduplicate bands from the album-level CSV.

    Args:
        csv_path: Path to bandlist.csv. Defaults to DATA_DIR/bandlist.csv.

    Returns:
        DataFrame with one row per unique band, columns:
        Band, Band Origin, album_count, best_score.
    """
    csv_path = csv_path or CSV_PATH
    df = pd.read_csv(csv_path)

    bands = (
        df.groupby("Band", as_index=False)
        .agg(
            origin=("Band Origin", "first"),
            album_count=("Album", "count"),
            best_score=("Score", "max"),
        )
        .rename(columns={"origin": "Band Origin"})
        .sort_values("best_score", ascending=False)
        .reset_index(drop=True)
    )
    log.info("Phase 0: %d unique bands from %d album rows", len(bands), len(df))
    return bands


async def phase1_resolve(bands: pd.DataFrame, checkpoint_path: Optional[Path] = None) -> None:
    """Phase 1: Resolve band names to Metal Archives IDs.

    Args:
        bands: Deduplicated band DataFrame from phase0.
        checkpoint_path: Path to ids.json checkpoint.
    """
    checkpoint_path = checkpoint_path or IDS_PATH
    ids = checkpoint.read_json(checkpoint_path)
    unresolved: dict[str, list[dict[str, str]]] = checkpoint.read_json(UNRESOLVED_PATH)
    names = [n for n in bands["Band"].tolist() if n not in ids]
    log.info("Phase 1: %d bands to resolve (%d cached)", len(names), len(ids))

    for i, name in enumerate(names):
        band_id, candidates = ma_client.search_band(name)
        if band_id:
            ids[name] = band_id
            checkpoint.write_json(checkpoint_path, ids)
            # Remove from unresolved if previously ambiguous
            if name in unresolved:
                del unresolved[name]
                checkpoint.write_json(UNRESOLVED_PATH, unresolved)
            log.info("Resolved (%d/%d): %s -> %s", i + 1, len(names), name, band_id)
        else:
            if candidates:
                unresolved[name] = candidates
                checkpoint.write_json(UNRESOLVED_PATH, unresolved)
            log.warning("No MA match for '%s'", name)
        if i < len(names) - 1:
            time.sleep(3)

    log.info("Phase 1 complete: %d resolved, %d unresolved with candidates",
             len(ids), len(unresolved))


async def phase2_enrich(checkpoint_path: Optional[Path] = None) -> None:
    """Phase 2: Fetch full profiles from MA + Last.fm for each resolved band.

    Args:
        checkpoint_path: Path to raw_bands.ndjson checkpoint.
    """
    checkpoint_path = checkpoint_path or NDJSON_PATH
    ids = checkpoint.read_json(IDS_PATH)
    done = checkpoint.ndjson_ids(checkpoint_path)
    pending = {name: bid for name, bid in ids.items() if bid not in done}
    log.info("Phase 2: %d bands to enrich (%d cached)", len(pending), len(done))

    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:

        async def enrich_one(name: str, band_id: str) -> None:
            profile = await loop.run_in_executor(pool, ma_client.fetch_profile, band_id)
            if not profile:
                return
            lfm = await loop.run_in_executor(pool, lastfm_client.fetch_stats, name)
            if lfm:
                profile["lastfm"] = lfm
            checkpoint.append_ndjson(checkpoint_path, profile)
            log.info("Enriched: %s", name)

        tasks = [enrich_one(name, bid) for name, bid in pending.items()]
        await asyncio.gather(*tasks)

    log.info("Phase 2 complete")


def phase3_geocode(
    ndjson_path: Optional[Path] = None,
    duckdb_path: Optional[Path] = None,
) -> None:
    """Phase 3: Geocode band locations with Nominatim + DuckDB cache.

    Args:
        ndjson_path: Path to raw_bands.ndjson.
        duckdb_path: Path to DuckDB database.
    """
    ndjson_path = ndjson_path or NDJSON_PATH
    duckdb_path = duckdb_path or DUCKDB_PATH
    records = checkpoint.read_ndjson(ndjson_path)

    con = duckdb.connect(str(duckdb_path))
    con.execute("""
        CREATE TABLE IF NOT EXISTS geo_cache (
            query  VARCHAR PRIMARY KEY,
            lat    DOUBLE,
            lon    DOUBLE
        )
    """)

    for rec in records:
        location = rec.get("location")
        country = rec.get("country")
        lat, lon, precision = geo_client.geocode(location, country, con)
        rec["lat"] = lat
        rec["lon"] = lon
        rec["geo_precision"] = precision

    # Overwrite NDJSON with geocoded data
    ndjson_path.write_text("")
    for rec in records:
        checkpoint.append_ndjson(ndjson_path, rec)

    con.close()
    log.info("Phase 3 complete: geocoded %d bands", len(records))


async def main(phase: Optional[int] = None) -> None:
    """Run the full pipeline or a single phase.

    Args:
        phase: If set, run only that phase (0-3). Otherwise run all.
    """
    if phase is None or phase == 0:
        bands = phase0_dedup()
        print(f"Phase 0: {len(bands)} unique bands")
        if phase == 0:
            print(bands[["Band", "Band Origin", "album_count", "best_score"]].head(20))
            return

    if phase is None or phase == 1:
        bands = phase0_dedup()
        await phase1_resolve(bands)

    if phase is None or phase == 2:
        await phase2_enrich()

    if phase is None or phase == 3:
        phase3_geocode()

    if phase is None:
        print("Pipeline complete.")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="Metal stats scraping pipeline")
    parser.add_argument("--phase", type=int, choices=[0, 1, 2, 3], help="Run single phase")
    args = parser.parse_args()

    asyncio.run(main(phase=args.phase))
