# metal-stats — Claude Code Project Brief

## Project Overview

A full-stack metal band data visualization and exploration site. Users can search
bands, explore membership lineups over time, browse discographies, view genre
breakdowns, lyric word clouds, and geospatial heatmaps of band origins.

---

## Developer Profile

- Geospatial Python developer; prefers brevity and Google-style docstrings
- Comfortable with GCP, Docker; less experienced with Cloud Run specifically
- Type hints required on all function arguments and return values
- Async/threaded scraping preferred over single-threaded

---

## Architecture

### Data Flow

```
bandlist.csv (album-level rows with rank scores; deduplicate bands first)
    → scraper/pipeline.py        (async, ThreadPoolExecutor, max_workers=4)
        → data/checkpoints/ids.json          (phase 1: name → MA band ID)
        → data/checkpoints/raw_bands.ndjson  (phase 2: ID → full profile)
        → data/checkpoints/geo_cache (DuckDB) (phase 3: city → lat/lon)
    → db/ingest.py
        └─ metal.duckdb   (all queries, read-only at serve time)
    → api/  (FastAPI, Cloud Run)
    → site/ (SvelteKit, Firebase Hosting)
```

### GCP Stack

| Component | Service |
|---|---|
| API backend | Cloud Run (serverless, scales to zero) |
| API database | DuckDB (baked into Docker image, read-only) |
| Frontend | Firebase Hosting (free tier, global CDN) |
| Container registry | Google Artifact Registry |
| CI/CD | Cloud Build (`cloudbuild.yaml`) |

### Database Strategy

- **DuckDB** (`metal.duckdb`) — single database for all queries; written during
  ingest, read-only at serve time; baked into the Docker image
- No Postgres — dataset is small (241 bands, static) and doesn't need a managed DB

---

## Project Structure

```
metal-stats/
├── scraper/
│   ├── __init__.py
│   ├── pipeline.py        # async orchestrator — phases 0, 1, 2, 3
│   ├── ma_client.py       # enmet wrapper (Metal Archives)
│   ├── lastfm_client.py   # pylast wrapper
│   ├── geo_client.py      # geopy Nominatim + DuckDB geo_cache
│   └── checkpoint.py      # JSON + NDJSON checkpoint read/write helpers
│
├── db/
│   ├── __init__.py
│   ├── duckdb_schema.sql   # DuckDB DDL (canonical)
│   └── ingest.py           # NDJSON → DuckDB
│
├── api/
│   ├── __init__.py
│   ├── main.py             # FastAPI app entry point (13 routes incl /health)
│   ├── models.py           # Pydantic v2 response models
│   ├── database.py         # DuckDB read-only connection helper
│   └── routers/
│       ├── __init__.py
│       ├── bands.py        # GET /bands/search, GET /bands/{id}
│       ├── albums.py       # GET /bands/{id}/albums
│       ├── members.py      # GET /bands/{id}/members
│       └── stats.py        # GET /stats/* (DuckDB-backed)
│
├── tests/
│   ├── __init__.py
│   └── test_phase0.py      # Phase 0 dedup tests (runs against real CSV)
│
├── site/                   # SvelteKit (not yet scaffolded)
│
├── data/
│   ├── bandlist.csv                      # input: 597 album-level rows, 241 unique bands
│   └── checkpoints/                      # scraper state (gitignored)
│       └── .gitkeep
│
├── Dockerfile              # multi-stage: api + scraper targets
├── pyproject.toml
├── poetry.lock
├── .env.example
├── .gitignore
└── CLAUDE.md
```

---

## Scraper Design

### Three-Phase Pipeline (`scraper/pipeline.py`)

**Phase 0 — Band deduplication** (local, no network)
- Input: `data/bandlist.csv`
- Parse CSV, extract unique band names (many albums per band)
- Preserve per-band metadata from CSV: origin, album count, aggregate scores
- Output: deduplicated band list for Phase 1

**Phase 1 — Name resolution** (sequential, 3s delay between requests)
- Input: deduplicated band names from Phase 0
- For each name: search MA via `enmet`, match by exact name then by
  normalized name (diacritics/punctuation stripped)
- Single match → saved to `data/checkpoints/ids.json` — `{band_name: ma_id}`
- Multiple matches → saved to `data/checkpoints/unresolved.json` with
  candidate IDs, names, genres, and countries for manual review
- No match → skipped (band likely not on Metal Archives)
- Skip names already present in ids.json on resume
- Uses `curl_cffi` to bypass Cloudflare TLS fingerprinting on MA

**Manual resolution step** (required before Phase 2)
- Review `data/checkpoints/unresolved.json` — each entry lists candidate
  bands with their MA ID, genre, and country
- Pick the correct ID and add it to `ids.json` (alphabetized)
- Bands not on MA (classic rock, punk, solo artists) can be left out

**Phase 2 — Profile enrichment** (async, 4 workers)
- Input: `ids.json`
- For each MA ID: fetch full band profile via `enmet` + `pylast`
- Append one JSON object per line to `data/checkpoints/raw_bands.ndjson`
- Skip bands already in NDJSON on resume

**Phase 3 — Geocoding** (sync, 1 req/sec — Nominatim ToS)
- Input: `raw_bands.ndjson` location fields
- Strategy: tiered fallback
  1. MA `location` field present → geocode `"{city}, {country}"` via Nominatim
  2. No city → use country centroid from static lookup table
- Cache all results in `geo_cache` table in `metal.duckdb`
- Store `geo_precision`: `"city"` | `"country"`

### Threading Model

```python
# Use ThreadPoolExecutor (enmet + pylast are sync/blocking)
from concurrent.futures import ThreadPoolExecutor
import asyncio

async def enrich_all(band_names: list[str]) -> None:
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=4) as pool:
        tasks = [loop.run_in_executor(pool, enrich_band, name)
                 for name in band_names]
        await asyncio.gather(*tasks)
```

---

## Database Schema (`db/duckdb_schema.sql`)

See `db/duckdb_schema.sql` for the canonical DDL. Key tables: `bands`, `albums`,
`members`, `album_rankings`, `lastfm_stats`, `lyrics_meta`, `geo_cache`.
Band search uses `ILIKE` (no trigram extension needed).

---

## API Endpoints (`api/`)

| Method | Path | Description |
|---|---|---|
| GET | `/bands/search?q=&limit=` | Band name search via ILIKE |
| GET | `/bands/{id}` | Full band profile (denormalized) |
| GET | `/bands/{id}/albums` | Discography |
| GET | `/bands/{id}/members` | Full lineup history |
| GET | `/stats/geo` | All bands with lat/lon for heatmap |
| GET | `/stats/genres` | Genre breakdown counts |
| GET | `/stats/timeline` | Bands formed/active per year |
| GET | `/stats/popularity` | Last.fm listeners by country/genre |

All responses are Pydantic v2 models. All endpoints query DuckDB (read-only).
Endpoint functions are sync (DuckDB is blocking).

---

## Frontend (`site/`)

**Framework:** SvelteKit with static adapter shell + runtime API calls  
**Hosting:** Firebase Hosting  
**Viz libraries:** D3.js, Observable Plot  
**Aesthetic:** Slick, dark, modern — think music editorial meets data dashboard.
Dark background, strong typographic hierarchy, fluid transitions.

### Key pages

- `/` — Search bar (instant, debounced), featured stats, geo heatmap preview
- `/band/[id]` — Full band detail:
  - Hero: name, country, status, genres
  - Membership timeline (D3 Gantt-style)
  - Album release grid with type badges
  - Lyric word cloud
  - Last.fm stats panel (listeners, similar artists)

### Deployment

```bash
cd site
npm run build        # outputs to /site/build
firebase deploy      # pushes to Firebase Hosting
```

---

## API Deployment (Cloud Run)

```bash
# Build + push image
gcloud builds submit --config api/cloudbuild.yaml

# Deploy to Cloud Run
gcloud run deploy metal-api \
  --image gcr.io/YOUR_PROJECT/metal-api \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars DUCKDB_PATH=/app/data/metal.duckdb
```

DuckDB file is baked into the Docker image — no external database needed.

---

## Environment Variables

```bash
# Scraper
LASTFM_API_KEY=
LASTFM_API_SECRET=

# API
DUCKDB_PATH=./data/metal.duckdb
```

---

## Python Dependencies (`pyproject.toml`)

See `pyproject.toml` for the canonical dependency list. Key additions beyond
the original brief:
- `pandas` — CSV parsing and band deduplication from `bandlist.csv`
- `python-dotenv` — environment variable loading
- Dev extras: `pytest`, `pytest-asyncio`, `httpx`, `ruff`, `mypy`

---

## Coding Conventions

- Google-style docstrings on all functions and classes
- Type hints on all arguments and return values
- `Optional[X]` over `X | None` for consistency with existing code
- Sync functions for API handlers (DuckDB is blocking)
- Never hardcode credentials — use environment variables
- Rate limit all external HTTP calls (MA: 4 workers max, Nominatim: 1 req/sec)

---

## Local Development

### Poetry

Poetry manages the virtualenv. Always run Python through it:

```bash
poetry install --no-root          # install deps (skip project packaging)
poetry run python -m scraper.pipeline --phase 0   # run a command
poetry run pytest                 # run tests
poetry run ruff check .           # lint
```

Dev tools (`ruff`, `pytest`, `pytest-asyncio`, `httpx`, `mypy`) are listed
under `[project.optional-dependencies] dev` in `pyproject.toml` — **not** as
a Poetry group. They are tracked in `poetry.lock` but may need manual install
into the venv if missing:

```bash
poetry run pip install ruff pytest pytest-asyncio httpx mypy
```

### Quick Verification

```bash
# Phase 0 — no network, no credentials needed
poetry run python -m scraper.pipeline --phase 0

# Lint
poetry run ruff check .

# API import check (no DB required)
poetry run python -c "from api.main import app"

# Tests
poetry run pytest tests/ -v
```

### Pipeline CLI

```bash
poetry run python -m scraper.pipeline              # all phases
poetry run python -m scraper.pipeline --phase 0     # dedup only (prints top 20)
poetry run python -m scraper.pipeline --phase 1     # MA ID resolution
# >>> manually resolve data/checkpoints/unresolved.json into ids.json <<<
poetry run python -m scraper.pipeline --phase 2     # profile enrichment
poetry run python -m scraper.pipeline --phase 3     # geocoding
```

Phase 1 requires network + Metal Archives access. After phase 1, review
`unresolved.json` and add correct IDs to `ids.json` before running phase 2.
Phase 2 requires Metal Archives + Last.fm access.
Phase 3 requires Nominatim (rate-limited to 1 req/sec).

---

## bandlist.csv Details

- **597 rows** (album-level), **241 unique bands** after dedup
- **48 columns**: index, Band, Album, Year, Label, Producer, Album No.,
  Band Average Age, Band Origin, Album Length, Average Song Length, Score,
  then 36 ranking source columns
- **Year column** contains date strings like `"3-Mar-86"`, not plain integers
- **Band Origin** format: `"City, Country"` (e.g. `"California, USA"`,
  `"Birmingham, UK"`) — many rows have `NaN`
- **Score** is a float (composite ranking score, higher = rarer/more niche)
- First unnamed column is the original CSV row index (used as `csv_rank`)

---

## Implementation Status

- [x] `scraper/` — all modules implemented (checkpoint, ma_client,
      lastfm_client, geo_client, pipeline)
- [x] `db/` — DuckDB DDL + ingest.py (DuckDB-only)
- [x] `api/` — FastAPI app with all routers and Pydantic models (DuckDB-only)
- [x] `tests/` — Phase 0 tests (4 passing)
- [ ] `site/` — SvelteKit frontend (not yet scaffolded)
- [ ] `api/cloudbuild.yaml` — Cloud Build config (not yet created)
- [ ] End-to-end pipeline run (requires MA/Last.fm API credentials)
