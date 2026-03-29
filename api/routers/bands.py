"""Band search and detail endpoints."""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.database import get_duckdb
from api.models import Album, AlbumRanking, BandDetail, BandSummary, LastfmStats, Member

router = APIRouter(prefix="/bands", tags=["bands"])


@router.get("/search", response_model=list[BandSummary])
def search_bands(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(20, ge=1, le=100),
) -> list[BandSummary]:
    """Band name search via ILIKE.

    Args:
        q: Search string.
        limit: Max results.

    Returns:
        Matching bands sorted by relevance (exact match first, then by name length).
    """
    con = get_duckdb()
    rows = con.execute(
        """
        SELECT id, name, country, genres, formed_year, status
        FROM bands
        WHERE name ILIKE '%' || $1 || '%'
        ORDER BY (name ILIKE $1) DESC, length(name)
        LIMIT $2
        """,
        [q, limit],
    ).fetchall()
    con.close()
    return [
        BandSummary(
            id=r[0], name=r[1], country=r[2],
            genres=r[3] or [], formed_year=r[4], status=r[5],
        )
        for r in rows
    ]


@router.get("/{band_id}", response_model=BandDetail)
def get_band(band_id: str) -> BandDetail:
    """Fetch full band profile with albums, members, rankings, and Last.fm stats.

    Args:
        band_id: Metal Archives band ID.

    Returns:
        Denormalized band detail.
    """
    con = get_duckdb()

    row = con.execute("SELECT * FROM bands WHERE id = $1", [band_id]).fetchone()
    if not row:
        con.close()
        raise HTTPException(status_code=404, detail="Band not found")

    # Get column names for the bands table
    cols = [desc[0] for desc in con.description]
    band = dict(zip(cols, row))

    albums_rows = con.execute(
        "SELECT * FROM albums WHERE band_id = $1 ORDER BY year", [band_id]
    ).fetchall()
    album_cols = [desc[0] for desc in con.description]

    members_rows = con.execute(
        "SELECT * FROM members WHERE band_id = $1 ORDER BY is_current DESC, name",
        [band_id],
    ).fetchall()
    member_cols = [desc[0] for desc in con.description]

    rankings_rows = con.execute(
        "SELECT source, rank, csv_rank, score FROM album_rankings WHERE band_id = $1",
        [band_id],
    ).fetchall()

    lfm_row = con.execute(
        "SELECT * FROM lastfm_stats WHERE band_id = $1", [band_id]
    ).fetchone()
    lfm_cols = [desc[0] for desc in con.description] if lfm_row else []

    con.close()

    lastfm: Optional[LastfmStats] = None
    if lfm_row:
        lfm = dict(zip(lfm_cols, lfm_row))
        lastfm = LastfmStats(
            listeners=lfm["listeners"], play_count=lfm["play_count"],
            tags=lfm.get("tags") or [], similar=lfm.get("similar") or [],
        )

    return BandDetail(
        id=band["id"], name=band["name"], country=band.get("country"),
        location_raw=band.get("location_raw"), lat=band.get("lat"), lon=band.get("lon"),
        geo_precision=band.get("geo_precision"), formed_year=band.get("formed_year"),
        status=band.get("status"), genres=band.get("genres") or [],
        albums=[
            Album(**dict(zip(album_cols, a)))
            for a in albums_rows
        ],
        members=[
            Member(**dict(zip(member_cols, m)))
            for m in members_rows
        ],
        rankings=[
            AlbumRanking(source=r[0], rank=r[1], csv_rank=r[2], score=r[3])
            for r in rankings_rows
        ],
        lastfm=lastfm,
    )
