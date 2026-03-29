"""DuckDB-backed aggregate statistics endpoints."""

from fastapi import APIRouter

from api.database import get_duckdb
from api.models import GenreCount, GeoPoint, PopularityStat, TimelinePoint

router = APIRouter(prefix="/stats", tags=["stats"])


@router.get("/geo", response_model=list[GeoPoint])
def stats_geo() -> list[GeoPoint]:
    """All bands with coordinates for the heatmap.

    Returns:
        List of geo points with band name and precision.
    """
    con = get_duckdb()
    rows = con.execute("""
        SELECT id, name, lat, lon, geo_precision
        FROM bands
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """).fetchall()
    con.close()
    return [
        GeoPoint(band_id=r[0], name=r[1], lat=r[2], lon=r[3], geo_precision=r[4])
        for r in rows
    ]


@router.get("/genres", response_model=list[GenreCount])
def stats_genres() -> list[GenreCount]:
    """Genre breakdown counts.

    Returns:
        Genre names with band counts, descending.
    """
    con = get_duckdb()
    rows = con.execute("""
        SELECT UNNEST(genres) AS genre, COUNT(*) AS cnt
        FROM bands
        WHERE genres IS NOT NULL
        GROUP BY genre
        ORDER BY cnt DESC
    """).fetchall()
    con.close()
    return [GenreCount(genre=r[0], count=r[1]) for r in rows]


@router.get("/timeline", response_model=list[TimelinePoint])
def stats_timeline() -> list[TimelinePoint]:
    """Bands formed per year.

    Returns:
        Year and count pairs, ascending by year.
    """
    con = get_duckdb()
    rows = con.execute("""
        SELECT formed_year, COUNT(*) AS cnt
        FROM bands
        WHERE formed_year IS NOT NULL
        GROUP BY formed_year
        ORDER BY formed_year
    """).fetchall()
    con.close()
    return [TimelinePoint(year=r[0], count=r[1]) for r in rows]


@router.get("/popularity", response_model=list[PopularityStat])
def stats_popularity() -> list[PopularityStat]:
    """Last.fm listener counts by band.

    Returns:
        Bands sorted by listener count descending.
    """
    con = get_duckdb()
    rows = con.execute("""
        SELECT b.id, b.name, b.country, s.listeners, s.play_count
        FROM bands b
        JOIN lastfm_stats s ON b.id = s.band_id
        ORDER BY s.listeners DESC
    """).fetchall()
    con.close()
    return [
        PopularityStat(
            band_id=r[0], name=r[1], country=r[2], listeners=r[3], play_count=r[4],
        )
        for r in rows
    ]
