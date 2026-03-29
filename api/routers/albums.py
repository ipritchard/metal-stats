"""Album/discography endpoints."""

from fastapi import APIRouter, Query

from api.database import get_duckdb
from api.models import Album

router = APIRouter(prefix="/bands", tags=["albums"])


@router.get("/{band_id}/albums", response_model=list[Album])
def get_albums(
    band_id: str,
    type: str = Query(None, description="Filter by release type"),
) -> list[Album]:
    """Fetch discography for a band.

    Args:
        band_id: Metal Archives band ID.
        type: Optional release type filter (e.g. 'Full-length').

    Returns:
        List of albums ordered by year.
    """
    con = get_duckdb()
    if type:
        rows = con.execute(
            "SELECT * FROM albums WHERE band_id = $1 AND type = $2 ORDER BY year",
            [band_id, type],
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT * FROM albums WHERE band_id = $1 ORDER BY year",
            [band_id],
        ).fetchall()
    cols = [desc[0] for desc in con.description]
    con.close()
    return [Album(**dict(zip(cols, r))) for r in rows]
