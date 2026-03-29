"""Band member/lineup endpoints."""

from fastapi import APIRouter, Query

from api.database import get_duckdb
from api.models import Member

router = APIRouter(prefix="/bands", tags=["members"])


@router.get("/{band_id}/members", response_model=list[Member])
def get_members(
    band_id: str,
    current_only: bool = Query(False, description="Only return current members"),
) -> list[Member]:
    """Fetch lineup history for a band.

    Args:
        band_id: Metal Archives band ID.
        current_only: If True, only return current members.

    Returns:
        List of members ordered by current status then name.
    """
    con = get_duckdb()
    if current_only:
        rows = con.execute(
            """SELECT * FROM members
               WHERE band_id = $1 AND is_current = TRUE
               ORDER BY name""",
            [band_id],
        ).fetchall()
    else:
        rows = con.execute(
            """SELECT * FROM members
               WHERE band_id = $1
               ORDER BY is_current DESC, name""",
            [band_id],
        ).fetchall()
    cols = [desc[0] for desc in con.description]
    con.close()
    return [Member(**dict(zip(cols, r))) for r in rows]
