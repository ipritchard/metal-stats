"""Metal Archives (enmet) wrapper for band search and profile fetching."""

import logging
import re
import time
import unicodedata
from typing import Any, Optional

import enmet
import enmet.entities
import enmet.pages
from curl_cffi import requests as cffi_requests
from enmet.common import url_to_id
from enmet.pages import BandSearchPage

log = logging.getLogger(__name__)

MAX_RETRIES = 3
BASE_DELAY = 5  # seconds


def _cffi_get(url: str, **kwargs: Any) -> cffi_requests.Response:
    """Drop-in replacement for requests.get using curl_cffi.

    Args:
        url: Request URL.
        **kwargs: Forwarded to curl_cffi (params, headers, etc.).

    Returns:
        Response object (compatible with requests.Response).
    """
    kwargs.pop("headers", None)  # enmet passes its own UA; not needed
    return cffi_requests.get(url, impersonate="chrome", **kwargs)


# Monkey-patch enmet to use curl_cffi (bypasses Cloudflare TLS fingerprinting).
enmet.pages.get = _cffi_get
enmet.entities.requests = type("_mod", (), {"get": staticmethod(_cffi_get)})()


def _retry(
    func: Any, *args: Any, label: str = "", **kwargs: Any,
) -> Any:
    """Call *func* with retries and exponential backoff.

    Args:
        func: Callable to invoke.
        *args: Positional args forwarded to func.
        label: Human-readable label for log messages.
        **kwargs: Keyword args forwarded to func.

    Returns:
        Whatever func returns.

    Raises:
        Exception: Re-raises after MAX_RETRIES failures.
    """
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            if attempt == MAX_RETRIES - 1:
                raise
            delay = BASE_DELAY * (2 ** attempt)
            log.warning(
                "Attempt %d failed for %s (%s: %s), retrying in %ds",
                attempt + 1, label, type(exc).__name__, exc, delay,
            )
            time.sleep(delay)


def _normalize(s: str) -> str:
    """Normalize a band name for comparison: strip diacritics, punctuation, lowercase.

    Args:
        s: Raw band name.

    Returns:
        Normalized string for fuzzy comparison.
    """
    # Decompose unicode and strip combining marks (ö → o, ü → u, etc.)
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower()
    # Strip everything except letters, digits, and spaces
    s = re.sub(r"[^a-z0-9 ]", "", s)
    # Collapse whitespace
    return re.sub(r"\s+", " ", s).strip()


def _raw_search(name: str) -> list[dict[str, str]]:
    """Search MA and return raw result dicts with genre/country from search page.

    Args:
        name: Band name to search for.

    Returns:
        List of dicts with id, name, genre, country keys.
    """
    page = _retry(BandSearchPage, {"bandName": name}, label=f"search '{name}'")
    results = []
    for tup in page.bands:
        band_link, band_name, genre, country = tup[0], tup[1], tup[2], tup[3]
        results.append({
            "id": str(url_to_id(band_link)),
            "name": band_name,
            "genre": genre,
            "country": country,
        })
    return results


def search_band(name: str) -> tuple[Optional[str], list[dict[str, str]]]:
    """Search Metal Archives for a band by name.

    Tries exact match first, then falls back to normalized comparison
    (strips diacritics and punctuation). If multiple bands match, returns
    None with a candidate list so the user can resolve manually.

    Args:
        name: Band name to search for.

    Returns:
        Tuple of (band_id_or_None, candidates). Candidates is non-empty
        only when the match was ambiguous.
    """
    try:
        results = _raw_search(name)
        if not results:
            return None, []

        # Exact case-insensitive match
        exact = [r for r in results if r["name"].lower() == name.lower()]
        if len(exact) == 1:
            return exact[0]["id"], []
        if len(exact) > 1:
            log.warning("Multiple exact matches for '%s' (%d candidates)", name, len(exact))
            return None, exact

        # Normalized match (diacritics + punctuation stripped)
        norm_query = _normalize(name)
        fuzzy = [r for r in results if _normalize(r["name"]) == norm_query]
        if len(fuzzy) == 1:
            log.info("Normalized match for '%s' -> '%s'", name, fuzzy[0]["name"])
            return fuzzy[0]["id"], []
        if len(fuzzy) > 1:
            log.warning("Multiple normalized matches for '%s' (%d candidates)", name, len(fuzzy))
            return None, fuzzy

        log.warning("No match for '%s'", name)
        return None, []
    except Exception:
        log.exception("Error searching for band '%s'", name)
        return None, []


def fetch_profile(band_id: str) -> Optional[dict[str, Any]]:
    """Fetch full band profile from Metal Archives.

    Args:
        band_id: MA band ID.

    Returns:
        Dict with band details, or None on failure.
    """
    try:
        band = _retry(enmet.Band, band_id, label=f"profile {band_id}")
        albums = []
        for disc in band.discography:
            albums.append({
                "id": str(disc.id),
                "title": disc.title,
                "year": disc.year,
                "type": disc.type.value if hasattr(disc.type, "value") else str(disc.type),
            })

        members_current = []
        members_past = []
        for m in band.lineup:
            members_current.append({"name": m.name, "role": m.role})
        for m in band.past_members:
            members_past.append({"name": m.name, "role": m.role})

        return {
            "id": band_id,
            "name": band.name,
            "country": band.country.value if hasattr(band.country, "value") else str(band.country),
            "location": band.location,
            "formed_year": band.formed_in,
            "status": band.status.value if hasattr(band.status, "value") else str(band.status),
            "genres": band.genres,
            "albums": albums,
            "members_current": members_current,
            "members_past": members_past,
        }
    except Exception:
        log.exception("Error fetching profile for band ID %s", band_id)
        return None
