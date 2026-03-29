"""Geocoding via Nominatim with DuckDB cache and 1 req/sec rate limit."""

import logging
import time
from typing import Optional

import duckdb
from geopy.geocoders import Nominatim

log = logging.getLogger(__name__)

_geocoder: Optional[Nominatim] = None
_last_request: float = 0.0


def _get_geocoder() -> Nominatim:
    """Lazy-init the Nominatim geocoder.

    Returns:
        Configured Nominatim instance.
    """
    global _geocoder
    if _geocoder is None:
        _geocoder = Nominatim(user_agent="metal-stats-scraper/0.1")
    return _geocoder


def _rate_limit() -> None:
    """Enforce 1 request per second for Nominatim ToS compliance."""
    global _last_request
    elapsed = time.monotonic() - _last_request
    if elapsed < 1.0:
        time.sleep(1.0 - elapsed)
    _last_request = time.monotonic()


def _cache_lookup(con: duckdb.DuckDBPyConnection, query: str) -> Optional[tuple[float, float]]:
    """Check geo_cache for a previously resolved query.

    Args:
        con: DuckDB connection.
        query: Location query string.

    Returns:
        (lat, lon) tuple or None if not cached.
    """
    result = con.execute(
        "SELECT lat, lon FROM geo_cache WHERE query = ?", [query]
    ).fetchone()
    return (result[0], result[1]) if result else None


def _cache_store(con: duckdb.DuckDBPyConnection, query: str, lat: float, lon: float) -> None:
    """Store a geocoding result in the cache.

    Args:
        con: DuckDB connection.
        query: Location query string.
        lat: Latitude.
        lon: Longitude.
    """
    con.execute(
        "INSERT OR REPLACE INTO geo_cache (query, lat, lon) VALUES (?, ?, ?)",
        [query, lat, lon],
    )


def geocode(
    location: Optional[str],
    country: Optional[str],
    con: duckdb.DuckDBPyConnection,
) -> tuple[Optional[float], Optional[float], str]:
    """Geocode a location with tiered fallback and caching.

    Strategy:
        1. If location present, geocode "{location}, {country}"
        2. Otherwise, geocode country alone (centroid)

    Args:
        location: City/region string from MA (may be None).
        country: Country name.
        con: DuckDB connection for cache reads/writes.

    Returns:
        (lat, lon, precision) where precision is 'city' or 'country'.
        Returns (None, None, 'country') if all lookups fail.
    """
    if location and country:
        query = f"{location}, {country}"
        precision = "city"
    elif country:
        query = country
        precision = "country"
    else:
        return None, None, "country"

    # Check cache first
    cached = _cache_lookup(con, query)
    if cached:
        return cached[0], cached[1], precision

    # Live geocode
    _rate_limit()
    geo = _get_geocoder()
    try:
        result = geo.geocode(query)
        if result:
            _cache_store(con, query, result.latitude, result.longitude)
            return result.latitude, result.longitude, precision
    except Exception:
        log.exception("Geocode failed for '%s'", query)

    # Fall back to country-level if city lookup failed
    if precision == "city" and country:
        cached_country = _cache_lookup(con, country)
        if cached_country:
            return cached_country[0], cached_country[1], "country"

        _rate_limit()
        try:
            result = geo.geocode(country)
            if result:
                _cache_store(con, country, result.latitude, result.longitude)
                return result.latitude, result.longitude, "country"
        except Exception:
            log.exception("Country fallback geocode failed for '%s'", country)

    return None, None, "country"
