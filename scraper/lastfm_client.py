"""Last.fm (pylast) wrapper for fetching band popularity stats."""

import logging
import os
from typing import Any, Optional

import pylast

log = logging.getLogger(__name__)

_network: Optional[pylast.LastFMNetwork] = None


def _get_network() -> pylast.LastFMNetwork:
    """Lazy-init the Last.fm network connection.

    Returns:
        Authenticated pylast network instance.
    """
    global _network
    if _network is None:
        _network = pylast.LastFMNetwork(
            api_key=os.environ["LASTFM_API_KEY"],
            api_secret=os.environ.get("LASTFM_API_SECRET", ""),
        )
    return _network


def fetch_stats(band_name: str) -> Optional[dict[str, Any]]:
    """Fetch Last.fm stats for a band.

    Args:
        band_name: Artist/band name to look up.

    Returns:
        Dict with listeners, play_count, tags, similar — or None on failure.
    """
    try:
        network = _get_network()
        artist = network.get_artist(band_name)

        listeners = artist.get_listener_count()
        play_count = artist.get_playcount()

        tags = [tag.item.get_name() for tag in artist.get_top_tags(limit=10)]

        similar = []
        try:
            for sim in artist.get_similar(limit=5):
                similar.append(sim.item.get_name())
        except pylast.WSError:
            pass  # some artists have no similar data

        return {
            "listeners": listeners,
            "play_count": play_count,
            "tags": tags,
            "similar": similar,
        }
    except Exception:
        log.exception("Error fetching Last.fm stats for '%s'", band_name)
        return None
