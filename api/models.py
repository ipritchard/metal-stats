"""Pydantic v2 response models for the metal-stats API."""

from typing import Optional

from pydantic import BaseModel


class BandSummary(BaseModel):
    """Lightweight band representation for search results."""

    id: str
    name: str
    country: Optional[str] = None
    genres: list[str] = []
    formed_year: Optional[int] = None
    status: Optional[str] = None


class Album(BaseModel):
    """Album/release entry."""

    id: str
    band_id: str
    title: str
    year: Optional[int] = None
    type: Optional[str] = None


class Member(BaseModel):
    """Band member entry."""

    id: int
    band_id: str
    name: str
    role: Optional[str] = None
    is_current: bool = False


class AlbumRanking(BaseModel):
    """Album ranking from a specific source list."""

    source: str
    rank: Optional[int] = None
    csv_rank: Optional[int] = None
    score: Optional[float] = None


class LastfmStats(BaseModel):
    """Last.fm popularity stats."""

    listeners: Optional[int] = None
    play_count: Optional[int] = None
    tags: list[str] = []
    similar: list[str] = []


class BandDetail(BaseModel):
    """Full denormalized band profile."""

    id: str
    name: str
    country: Optional[str] = None
    location_raw: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    geo_precision: Optional[str] = None
    formed_year: Optional[int] = None
    status: Optional[str] = None
    genres: list[str] = []
    albums: list[Album] = []
    members: list[Member] = []
    rankings: list[AlbumRanking] = []
    lastfm: Optional[LastfmStats] = None


class GeoPoint(BaseModel):
    """Band location for heatmap."""

    band_id: str
    name: str
    lat: float
    lon: float
    geo_precision: str


class GenreCount(BaseModel):
    """Genre frequency for breakdown charts."""

    genre: str
    count: int


class TimelinePoint(BaseModel):
    """Bands formed per year for timeline chart."""

    year: int
    count: int


class PopularityStat(BaseModel):
    """Last.fm popularity aggregate."""

    band_id: str
    name: str
    country: Optional[str] = None
    listeners: int
    play_count: int
